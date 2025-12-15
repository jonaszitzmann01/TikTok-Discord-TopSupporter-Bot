import os
import asyncio
import sqlite3
import hashlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import aiohttp
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent


# ================== KONFIG (ENV) ==================
STREAMER_UNIQUE_ID = os.environ.get("TIKTOK_STREAMER", "").lstrip("@").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
DB_PATH = os.environ.get("DB_PATH", "tiktok_gifts.sqlite")
TZ_NAME = os.environ.get("TZ_NAME", "Europe/Berlin")

OFFLINE_RETRY_SECONDS = int(os.environ.get("OFFLINE_RETRY_SECONDS", "120"))
ERROR_RETRY_SECONDS = int(os.environ.get("ERROR_RETRY_SECONDS", "60"))

# Discord-Zeiten (lokal)
DAILY_POST_HOUR = int(os.environ.get("DAILY_POST_HOUR", "0"))
DAILY_POST_MINUTE = int(os.environ.get("DAILY_POST_MINUTE", "0"))
SUNDAY_FINAL_HOUR = int(os.environ.get("SUNDAY_FINAL_HOUR", "23"))
SUNDAY_FINAL_MINUTE = int(os.environ.get("SUNDAY_FINAL_MINUTE", "59"))

# Health / Check-Logs
HEALTH_INTERVAL_SECONDS = int(os.environ.get("HEALTH_INTERVAL_SECONDS", "60"))
STALE_CONNECTION_SECONDS = int(os.environ.get("STALE_CONNECTION_SECONDS", "180"))  # wenn 3 Min. gar kein Event -> reconnect (optional)

if not STREAMER_UNIQUE_ID:
    raise SystemExit("ENV TIKTOK_STREAMER fehlt")
if not DISCORD_WEBHOOK_URL:
    raise SystemExit("ENV DISCORD_WEBHOOK_URL fehlt")

TZ = ZoneInfo(TZ_NAME)


# ================== DB ==================
def db_connect():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init_db(con: sqlite3.Connection):
    # Gifts: event_key UNIQUE verhindert doppelte Inserts
    con.execute("""
    CREATE TABLE IF NOT EXISTS gifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key TEXT UNIQUE,
        week_id TEXT,
        gifter_username TEXT,
        gift_name TEXT,
        amount INTEGER,
        diamonds INTEGER,
        created_at_utc TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS weekly_top5 (
        week_id TEXT,
        rank INTEGER,
        gifter_username TEXT,
        total_diamonds INTEGER,
        created_at_utc TEXT,
        UNIQUE(week_id, rank)
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT
    )
    """)
    con.commit()

def meta_get(con, key, default=""):
    cur = con.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

def meta_set(con, key, value):
    con.execute(
        "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    con.commit()


# ================== WEEK ==================
def now_local():
    return datetime.now(TZ)

def current_week():
    y, w, _ = now_local().isocalendar()
    return f"{y}-W{w:02d}", y, w

def week_range(year, week):
    mon = datetime.fromisocalendar(year, week, 1).date()
    sun = datetime.fromisocalendar(year, week, 7).date()
    return mon, sun


# ================== DISCORD ==================
async def discord_post(content: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(DISCORD_WEBHOOK_URL, json={"content": content}) as r:
            if r.status >= 300:
                raise RuntimeError(await r.text())

def format_message(title: str, rows):
    lines = [
        title, "",
        "```",
        f"{'Rang':<4} {'User':<24} {'Coins':>8}",
        f"{'-'*4} {'-'*24} {'-'*8}",
    ]
    for i, (u, t) in enumerate(rows, 1):
        lines.append(f"{i:<4} {(u or 'unknown')[:24]:<24} {t:>8}")
    lines.append("```")
    return "\n".join(lines)[:1990]


# ================== QUERIES ==================
def top5_week(con, week_id):
    cur = con.cursor()
    cur.execute("""
        SELECT gifter_username, SUM(diamonds * amount)
        FROM gifts
        WHERE week_id=?
        GROUP BY gifter_username
        ORDER BY 2 DESC
        LIMIT 5
    """, (week_id,))
    return [(r[0], int(r[1] or 0)) for r in cur.fetchall()]

def finalize_week(con, week_id):
    rows = top5_week(con, week_id)
    now = datetime.now(timezone.utc).isoformat()
    for i, (u, t) in enumerate(rows, 1):
        con.execute("""
            INSERT OR IGNORE INTO weekly_top5
            VALUES (?,?,?,?,?)
        """, (week_id, i, u, t, now))
    con.commit()


# ================== HELPERS ==================
def stable_amount_from_gift(gift) -> int:
    # Robust gegen verschiedene TikTokLive-Versionen / Gift-Objekte
    for attr in ("repeat_count", "repeat_total"):
        if hasattr(gift, attr):
            v = getattr(gift, attr)
            if isinstance(v, int) and v > 0:
                return v
    return 1

def make_event_key(week_id: str, user: str, gift_name: str, diamonds: int, amount: int, ts_utc: datetime) -> str:
    # 2-Sekunden-Bucket: verhindert Double-Events bei Reconnect / repeat-spam
    bucket = int(ts_utc.timestamp()) // 2
    raw = f"{week_id}|{user}|{gift_name}|{diamonds}|{amount}|{bucket}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


# ================== MAIN ==================
async def run():
    con = db_connect()
    init_db(con)

    client = TikTokLiveClient(unique_id=STREAMER_UNIQUE_ID)

    # Für Health Checks
    state = {
        "connected": False,
        "last_event_utc": datetime.now(timezone.utc),
        "last_connect_attempt_utc": None,
    }

    @client.on(GiftEvent)
    async def on_gift(e: GiftEvent):
        gift = e.gift
        user = getattr(e.user, "unique_id", None) or getattr(e.user, "nickname", None) or "unknown"
        gift_name = getattr(gift, "name", None) or "unknown"
        diamonds = int(getattr(gift, "diamond_count", 0) or 0)
        amount = stable_amount_from_gift(gift)

        week_id, _, _ = current_week()
        ts = datetime.now(timezone.utc)

        event_key = make_event_key(week_id, user, gift_name, diamonds, amount, ts)

        cur = con.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO gifts
            (event_key, week_id, gifter_username, gift_name, amount, diamonds, created_at_utc)
            VALUES (?,?,?,?,?,?,?)
        """, (
            event_key,
            week_id,
            user,
            gift_name,
            amount,
            diamonds,
            ts.isoformat()
        ))
        con.commit()

        # Nur loggen, wenn wirklich neu gespeichert wurde
        if cur.rowcount == 1:
            state["last_event_utc"] = ts
            print(f"[GIFT] {week_id} {user} -> {gift_name} x{amount} (diamonds={diamonds})")
        else:
            # Duplikat ignoriert (nicht spammen)
            state["last_event_utc"] = ts

    async def schedule_loop():
        while True:
            now = now_local()
            week_id, y, w = current_week()
            mon, sun = week_range(y, w)

            # 00:00 täglicher Zwischenstand
            if now.hour == DAILY_POST_HOUR and now.minute == DAILY_POST_MINUTE:
                if meta_get(con, "last_daily") != now.date().isoformat():
                    await discord_post(format_message(
                        f"**Woche {w} - {mon:%d.%m.%Y} bis {sun:%d.%m.%Y} - LÄUFT**",
                        top5_week(con, week_id)
                    ))
                    meta_set(con, "last_daily", now.date().isoformat())
                    print("[DISCORD] daily posted")

            # Sonntag 23:59 final
            if now.isoweekday() == 7 and now.hour == SUNDAY_FINAL_HOUR and now.minute == SUNDAY_FINAL_MINUTE:
                if meta_get(con, "last_final") != week_id:
                    finalize_week(con, week_id)
                    await discord_post(format_message(
                        f"**Woche {w} - {mon:%d.%m.%Y} bis {sun:%d.%m.%Y} - ENDE**",
                        top5_week(con, week_id)
                    ))
                    meta_set(con, "last_final", week_id)
                    print("[DISCORD] final posted")

            await asyncio.sleep(20)

    async def health_loop():
        while True:
            now = datetime.now(timezone.utc)
            age = (now - state["last_event_utc"]).total_seconds()

            if state["connected"]:
                print(f"[HEALTH] connected ✅ | last_event={int(age)}s ago")
                # Optional: wenn lange gar kein Event kommt, kann Verbindung trotzdem hängen -> reconnect triggern
                if age > STALE_CONNECTION_SECONDS:
                    print("[RECONNECT] no events for a while -> forcing reconnect")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
            else:
                print("[HEALTH] not connected ❌ (will retry)")

            await asyncio.sleep(HEALTH_INTERVAL_SECONDS)

    async def connect_loop():
        backoff = OFFLINE_RETRY_SECONDS
        max_backoff = 600

        while True:
            try:
                state["last_connect_attempt_utc"] = datetime.now(timezone.utc)
                print(f"[CONNECT] trying @{STREAMER_UNIQUE_ID} ...")

                # immer sauber trennen vor neuem start
                try:
                    await client.disconnect()
                except Exception:
                    pass

                await client.start()  # blockt, bis disconnected
                # wenn wir hier sind: disconnected
                state["connected"] = False
                print("[LIVE] disconnected ❌ -> will retry")
                backoff = OFFLINE_RETRY_SECONDS
                await asyncio.sleep(backoff)

            except Exception as e:
                state["connected"] = False
                msg = str(e)
                low = msg.lower()

                # sehr häufig: offline (kein echter Fehler)
                if "offline" in low:
                    backoff = OFFLINE_RETRY_SECONDS
                    print(f"[LIVE] streamer offline -> retry in {backoff}s")

                # Sign/504/500: Backoff hochfahren
                elif "sign" in low or "504" in low or "status code 500" in low:
                    backoff = min(max_backoff, max(60, backoff * 2))
                    print(f"[ERROR] sign/timeout -> retry in {backoff}s | {msg}")

                # doppelte Verbindung
                elif "one connection" in low:
                    backoff = 10
                    print(f"[ERROR] double-connection -> retry in {backoff}s | {msg}")

                else:
                    backoff = ERROR_RETRY_SECONDS
                    print(f"[ERROR] {msg} -> retry in {backoff}s")

                try:
                    await client.disconnect()
                except Exception:
                    pass

                await asyncio.sleep(backoff)
            else:
                # nur falls start() ohne Exception sofort zurückkommt (selten)
                state["connected"] = True
                print("[LIVE] connected ✅")

    # Wenn die Library einen Connect-Event hätte, könnte man state["connected"]=True setzen.
    # Praktisch: sobald Gifts kommen, wissen wir: verbunden. Zusätzlich setzen wir "connected" hier,
    # wenn start() erfolgreich läuft (nicht mit Exception rausfliegt).
    async def connection_flag_loop():
        # Heuristik: wenn connect_loop gerade läuft und keine Exception/Disconnect meldet,
        # setzen wir connected auf True, sobald wir mind. einmal erfolgreich starten konnten.
        # Da client.start() blockt, ist das schwer perfekt zu erkennen, aber:
        # - wenn Gifts reinkommen => connected.
        # - health_loop zeigt "connected" anhand von state["connected"].
        while True:
            # Wenn in den letzten 10 Sekunden ein Event kam -> connected
            age = (datetime.now(timezone.utc) - state["last_event_utc"]).total_seconds()
            if age < 10:
                state["connected"] = True
            await asyncio.sleep(5)

    print("[BOOT] starting bot")
    await asyncio.gather(connect_loop(), schedule_loop(), health_loop(), connection_flag_loop())


if __name__ == "__main__":
    asyncio.run(run())
