import os
import asyncio
import sqlite3
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

DAILY_POST_HOUR = int(os.environ.get("DAILY_POST_HOUR", "0"))
DAILY_POST_MINUTE = int(os.environ.get("DAILY_POST_MINUTE", "0"))
SUNDAY_FINAL_HOUR = int(os.environ.get("SUNDAY_FINAL_HOUR", "23"))
SUNDAY_FINAL_MINUTE = int(os.environ.get("SUNDAY_FINAL_MINUTE", "59"))

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

def init_db(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS gifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
async def discord_post(content):
    async with aiohttp.ClientSession() as session:
        async with session.post(DISCORD_WEBHOOK_URL, json={"content": content}) as r:
            if r.status >= 300:
                raise RuntimeError(await r.text())

def format_message(title, rows):
    lines = [
        title, "",
        "```",
        f"{'Rang':<4} {'User':<24} {'Coins':>8}",
        f"{'-'*4} {'-'*24} {'-'*8}"
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

# ================== MAIN ==================
async def run():
    con = db_connect()
    init_db(con)

    client = TikTokLiveClient(unique_id=STREAMER_UNIQUE_ID)

    @client.on(GiftEvent)
    async def on_gift(e: GiftEvent):
        gift = e.gift
        user = e.user.unique_id if e.user else "unknown"

        # ✅ STABILE Gift-Menge
        amount = 1
        if hasattr(gift, "repeat_count") and gift.repeat_count:
            amount = gift.repeat_count
        elif hasattr(gift, "repeat_total") and gift.repeat_total:
            amount = gift.repeat_total

        diamonds = gift.diamond_count or 0

        week_id, _, _ = current_week()
        con.execute("""
            INSERT INTO gifts VALUES (NULL,?,?,?,?,?,?)
        """, (
            week_id,
            user,
            gift.name if gift else None,
            amount,
            diamonds,
            datetime.now(timezone.utc).isoformat()
        ))
        con.commit()

        print(f"[GIFT] {week_id} {user} -> {gift.name} x{amount} ({diamonds})")

    async def schedule_loop():
        while True:
            now = now_local()
            week_id, y, w = current_week()
            mon, sun = week_range(y, w)

            if now.hour == DAILY_POST_HOUR and now.minute == DAILY_POST_MINUTE:
                if meta_get(con, "last_daily") != now.date().isoformat():
                    await discord_post(format_message(
                        f"Woche {w} - {mon:%d.%m.%Y} bis {sun:%d.%m.%Y} - läuft",
                        top5_week(con, week_id)
                    ))
                    meta_set(con, "last_daily", now.date().isoformat())
                    print("[DISCORD] daily posted")

            if now.isoweekday() == 7 and now.hour == SUNDAY_FINAL_HOUR and now.minute == SUNDAY_FINAL_MINUTE:
                if meta_get(con, "last_final") != week_id:
                    finalize_week(con, week_id)
                    await discord_post(format_message(
                        f"Woche {w} - {mon:%d.%m.%Y} bis {sun:%d.%m.%Y} - Ende",
                        top5_week(con, week_id)
                    ))
                    meta_set(con, "last_final", week_id)
                    print("[DISCORD] final posted")

            await asyncio.sleep(20)

    async def connect_loop():
        backoff = OFFLINE_RETRY_SECONDS
        max_backoff = 600

        while True:
            try:
                try:
                    await client.disconnect()
                except Exception:
                    pass

                print(f"[CHECK] connecting to @{STREAMER_UNIQUE_ID}")
                await client.start()
                backoff = OFFLINE_RETRY_SECONDS
                await asyncio.sleep(backoff)

            except Exception as e:
                msg = str(e).lower()
                if "sign" in msg or "504" in msg:
                    backoff = min(max_backoff, backoff * 2)
                elif "one connection" in msg:
                    backoff = 10
                else:
                    backoff = ERROR_RETRY_SECONDS

                print(f"[ERROR] {e} -> retry in {backoff}s")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(backoff)

    await asyncio.gather(connect_loop(), schedule_loop())

if __name__ == "__main__":
    asyncio.run(run())
