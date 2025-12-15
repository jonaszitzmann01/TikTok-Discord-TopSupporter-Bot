import os
import asyncio
import sqlite3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent

# ========= KONFIG (ENV) =========
STREAMER_UNIQUE_ID = os.environ.get("TIKTOK_STREAMER", "").lstrip("@").strip()
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
DB_PATH = os.environ.get("DB_PATH", "tiktok_gifts.sqlite")
TZ_NAME = os.environ.get("TZ_NAME", "Europe/Berlin")

OFFLINE_RETRY_SECONDS = int(os.environ.get("OFFLINE_RETRY_SECONDS", "120"))
ERROR_RETRY_SECONDS = int(os.environ.get("ERROR_RETRY_SECONDS", "60"))

# Discord-Zeiten (lokal)
DAILY_POST_HOUR = int(os.environ.get("DAILY_POST_HOUR", "0"))     # 00:00
DAILY_POST_MINUTE = int(os.environ.get("DAILY_POST_MINUTE", "0")) # 00
SUNDAY_FINAL_HOUR = int(os.environ.get("SUNDAY_FINAL_HOUR", "23"))       # 23:59
SUNDAY_FINAL_MINUTE = int(os.environ.get("SUNDAY_FINAL_MINUTE", "59"))

# ========= CHECKS =========
if not STREAMER_UNIQUE_ID:
    raise SystemExit("ENV TIKTOK_STREAMER fehlt (z.B. 'some_streamer' ohne @).")
if not DISCORD_WEBHOOK_URL:
    raise SystemExit("ENV DISCORD_WEBHOOK_URL fehlt (Discord Webhook URL).")

TZ = ZoneInfo(TZ_NAME)

# ========= DB =========
def db_connect():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init_db(con):
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS gifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_id TEXT NOT NULL,
        streamer TEXT NOT NULL,
        gifter_username TEXT,
        gift_name TEXT,
        gift_id INTEGER,
        amount INTEGER NOT NULL,
        diamonds INTEGER NOT NULL,
        created_at_utc TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weekly_top5 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_id TEXT NOT NULL,
        rank INTEGER NOT NULL,
        gifter_username TEXT NOT NULL,
        total_diamonds INTEGER NOT NULL,
        created_at_utc TEXT NOT NULL,
        UNIQUE(week_id, rank)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL
    )
    """)
    con.commit()

def meta_get(con, key, default=""):
    cur = con.cursor()
    cur.execute("SELECT v FROM meta WHERE k=?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

def meta_set(con, key, value):
    cur = con.cursor()
    cur.execute(
        "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    con.commit()

# ========= WEEK HELPERS =========
def now_local():
    return datetime.now(TZ)

def current_week_id_local():
    y, w, _ = now_local().isocalendar()
    return f"{y}-W{w:02d}", y, w

def week_range_dates_local(year, week):
    mon = datetime.fromisocalendar(year, week, 1).date()
    sun = datetime.fromisocalendar(year, week, 7).date()
    return mon, sun

# ========= DISCORD =========
def format_top5_message(title, rows):
    lines = [title, "", "```", f"{'Rang':<4} {'Username':<24} {'Coins':>8}",
             f"{'-'*4} {'-'*24} {'-'*8}"]
    for i, (user, total) in enumerate(rows, start=1):
        user = (user or "unknown")[:24]
        lines.append(f"{i:<4} {user:<24} {total:>8}")
    lines.append("```")
    msg = "\n".join(lines)
    return msg[:1990]

async def discord_post(content):
    async with aiohttp.ClientSession() as session:
        async with session.post(DISCORD_WEBHOOK_URL, json={"content": content}) as resp:
            if resp.status >= 300:
                text = await resp.text()
                raise RuntimeError(f"Discord webhook error {resp.status}: {text}")

# ========= QUERIES =========
def top5_for_week(con, week_id):
    cur = con.cursor()
    cur.execute("""
        SELECT gifter_username, SUM(diamonds * amount) AS total
        FROM gifts
        WHERE week_id = ?
        GROUP BY gifter_username
        ORDER BY total DESC
        LIMIT 5
    """, (week_id,))
    rows = cur.fetchall()
    return [(r[0], int(r[1] or 0)) for r in rows]

def weekly_final_exists(con, week_id):
    cur = con.cursor()
    cur.execute("SELECT 1 FROM weekly_top5 WHERE week_id=? LIMIT 1", (week_id,))
    return cur.fetchone() is not None

def finalize_week(con, week_id):
    if weekly_final_exists(con, week_id):
        return
    rows = top5_for_week(con, week_id)
    cur = con.cursor()
    now_utc = datetime.now(timezone.utc).isoformat()
    for rank, (user, total) in enumerate(rows, start=1):
        cur.execute("""
            INSERT OR IGNORE INTO weekly_top5(week_id, rank, gifter_username, total_diamonds, created_at_utc)
            VALUES(?,?,?,?,?)
        """, (week_id, rank, user or "unknown", total, now_utc))
    con.commit()

def top5_final(con, week_id):
    cur = con.cursor()
    cur.execute("""
        SELECT gifter_username, total_diamonds
        FROM weekly_top5
        WHERE week_id=?
        ORDER BY rank ASC
        LIMIT 5
    """, (week_id,))
    rows = cur.fetchall()
    return [(r[0], int(r[1] or 0)) for r in rows]

# ========= MAIN =========
async def run():
    con = db_connect()
    init_db(con)

    client = TikTokLiveClient(unique_id=STREAMER_UNIQUE_ID)

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        gifter = getattr(event.user, "unique_id", None) or getattr(event.user, "nickname", None) or "unknown"
        gift = getattr(event, "gift", None)

        gift_name = getattr(gift, "name", None) if gift else None
        gift_id = getattr(gift, "id", None) if gift else None

        amount = getattr(gift, "repeat_count", None) if gift else None
        if not amount:
            amount = 1

        diamonds = getattr(gift, "diamond_count", None) if gift else None
        if diamonds is None:
            diamonds = 0

        week_id, _, _ = current_week_id_local()
        now_utc = datetime.now(timezone.utc).isoformat()

        con.execute("""
            INSERT INTO gifts(week_id, streamer, gifter_username, gift_name, gift_id, amount, diamonds, created_at_utc)
            VALUES(?,?,?,?,?,?,?,?)
        """, (week_id, STREAMER_UNIQUE_ID, gifter, gift_name, gift_id, int(amount), int(diamonds), now_utc))
        con.commit()

        print(f"[GIFT] {week_id} {gifter} -> {gift_name} x{amount} (diamonds={diamonds})")

    async def schedule_loop():
        # verhindert doppelte Posts
        last_daily_post_date = meta_get(con, "last_daily_post_date", "")
        last_final_post_week = meta_get(con, "last_final_post_week", "")

        while True:
            now = now_local()
            week_id, year, week = current_week_id_local()
            mon, sun = week_range_dates_local(year, week)

            # 1) TÄGLICH 00:00 -> Zwischenstand "läuft"
            if now.hour == DAILY_POST_HOUR and now.minute == DAILY_POST_MINUTE:
                today = now.date().isoformat()
                if today != last_daily_post_date:
                    title = f"**Woche {week} - {mon.strftime('%d.%m.%Y')} bis {sun.strftime('%d.%m.%Y')} - LÄUFT**"
                    rows = top5_for_week(con, week_id)
                    await discord_post(format_top5_message(title, rows))
                    last_daily_post_date = today
                    meta_set(con, "last_daily_post_date", last_daily_post_date)
                    print("[DISCORD] daily posted")

            # 2) SONNTAG 23:59 -> FINAL "Ende"
            # isoweekday(): 7 = Sonntag
            if now.isoweekday() == 7 and now.hour == SUNDAY_FINAL_HOUR and now.minute == SUNDAY_FINAL_MINUTE:
                if week_id != last_final_post_week:
                    finalize_week(con, week_id)
                    title = f"**Woche {week} - {mon.strftime('%d.%m.%Y')} bis {sun.strftime('%d.%m.%Y')} - ENDE**"
                    rows = top5_final(con, week_id)
                    await discord_post(format_top5_message(title, rows))
                    last_final_post_week = week_id
                    meta_set(con, "last_final_post_week", last_final_post_week)
                    print("[DISCORD] final posted (Sunday)")

            await asyncio.sleep(20)

    async def connect_loop():
        while True:
            try:
                print(f"[CHECK] connecting to @{STREAMER_UNIQUE_ID} ...")
                await client.start()
                print("[CHECK] disconnected, retrying...")
                await asyncio.sleep(OFFLINE_RETRY_SECONDS)
            except Exception as e:
                print(f"[ERROR] {e} -> retry in {ERROR_RETRY_SECONDS}s")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(ERROR_RETRY_SECONDS)

    await asyncio.gather(connect_loop(), schedule_loop())

if __name__ == "__main__":
    asyncio.run(run())
