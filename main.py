import os
import html
import sqlite3
import asyncio
import feedparser
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.constants import ParseMode

# ========= CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
CHECK_EVERY_MINUTES = int(os.getenv("CHECK_EVERY_MINUTES", "15"))
DB_PATH = os.getenv("DB_PATH", "sent.db")

# Feeds (podés sumar más)
FEED_URLS = [
    # CyberSecurityNews (FeedBurner)
    os.getenv("FEED_URL", "https://feeds.feedburner.com/cyber-security-news"),
    # CCN-CERT (ruta que devuelve el 301 → Location final)
    "https://www.ccn-cert.cni.es/es/component/obrss/rss-ultimas-vulnerabilidades?format=feed",
    # Trend Micro Simply Security (opcional):
    # "http://feeds.trendmicro.com/TrendMicroSimplySecurity",
]

# Filtrado opcional por keywords en TÍTULO (dejar set() para no filtrar)
KEYWORDS = set()  # ej: {"malware","ransomware","cve","windows","chrome"}

# ========= DB (de-dupe) =========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guid TEXT UNIQUE,
            url TEXT,
            published_ts INTEGER
        )
    """)
    conn.commit()
    conn.close()

def already_sent(guid: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent WHERE guid = ?", (guid,))
    row = cur.fetchone()
    conn.close()
    return row is not None

def mark_sent(guid: str, url: str, published_ts: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent(guid, url, published_ts) VALUES (?, ?, ?)",
        (guid, url, published_ts),
    )
    conn.commit()
    conn.close()

# ========= Helpers =========
def parse_published(entry) -> int:
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            return int(datetime(*entry.published_parsed[:6]).replace(tzinfo=timezone.utc).timestamp())
        if hasattr(entry, "updated_parsed") and entry.updated_parsed:
            return int(datetime(*entry.updated_parsed[:6]).replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        pass
    return int(datetime.now(tz=timezone.utc).timestamp())

def matches_keywords(title: str) -> bool:
    if not KEYWORDS:
        return True
    t = (title or "").lower()
    return any(k.lower() in t for k in KEYWORDS)

# ========= Core =========
async def send_news(bot: Bot):
    total_enviados = 0
    for FEED_URL in FEED_URLS:
        try:
            feed = feedparser.parse(FEED_URL)
        except Exception as ex:
            print(f"[WARN] parse error: {FEED_URL} :: {ex}")
            continue

        if getattr(feed, "bozo", False) and not getattr(feed, "entries", []):
            print(f"[WARN] feed vacío/bozo: {FEED_URL}")
            continue

        entries = sorted(feed.entries, key=parse_published, reverse=True)
        print(f"[INFO] {FEED_URL} → {len(entries)} entradas")

        for e in entries[:10]:  # mostramos las 10 más nuevas para debug
            title = getattr(e, "title", "Sin título")
            link = getattr(e, "link", None)
            guid_base = getattr(e, "id", None) or link or title
            guid = f"{FEED_URL}::{guid_base}"

            reasons = []
            if not link: reasons.append("sin link")
            if already_sent(guid): reasons.append("ya enviado")
            if not matches_keywords(title): reasons.append("no pasa keywords")

            if reasons:
                print(f"  └─ skip: {title}  [{', '.join(reasons)}]")
                continue

            text = f"<b>{html.escape(title)}</b>\n{link}"
            try:
                await bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )
                mark_sent(guid, link, parse_published(e))
                total_enviados += 1
                print(f"[OK] {title}  ({FEED_URL})")
                await asyncio.sleep(0.6)
            except Exception as ex:
                print(f"[ERR] enviando: {ex} ({FEED_URL})")

    if total_enviados == 0:
        print("No hubo artículos nuevos.")

async def main():
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID == 0:
        raise SystemExit("Faltan TELEGRAM_BOT_TOKEN y/o TELEGRAM_CHAT_ID")

    print(f"[BOOT] CHAT_ID={TELEGRAM_CHAT_ID}  CHECK_EVERY_MINUTES={CHECK_EVERY_MINUTES}")
    init_db()
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    print("Chequeo inicial del/los feed(s)...")
    await send_news(bot)

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(send_news, "interval", minutes=CHECK_EVERY_MINUTES, args=[bot])
    scheduler.start()
    print(f"Bot activo. Revisando cada {CHECK_EVERY_MINUTES} minutos.")

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Saliendo...")
