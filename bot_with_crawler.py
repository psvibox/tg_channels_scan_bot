# bot_with_crawler.py
# pip install aiogram fastapi uvicorn telethon
import os, html, asyncio, sqlite3
from pathlib import Path
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, LinkPreviewOptions
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.webhook.middlewares.fastapi import FastAPIRequestHandler

from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest

# ===== настройки окружения =====
PORT = int(os.getenv("PORT", "10000"))  # Render открывает порт из $PORT
BASE_URL = os.getenv("BASE_URL")        # публичный https URL Render сервиса, например https://your-app.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "secret123")  # любой токен
WEBHOOK_PATH = f"/webhook/{WEBHOOK_SECRET}"

DB_PATH = Path("data/search.db")
CHANNELS_FILE = Path(os.getenv("CHANNELS_FILE", "channels.txt"))
CRAWL_INTERVAL_SEC = int(os.getenv("CRAWL_INTERVAL_SEC", "900"))  # каждые 15 минут

# Bot API токен
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN or ":" not in TOKEN:
    raise SystemExit("BOT_TOKEN не задан или неверен")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# Telethon (my.telegram.org)
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
if not API_ID or not API_HASH:
    raise SystemExit("TG_API_ID / TG_API_HASH не заданы")

SESSION = os.getenv("TG_SESSION", "tg_crawler_embedded")

DDL = """
CREATE TABLE IF NOT EXISTS docs (
  id INTEGER PRIMARY KEY,
  chat_id INTEGER,
  chat_title TEXT,
  msg_id INTEGER,
  date TEXT,
  url TEXT,
  text TEXT,
  UNIQUE(chat_id, msg_id)
);
CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(
  text, chat_title, url, content='docs', content_rowid='id'
);
CREATE TRIGGER IF NOT EXISTS docs_ai AFTER INSERT ON docs BEGIN
  INSERT INTO docs_fts(rowid, text, chat_title, url)
  VALUES (new.id, new.text, new.chat_title, new.url);
END;
CREATE TRIGGER IF NOT EXISTS docs_ad AFTER DELETE ON docs BEGIN
  INSERT INTO docs_fts(docs_fts, rowid, text, chat_title, url)
  VALUES('delete', old.id, old.text, old.chat_title, old.url);
END;
CREATE TRIGGER IF NOT EXISTS docs_au AFTER UPDATE ON docs BEGIN
  INSERT INTO docs_fts(docs_fts, rowid, text, chat_title, url)
  VALUES('delete', old.id, old.text, old.chat_title, url);
  INSERT INTO docs_fts(rowid, text, chat_title, url)
  VALUES (new.id, new.text, new.chat_title, new.url);
END;
CREATE TABLE IF NOT EXISTS channels_state (
  chat_id INTEGER PRIMARY KEY,
  chat_title TEXT,
  username TEXT,
  last_msg_id INTEGER DEFAULT 0,
  updated_at TEXT
);
"""

def db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.executescript(DDL)
    return con

def load_channels():
    if not CHANNELS_FILE.exists():
        return []
    lines = CHANNELS_FILE.read_text(encoding="utf-8").splitlines()
    return [s.strip() for s in lines if s.strip() and not s.strip().startswith("#")]

def last_msg_id_for(con, chat_id: int) -> int:
    row = con.execute("SELECT last_msg_id FROM channels_state WHERE chat_id=?", (chat_id,)).fetchone()
    return int(row[0]) if row and row[0] else 0

def upsert_state(con, chat_id, title, username, last_id):
    con.execute(
        """INSERT INTO channels_state (chat_id, chat_title, username, last_msg_id, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(chat_id) DO UPDATE SET
             chat_title=excluded.chat_title,
             username=excluded.username,
             last_msg_id=max(channels_state.last_msg_id, excluded.last_msg_id),
             updated_at=excluded.updated_at
        """,
        (chat_id, title, username or "", last_id, datetime.now(timezone.utc).isoformat())
    )

async def crawl_once():
    chans = load_channels()
    if not chans:
        print("[crawler] channels.txt пуст")
        return

    con = db()
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    try:
        for chan in chans:
            try:
                try:
                    entity = await client.get_entity(chan)
                except Exception:
                    await client(JoinChannelRequest(chan))
                    entity = await client.get_entity(chan)
            except Exception as e:
                print(f"[crawler] пропуск {chan}: {e}")
                continue

            chat_id = entity.id
            title = getattr(entity, "title", str(chan))
            username = getattr(entity, "username", None)

            last_id = last_msg_id_for(con, chat_id)
            kwargs = {"min_id": last_id} if last_id > 0 else {"limit": None}

            max_seen = last_id
            inserted_before = con.total_changes

            async for m in client.iter_messages(entity, **kwargs):
                text = m.message or ""
                if not text:
                    continue
                url = f"https://t.me/{username}/{m.id}" if username else ""
                con.execute(
                    "INSERT OR IGNORE INTO docs (chat_id, chat_title, msg_id, date, url, text) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (chat_id, title, m.id, m.date.isoformat() if m.date else "", url, text)
                )
                if m.id and m.id > max_seen:
                    max_seen = m.id

            con.commit()
            upsert_state(con, chat_id, title, username, max_seen)
            con.commit()
            added = con.total_changes - inserted_before
            print(f"[crawler] {title}: +{added} (last_id -> {max_seen})")
    finally:
        await client.disconnect()
        con.close()

async def crawler_loop():
    while True:
        try:
            await crawl_once()
        except Exception as e:
            print("[crawler] ошибка:", e)
        await asyncio.sleep(CRAWL_INTERVAL_SEC)

def query_db(q: str, limit: int = 10):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    sql = """
    SELECT d.chat_title, d.url, d.date, substr(d.text, 1, 500) AS cut
    FROM docs_fts
    JOIN docs d ON d.id = docs_fts.rowid
    WHERE docs_fts MATCH ?
    ORDER BY bm25(docs_fts)
    LIMIT ?;
    """
    rows = con.execute(sql, (q, limit)).fetchall()
    con.close()
    return rows

# ==== хендлеры бота ====
@dp.message(F.text & ~F.text.startswith("/"))
async def plain_text(m: Message):
    await do_search(m)

@dp.message(F.command == "start")
async def start(m: Message):
    await m.answer("Пишите запрос прямо сообщением. Для фраз используйте кавычки, для префиксов — *.")

async def do_search(m: Message):
    q = (m.text or "").strip()
    if not q:
        await m.reply("Введите запрос")
        return
    rows = query_db(q, limit=10)
    if not rows:
        await m.reply("Ничего не нашлось.")
        return
    lp_opts = LinkPreviewOptions(is_disabled=True)
    for r in rows:
        title = html.escape(r["chat_title"] or "Канал")
        date = (r["date"] or "")[:19]
        url = r["url"] or ""
        snip = html.escape((r["cut"] or "").replace("\n", " "))
        if len(snip) > 300:
            snip = snip[:300] + "…"
        text = f"<b>{title}</b>\n{date}\n{snip}\n"
        if url:
            text += url + "\n"
        await m.answer(text, link_preview_options=lp_opts)

# ==== FastAPI + webhook ====
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    # ставим webhook
    if not BASE_URL:
        raise RuntimeError("BASE_URL не задан (например, https://your-app.onrender.com)")
    await bot.set_webhook(url=BASE_URL + WEBHOOK_PATH, drop_pending_updates=True)
    # запускаем фоновый краулер
    asyncio.create_task(crawler_loop())
    print("[startup] webhook установлен, краулер запущен")

@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook(drop_pending_updates=True)

# прокидываем обновления в aiogram
request_handler = FastAPIRequestHandler(dp, bot)
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        await request_handler.handle(request)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# health
@app.get("/")
async def root():
    return {"status": "ok"}
