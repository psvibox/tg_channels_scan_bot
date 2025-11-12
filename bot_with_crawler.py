# bot_with_crawler.py v1.1
# pip install aiogram fastapi uvicorn telethon
import os, html, asyncio, sqlite3, json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request, HTTPException, Response, Query
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, LinkPreviewOptions, Update
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import Command

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest

# ===== настройки окружения =====
PORT = int(os.getenv("PORT", "10000"))  # Render открывает порт из $PORT
BASE_URL = os.getenv("BASE_URL")        # публичный https URL Render сервиса, например https://your-app.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "secret123")  # любой токен (только латиница/цифры/подчёркивание/дефис)
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")  # не включаем секрет в путь
DELETE_WEBHOOK_ON_SHUTDOWN = os.getenv("DELETE_WEBHOOK_ON_SHUTDOWN", "0") == "1"
WEBHOOK_WATCHDOG = os.getenv("WEBHOOK_WATCHDOG", "1") == "1"  # включить сторожа
EXPECTED_WEBHOOK_URL = None  # вычислим на старте из BASE_URL + WEBHOOK_PATH

#DB_PATH = Path(os.getenv("DB_PATH", "data/search.db"))
RUN_DIR = Path(os.getenv("RUN_DIR", ".")).resolve()
DB_PATH = Path(os.getenv("DB_PATH", str(RUN_DIR / "data" / "search.db"))).resolve()
print(f"[env] cwd={Path.cwd()} DB_PATH={DB_PATH}")
CHANNELS_FILE = Path(os.getenv("CHANNELS_FILE", "channels.txt"))
CRAWL_INTERVAL_SEC = int(os.getenv("CRAWL_INTERVAL_SEC", "900"))  # каждые 15 минут
# Порог первичного бэкапа: либо точная дата ISO, либо N дней «вглубь»
CRAWL_SINCE_DAYS = int(os.getenv("CRAWL_SINCE_DAYS", "365"))
CRAWL_SINCE_ISO  = os.getenv("CRAWL_SINCE_ISO", "").strip()

ADMIN_KEY = os.getenv("ADMIN_KEY", "")

# Bot API токен
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN or ":" not in TOKEN:
    raise SystemExit("BOT_TOKEN не задан или неверен")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

kb_search = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔎 Пример запроса")]
    ],
    resize_keyboard=True,     # подгоняем под экран
    one_time_keyboard=False,  # пусть висит постоянно
    input_field_placeholder=None  # placeholder зададим при отправке сообщения
)

# Telethon (my.telegram.org)
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
if not API_ID or not API_HASH:
    raise SystemExit("TG_API_ID / TG_API_HASH не заданы")

SESSION = os.getenv("TG_SESSION", "tg_crawler_embedded")
raw_s = os.getenv("TG_SESSION_STRING", "")
SESSION_STRING = raw_s.strip().strip("'").strip('"')

print("[env] TG_SESSION_STRING set:", bool(SESSION_STRING), "len:", len(SESSION_STRING))
if SESSION_STRING and len(SESSION_STRING) < 100:
    print("[env][warn] сессия подозрительно короткая, похоже, битая.")

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
    print(f"[crawler] channels loaded: {len(chans)} -> {chans[:5]}{' ...' if len(chans)>5 else ''}")
    if not chans:
        print("[crawler] channels.txt пуст")
        return

    con = db()
    print("[env] TG_SESSION_STRING set:", bool(SESSION_STRING), "len:", len(SESSION_STRING or 0))
    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
    else:
        client = TelegramClient(SESSION, API_ID, API_HASH)
        await client.connect()
        
    try:
        me = await client.get_me()
        print("[telethon] me:", getattr(me, "id", None), getattr(me, "username", None), "bot=", getattr(me, "bot", None))
    except Exception as e:
        print("[telethon] get_me() failed:", e)
        
    if getattr(me, "bot", False):
       raise RuntimeError(
             "TG_SESSION_STRING указывает на БОТА. Краулер Telethon требует пользовательскую сессию. "
             "Сгенерируй StringSession через вход по телефону и подставь её в переменную окружения."
             )

    # вычисляем порог «не старше чем»
    if CRAWL_SINCE_ISO:
         try:
             cutoff_dt = datetime.fromisoformat(CRAWL_SINCE_ISO)
             if cutoff_dt.tzinfo is None:
                 cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
         except Exception:
             cutoff_dt = datetime.now(timezone.utc) - timedelta(days=CRAWL_SINCE_DAYS)
    else:
         cutoff_dt = datetime.now(timezone.utc) - timedelta(days=CRAWL_SINCE_DAYS)
    print(f"[crawler] cutoff date (first run): {cutoff_dt.isoformat()}")

    if not await client.is_user_authorized():
        raise RuntimeError("Telethon не авторизован. Установи TG_SESSION_STRING (StringSession) или смонтируй persist-диск и выполни вход один раз.")
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
                # Если это первичный прогон (last_id == 0), обрезаем по дате
                if last_id == 0 and m.date:
                     md = m.date if m.date.tzinfo else m.date.replace(tzinfo=timezone.utc)
                     # Telethon отдаёт от новых к старым, как только ушли ниже порога — дальше только старее
                     if md < cutoff_dt:
                         break
                         
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
     AND d.date>Date('now',?)
    ORDER BY bm25(docs_fts)
    LIMIT ?;
    """
    rows = con.execute(sql, (q, '-'+str(CRAWL_SINCE_DAYS)+' days', limit)).fetchall()
    con.close()
    return rows


# ==== хендлеры бота ====
@dp.message(F.text & ~F.text.startswith("/"))
async def plain_text(m: Message):
    await do_search(m)


@dp.message(F.command == "start")
async def start(m: Message):
    await m.answer("Пишите запрос прямо сообщением. Для фраз используйте кавычки, для префиксов *.")


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

#Хендлер для кнопки «Пример запроса»:
@dp.message(F.text == "🔎 Пример запроса")
async def show_examples(m: Message):
    await m.answer(
        "Примеры:\n"
        "• гостин* проект — найдёт гостиница, гостиничный, и т. п. вместе со словом «проект»\n"
        "• (отель OR гостин*) AND проект — отели или гостиницы, где есть «проект»\n"
        "• \"строительство гостиницы\" — точная фраза\n"
        "• text:туризм AND chat_title:Россия — слово в тексте и в названии канала",
        reply_markup=kb_search,
        input_field_placeholder="Введите свой запрос"
    )


# ==== FastAPI + lifespan вместо on_event ====
@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    if not BASE_URL:
        # Если упадём на старте, корректно закроем сессию бота
        try:
            await bot.session.close()
        except Exception:
            pass
        raise RuntimeError("BASE_URL не задан (например, https://your-app.onrender.com)")

    # IMPORTANT: не трогаем глобальную BASE_URL, работаем с локальной копией
    base_url = (BASE_URL or "").strip()
    
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"[startup] ensure db dir: {DB_PATH.parent} exists")

    # Устанавливаем вебхук и готовим фон
    try:
        base_url = base_url.rstrip("/")
        global EXPECTED_WEBHOOK_URL
        EXPECTED_WEBHOOK_URL = base_url + WEBHOOK_PATH

        await bot.set_webhook(
            url=EXPECTED_WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
            allowed_updates=["message","edited_message","channel_post","edited_channel_post","callback_query"],
        )
        crawler_task = asyncio.create_task(crawler_loop())
        print("[startup] webhook установлен {EXPECTED_WEBHOOK_URL}, краулер запущен")
        # Запускаем сторожа
        watchdog_task = asyncio.create_task(webhook_watchdog()) if WEBHOOK_WATCHDOG else None
    except Exception:
        # Если что-то пошло не так на старте — закрываем HTTP-сессию бота
        try:
            await bot.session.close()
        except Exception:
            pass
        raise

    try:
        yield
    finally:
        # останавливаем фон
        for t in (locals().get("crawler_task"), locals().get("webhook_task"), locals().get("watchdog_task")):
            if t:
                t.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await t
        # НЕ снимаем вебхук на Render Free: Telegram сам ретраит запросы, URL остаётся тем же
        if DELETE_WEBHOOK_ON_SHUTDOWN:
            with suppress(Exception):
                 await bot.delete_webhook(drop_pending_updates=True)
            print("[shutdown] webhook снят")
        
        with suppress(Exception):
            await bot.session.close()
        print("[shutdown] фон остановлен, сессия закрыта")


app = FastAPI(lifespan=lifespan)

#Чтобы быстро увидеть, кто «сбивает» вебхук, добавь лёгкий эндпоинт:
@app.get("/admin/webhook_info")
async def admin_webhook_info(key: str = Query("")):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    info = await bot.get_webhook_info()
    return json.loads(info.model_dump_json())
    
# прокидываем обновления в aiogram
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    # Проверяем секрет вебхука
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    update = Update(**data)
    await dp.feed_update(bot, update)
    return Response(content='{"ok": true}', media_type="application/json")

#Хелпер для статистики:
def db_stats():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total_docs = cur.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
    total_channels = cur.execute("SELECT COUNT(*) FROM channels_state").fetchone()[0]
    last_rows = cur.execute("""
        SELECT chat_title, username, last_msg_id, updated_at
        FROM channels_state
        ORDER BY updated_at DESC
        LIMIT 10
    """).fetchall()
    con.close()
    return total_docs, total_channels, last_rows

#Команда бота /stats
@dp.message(Command("stats"))
async def stats_cmd(m: Message):
    total_docs, total_channels, last_rows = db_stats()
    lines = [f"docs: {total_docs}", f"channels: {total_channels}"]
    for t in last_rows:
        title, username, last_id, updated = t
        u = f"@{username}" if username else ""
        lines.append(f"• {title} {u}  last_id={last_id}  {updated}")
    await m.answer("\n".join(lines))

#HTTP-эндпоинт /admin/stats с ключом:
@app.get("/admin/stats")
async def admin_stats(key: str = Query(""), limit: int = 10):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    total_docs, total_channels, last_rows = db_stats()
    return {
        "docs": total_docs,
        "channels": total_channels,
        "last": [
            {
                "chat_title": r[0],
                "username": r[1],
                "last_msg_id": r[2],
                "updated_at": r[3],
            }
            for r in last_rows[:limit]
        ],
    }

#Фоновая задача-сторож
async def webhook_watchdog():
    assert EXPECTED_WEBHOOK_URL, "EXPECTED_WEBHOOK_URL не задан"
    while True:
        try:
            info = await bot.get_webhook_info()
            # Приведём к dict для наглядных логов
            data = json.loads(info.model_dump_json())
            url = data.get("url")
            last_err = data.get("last_error_message")
            pend = data.get("pending_update_count")
            if url != EXPECTED_WEBHOOK_URL:
                print(f"[webhook][watchdog] рассинхрон: сейчас url={url!r}, ожидаю {EXPECTED_WEBHOOK_URL!r}. Переставляю…")
                await bot.set_webhook(
                    url=EXPECTED_WEBHOOK_URL,
                    secret_token=WEBHOOK_SECRET,
                    drop_pending_updates=False,
                    allowed_updates=["message","edited_message","channel_post","edited_channel_post","callback_query"],
                )
                print("[webhook][watchdog] установлен заново.")
            else:
                if last_err:
                    print(f"[webhook][watchdog] ok, но у Telegram ошибка доставки: {last_err}. pending={pend}")
                else:
                    print(f"[webhook][watchdog] ok. pending={pend}")
        except Exception as e:
            print("[webhook][watchdog] ошибка:", e)
        await asyncio.sleep(60)

# health
@app.get("/")
async def root():
    return {"status": "ok"}























