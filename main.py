import asyncio
import os
import re
import csv
import sqlite3
import logging
import html
import tempfile
import aiohttp
from urllib.parse import urlparse
from typing import Dict, List, Optional, Any, Union

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

# Event loop
APP_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(APP_LOOP)

# --- Configuración vía ENV ---
API_ID_STR = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING", "").strip().strip("\"\'")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BOT_COMM_CHANNEL = os.environ.get("BOT_COMM_CHANNEL", "")
TELEGRAPH_SOURCE = "@AsukaScr"

# Validar variables críticas
if not all([API_ID_STR, API_HASH, BOT_TOKEN]):
    raise ValueError("Las variables de entorno API_ID, API_HASH y BOT_TOKEN son obligatorias.")
try:
    API_ID = int(API_ID_STR)
except (TypeError, ValueError):
    raise ValueError("API_ID debe ser un número entero válido.")

# Destination chat
DESTINATION_CHAT_STR = os.environ.get("DESTINATION_CHAT", "").strip()
if not DESTINATION_CHAT_STR:
    raise ValueError("La variable de entorno DESTINATION_CHAT es obligatoria.")
try:
    DESTINATION_CHAT = int(DESTINATION_CHAT_STR)
except ValueError:
    DESTINATION_CHAT = DESTINATION_CHAT_STR

SEND_INTERVAL_SECONDS: int = int(os.environ.get("SEND_INTERVAL_SECONDS", 30))
DESTINATION_CHAT_ID: Optional[int] = DESTINATION_CHAT if isinstance(DESTINATION_CHAT, int) else None
DESTINATION_REFRESH_PENDING: bool = False
BUTTON_URL = os.environ.get("BUTTON_URL", "")
BOT_TOKEN_2 = os.environ.get("BOT_TOKEN_2", "")
BOT2_CHAT_ID = os.environ.get("BOT2_CHAT_ID", "")

# Nombre del CSV en el repo. Se puede sobrescribir con la variable de entorno SCRAPP_DB_CSV_PATH
REPO_CSV_FILENAME: str = os.environ.get("SCRAPP_DB_CSV_PATH", "scrapp_db.csv")

CHATS_TO_SCRAPE: List[str] = [
    "@viplunaticscrapper",
    "@AsukaScr",
    "-1003636233013",
    "-1003075577632",
    "-1003658677167",
    "-1002408067156",
    "-1002271492504",
    "-1002328190486",
]

CHECK_INTERVAL: int = int(os.environ.get("CHECK_INTERVAL", 30))
DB_VOLUME: str = os.environ.get("DB_VOLUME", "/data")
DB_FILENAME: str = os.environ.get("DB_FILENAME", "scrapp.sqlite3")
CSV_FILE: str = "tarjetas.csv"

PROCESSABLE_LINK_DOMAINS: List[str] = ["telegram.ph", "telegra.ph", "te.legra.ph"]
IGNORED_LINK_DOMAINS: List[str] = []
MAX_LINK_CONTENT_SIZE: int = 1024 * 1024
LINK_REQUEST_TIMEOUT: int = 30
LINK_MAX_RETRIES: int = 3

COUNTRY_CODE_BY_NAME = {
    "ARGENTINA": "AR", "MEXICO": "MX", "UNITED STATES": "US", "SPAIN": "ES",
}

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

# --- SimpleDB ---
class SimpleDB:
    def __init__(self, db_volume: str = DB_VOLUME, db_filename: str = DB_FILENAME):
        self.db_path = self._resolve_db_path(db_volume, db_filename)
        self.data = self._load()

    @staticmethod
    def _resolve_db_path(db_volume: str, db_filename: str) -> str:
        if os.path.splitext(db_volume)[1].lower() in {".db", ".sqlite", ".sqlite3"}:
            db_path = db_volume
            db_dir = os.path.dirname(db_path)
        else:
            db_dir = db_volume
            db_path = os.path.join(db_dir, db_filename)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        return db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _migrate_database(self, conn: sqlite3.Connection) -> None:
        try:
            cursor = conn.execute("PRAGMA table_info(processed_cards)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'country' not in columns:
                conn.execute("ALTER TABLE processed_cards ADD COLUMN country TEXT DEFAULT 'Desconocido'")
                conn.commit()
        except Exception:
            pass

    def _load(self) -> Dict[str, Any]:
        try:
            with self._connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS last_ids (
                        chat_id TEXT PRIMARY KEY,
                        message_id INTEGER NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stats (
                        key TEXT PRIMARY KEY,
                        value INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processed_cards (
                        card_data TEXT PRIMARY KEY,
                        processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        source_info TEXT,
                        country TEXT DEFAULT 'Desconocido'
                    )
                """)
                conn.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_cards', 0)")
                conn.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_scans', 0)")
                conn.commit()
                self._migrate_database(conn)
        except sqlite3.Error as e:
            logger.error(f"DB init error: {e}")
        return self._snapshot()

    def _snapshot(self) -> Dict[str, Any]:
        snapshot = {"last_ids": {}, "stats": {"total_cards": 0, "total_scans": 0}, "processed_cards": []}
        try:
            with self._connect() as conn:
                snapshot["last_ids"] = {row["chat_id"]: row["message_id"] for row in conn.execute("SELECT chat_id, message_id FROM last_ids")}
                snapshot["stats"] = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM stats")}
                snapshot["processed_cards"] = [row["card_data"] for row in conn.execute("SELECT card_data FROM processed_cards ORDER BY processed_at DESC LIMIT 10000")]
        except sqlite3.Error as e:
            logger.error(f"DB snapshot error: {e}")
        return snapshot

    def _refresh_cache(self) -> None:
        self.data = self._snapshot()

    def is_card_processed(self, card_data: str) -> bool:
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT 1 FROM processed_cards WHERE card_data = ?", (card_data,)).fetchone()
            return row is not None
        except sqlite3.Error:
            return card_data in self.data.get("processed_cards", [])

    def mark_card_processed(self, card_data: str, source_info: str = "", country: str = "Desconocido") -> None:
        try:
            with self._connect() as conn:
                conn.execute("INSERT OR IGNORE INTO processed_cards (card_data, source_info, country) VALUES (?, ?, ?)", (card_data, source_info, country))
                conn.commit()
            self._refresh_cache()
        except sqlite3.Error as e:
            logger.error(f"Error marking processed: {e}")

    def add_cards_stats(self, count: int = 1) -> None:
        try:
            with self._connect() as conn:
                conn.execute("INSERT INTO stats (key, value) VALUES ('total_cards', ?) ON CONFLICT(key) DO UPDATE SET value = value + excluded.value", (count,))
                conn.execute("INSERT INTO stats (key, value) VALUES ('total_scans', 1) ON CONFLICT(key) DO UPDATE SET value = value + 1")
                conn.commit()
            self._refresh_cache()
        except sqlite3.Error as e:
            logger.error(f"Error updating stats: {e}")

    def export_csv(self) -> str:
        export_dir = os.path.join(os.path.dirname(self.db_path), "exports")
        os.makedirs(export_dir, exist_ok=True)
        fd, export_path = tempfile.mkstemp(prefix="scrapp_db_", suffix=".csv", dir=export_dir, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as f, self._connect() as conn:
                writer = csv.writer(f)
                writer.writerow(["Card Data", "Source Info", "Country", "Processed At"])
                for row in conn.execute("SELECT card_data, COALESCE(source_info, '') as source_info, COALESCE(country, 'Desconocido') as country, processed_at FROM processed_cards ORDER BY processed_at DESC"):
                    writer.writerow([row["card_data"], row["source_info"], row["country"], row["processed_at"]])
        except Exception as e:
            logger.error(f"Export error: {e}")
            raise
        return export_path

# --- Helpers for cards ---
def mask_card_number(card_number: str) -> str:
    if 'x' in card_number.lower():
        return card_number
    if len(card_number) <= 4:
        return "X" * len(card_number)
    show_digits = min(12, len(card_number) - 4)
    return card_number[:show_digits] + "X" * (len(card_number) - show_digits)

def extract_cards(text: str) -> List[str]:
    if not text:
        return []
    cards = []
    patterns = [
        r'(\d{16})[|/\s]+(\d{2})[|/\s]+(\d{2,4})[|/\s]+(\d{3,4})',
        r'(\d{16})[|/\s]+(\d{2})[|/\s]+(\d{2,4})',
        r'(\d{12,15}x{1,4})[|/\s]+(\d{2})[|/\s]+(\d{2,4})',
    ]
    for pattern in patterns:
        for match in re.findall(pattern, text):
            if len(match) == 4:
                card_num, month, year, cvv = match
                year_norm = year[-2:]
                cards.append(f"{re.sub(r'\D', '', card_num)}|{month}|{year_norm}|{cvv[:3]}")
            elif len(match) == 3:
                card_num, month, year = match
                year_norm = year[-2:]
                cards.append(f"{re.sub(r'\D', '', card_num)}|{month}|{year_norm}|xxx")
    return list(set(cards))

# --- BIN DB loader (optional) ---
def load_bin_database(csv_path: str = CSV_FILE) -> Dict[str, Dict[str, str]]:
    bin_db: Dict[str, Dict[str, str]] = {}
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                bin_code = row.get("bin", "").strip()
                if bin_code:
                    bin_db[bin_code] = {k: (row.get(k, "").strip() if row.get(k) else "") for k in ("brand", "tipo", "nivel", "Banco", "país")}
    except FileNotFoundError:
        logger.warning(f"BIN CSV not found: {csv_path}")
    except Exception as e:
        logger.exception(f"Error loading BINs: {e}")
    return bin_db

BIN_DATABASE = load_bin_database()

db = SimpleDB()
OUTGOING_CARD_QUEUE: asyncio.Queue = asyncio.Queue()

# --- Pyrogram clients ---
user = Client("user_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING, workers=50)
app = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=50)

async def resolve_destination_chat(force_refresh: bool = False) -> Optional[int]:
    global DESTINATION_CHAT_ID, DESTINATION_REFRESH_PENDING
    if DESTINATION_CHAT_ID is not None and not force_refresh:
        return DESTINATION_CHAT_ID
    try:
        chat = await app.get_chat(DESTINATION_CHAT)
        DESTINATION_CHAT_ID = chat.id
        DESTINATION_REFRESH_PENDING = False
        return DESTINATION_CHAT_ID
    except Exception as e:
        DESTINATION_REFRESH_PENDING = True
        logger.error(f"Could not resolve destination chat: {e}")
        return None

async def deliver_card_message(message_content: str) -> bool:
    destination_chat_id = await resolve_destination_chat()
    if destination_chat_id is None:
        return False
    reply_markup = None
    if BUTTON_URL:
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("⭐ OLIMPO", url=BUTTON_URL)]])
    try:
        await app.send_message(destination_chat_id, message_content, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        return True
    except Exception as e:
        logger.error(f"Send failed: {e}")
        return False

async def send_card_immediately(card_data: str, source_info: str = "CSV") -> bool:
    if db.is_card_processed(card_data):
        return False
    # format message
    censored = mask_card_number(card_data.split('|')[0])
    message = f"<b>OLIMPO SCRAPPER</b>\n\n<b>Serie= <code>{html.escape(censored)}</code></b>"
    await OUTGOING_CARD_QUEUE.put((card_data, message, source_info, "Desconocido", 0))
    logger.info(f"Enqueued from CSV: {card_data}")
    return True

async def outgoing_card_sender() -> None:
    while True:
        card_data, message_content, source_info, country, attempts = await OUTGOING_CARD_QUEUE.get()
        try:
            if db.is_card_processed(card_data):
                pass
            else:
                sent = await deliver_card_message(message_content)
                if sent:
                    db.mark_card_processed(card_data, source_info, country)
                    db.add_cards_stats(1)
                    logger.info(f"Sent and marked: {card_data}")
                elif attempts < 3:
                    await OUTGOING_CARD_QUEUE.put((card_data, message_content, source_info, country, attempts+1))
        finally:
            OUTGOING_CARD_QUEUE.task_done()
            await asyncio.sleep(SEND_INTERVAL_SECONDS)

# --- CSV replay functions ---
def get_repo_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))

def load_cards_from_repo_csv(csv_filename: str = REPO_CSV_FILENAME) -> List[str]:
    cards: List[str] = []
    csv_path = os.path.join(get_repo_root(), csv_filename)
    if not os.path.exists(csv_path):
        logger.debug(f"CSV not found at {csv_path}")
        return cards
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return cards
            field_map = {name.lower(): name for name in reader.fieldnames}
            key = None
            for candidate in ("card data", "card_data", "carddata", "card"):
                if candidate in field_map:
                    key = field_map[candidate]
                    break
            if not key:
                return cards
            for row in reader:
                raw = (row.get(key) or "").strip()
                if raw:
                    cards.append(raw)
    except Exception as e:
        logger.exception(f"Error reading CSV: {e}")
    logger.info(f"Loaded {len(cards)} cards from {csv_path}")
    return cards

async def replay_cards_from_csv_loop(csv_filename: str = REPO_CSV_FILENAME) -> None:
    logger.info("Starting CSV replay loop...")
    while True:
        try:
            cards = load_cards_from_repo_csv(csv_filename)
            for card in cards:
                if db.is_card_processed(card):
                    continue
                await send_card_immediately(card, "CSV import")
                await asyncio.sleep(SEND_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in CSV replay loop")
        await asyncio.sleep(CHECK_INTERVAL)

# --- Bot commands (minimal) ---
@app.on_message(filters.command("test") & filters.private)
async def test_cmd(client, message: Message):
    test_card = "4207670324511073|02|2030|816"
    success = await send_card_immediately(test_card, "test")
    await message.reply("Enqueued" if success else "Not enqueued")

# --- Startup ---
async def start_user_client() -> bool:
    if not SESSION_STRING:
        logger.warning("SESSION_STRING not configured; user client disabled")
        return False
    try:
        await user.start()
        return True
    except Exception:
        logger.exception("Failed to start user client")
        return False

async def main() -> None:
    sender_task = None
    csv_task = None
    scanner_task = None
    try:
        await app.start()
        await resolve_destination_chat(force_refresh=True)
        sender_task = asyncio.create_task(outgoing_card_sender())
        csv_task = asyncio.create_task(replay_cards_from_csv_loop(REPO_CSV_FILENAME))
        if await start_user_client():
            scanner_task = asyncio.create_task(asyncio.sleep(0))
        await asyncio.Event().wait()
    finally:
        for t in (scanner_task, sender_task, csv_task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        if user.is_connected:
            await user.stop()
        if app.is_connected:
            await app.stop()

if __name__ == "__main__":
    try:
        APP_LOOP.run_until_complete(main())
    finally:
        APP_LOOP.run_until_complete(APP_LOOP.shutdown_asyncgens())
        APP_LOOP.close()
