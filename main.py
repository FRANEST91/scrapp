# (Contenido completo del archivo corregido)
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
from pyrogram.types import InlineKeyboardButton, ReplyKeyboardMarkup, Message, InlineKeyboardMarkup
from typing import Dict, List, Optional, Any, Union

APP_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(APP_LOOP)

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, ReplyKeyboardMarkup, Message

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

# Configurar el chat de destino. Acepta ID numérico, @username o enlace t.me.
DESTINATION_CHAT_STR = os.environ.get("DESTINATION_CHAT", "").strip()
if not DESTINATION_CHAT_STR:
    raise ValueError("La variable de entorno DESTINATION_CHAT es obligatoria.")

DESTINATION_CHAT: Union[int, str]
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

CHATS_TO_SCRAPE: List[str] = [
    "@viplunaticscrapper",
    "@AsukaScr",
    "-1003636233013",
    "-1003075577632",
    "-1003658677167",
    "-1002408067156",
    "-1002271492504",
    "-1002328190486"
]

CHANNEL_MAPPING: Dict[str, Optional[Dict[str, str]]] = {
    "-1002328190486": {"destination": "DESTINATION_CHAT_ID"}
}
    
CHECK_INTERVAL: int = int(os.environ.get("CHECK_INTERVAL", 30))
DB_VOLUME: str = os.environ.get("DB_VOLUME", "/data")
DB_FILENAME: str = os.environ.get("DB_FILENAME", "scrapp.sqlite3")
CSV_FILE: str = "tarjetas.csv"
REPO_CSV_FILENAME: str = "scrapp_db.csv"

# Dominios que deben ser procesados para extraer tarjetas
PROCESSABLE_LINK_DOMAINS: List[str] = [
    "telegram.ph",
    "telegra.ph",
    "te.legra.ph"
]

# Dominios que deben ser ignorados (si se quiere mantener esta funcionalidad)
IGNORED_LINK_DOMAINS: List[str] = [
    # Aquí se pueden agregar dominios que se quieren ignorar completamente
]

# Configuración de scraping de enlaces
MAX_LINK_CONTENT_SIZE: int = 1024 * 1024  # 1MB máximo para contenido de enlaces
LINK_REQUEST_TIMEOUT: int = 30  # 30 segundos timeout para requests HTTP (aumentado para telegra.ph)
LINK_MAX_RETRIES: int = 3  # Máximo de reintentos para scraping de enlaces

COUNTRY_CODE_BY_NAME = {
    "ARGENTINA": "AR", "AUSTRALIA": "AU", "AUSTRIA": "AT", "BANGLADESH": "BD", "BELGIUM": "BE",
    "BRAZIL": "BR", "BULGARIA": "BG", "CANADA": "CA", "CHILE": "CL", "CHINA": "CN", 
    "COLOMBIA": "CO", "COSTA RICA": "CR", "CROATIA": "HR", "DENMARK": "DK", "DOMINICAN REPUBLIC": "DO",
    "ECUADOR": "EC", "EGYPT": "EG", "FINLAND": "FI", "FRANCE": "FR", "GERMANY": "DE", "GREECE": "GR",
    "GUATEMALA": "GT", "HONG KONG": "HK", "INDIA": "IN", "INDONESIA": "ID", "IRELAND": "IE",
    "ITALY": "IT", "JAPAN": "JP", "KOREA, REPUBLIC OF": "KR", "LEBANON": "LB", "MALAYSIA": "MY",
    "MEXICO": "MX", "NETHERLANDS": "NL", "NIGERIA": "NG", "NORWAY": "NO", "PAKISTAN": "PK",
    "PANAMA": "PA", "PERU": "PE", "PHILIPPINES": "PH", "POLAND": "PL", "PORTUGAL": "PT",
    "ROMANIA": "RO", "RUSSIAN FEDERATION": "RU", "SAUDI ARABIA": "SA", "SERBIA": "RS",
    "SINGAPORE": "SG", "SOUTH AFRICA": "ZA", "SPAIN": "ES", "SWEDEN": "SE", "SWITZERLAND": "CH",
    "TAIWAN, PROVINCE OF CHINA": "TW", "THAILAND": "TH", "TURKEY": "TR", "UKRAINE": "UA",
    "UNITED ARAB EMIRATES": "AE", "UNITED KINGDOM": "GB", "UNITED STATES": "US",
    "VENEZUELA, BOLIVARIAN REPUBLIC OF": "VE", "VIET NAM": "VN",
}


def country_flag(country_name: str) -> str:
    """Devuelve la bandera emoji para países conocidos en la base BIN."""
    country_code = COUNTRY_CODE_BY_NAME.get((country_name or "").strip().upper())
    if not country_code:
        return ""
    return "".join(chr(ord(char) + 127397) for char in country_code)

# ---------- START: CSV-REPLAY (funciones añadidas) ----------

def get_repo_root() -> str:
    """Devuelve la ruta del directorio donde está este script (raíz del repo en ejecución)."""
    return os.path.dirname(os.path.abspath(__file__))


def load_cards_from_repo_csv(csv_filename: str = REPO_CSV_FILENAME) -> List[str]:
    """
    Lee el archivo scrapp_db.csv situado junto al script y devuelve la lista
    de valores de la columna 'card data' (búsqueda case-insensitive).
    """
    cards: List[str] = []
    csv_path = os.path.join(get_repo_root(), csv_filename)

    if not os.path.exists(csv_path):
        logger.warning(f"📁 CSV de tarjetas no encontrado en: {csv_path}")
        return cards

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                logger.warning(f"⚠️ CSV '{csv_path}' no tiene cabeceras.")
                return cards

            # Buscar la columna "card data" de forma case-insensitive
            field_map = {name.lower(): name for name in reader.fieldnames}
            key = None
            for candidate in ("card data", "card_data", "carddata", "card"):
                if candidate in field_map:
                    key = field_map[candidate]
                    break

            if not key:
                logger.warning(
                    f"⚠️ No se encontró la columna 'card data' en '{csv_path}'. "
                    f"Cabeceras disponibles: {reader.fieldnames}"
                )
                return cards

            for row in reader:
                raw = (row.get(key) or "").strip()
                if raw:
                    cards.append(raw)

    except Exception as e:
        logger.exception(f"❌ Error leyendo CSV '{csv_path}': {e}")

    logger.info(f"✅ Cargadas {len(cards)} tarjetas desde CSV: {csv_path}")
    return cards


async def replay_cards_from_csv_loop(csv_filename: str = REPO_CSV_FILENAME) -> None:
    """
    Tarea de fondo que lee periódicamente scrapp_db.csv y encola las tarjetas
    no procesadas para ser enviadas al destination chat. Respeta SEND_INTERVAL_SECONDS
    entre encolados para evitar saturar la cola.
    """
    logger.info("🔁 Iniciando tarea de replay desde CSV (scrapp_db.csv)...")
    while True:
        try:
            cards = load_cards_from_repo_csv(csv_filename)
            if not cards:
                logger.debug("📭 No hay tarjetas en CSV para reenviar.")
            else:
                logger.info(f"🔁 Procesando {len(cards)} tarjetas desde CSV...")
                for card in cards:
                    # Saltar las ya procesadas
                    if db.is_card_processed(card):
                        logger.debug(f"↩️ Tarjeta ya procesada (CSV): {card}")
                        continue

                    enqueued = await send_card_immediately(card, "CSV import")
                    if enqueued:
                        logger.info(f"📥 Encolada tarjeta desde CSV: {card}")
                    else:
                        logger.debug(f"⚠️ No encolada (ya procesada o formato inválido): {card}")

                    # Pausa entre encolados para no saturar la cola/salida
                    await asyncio.sleep(SEND_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            logger.info("⏹️ Tarea CSV replay cancelada.")
            raise
        except Exception as e:
            logger.exception(f"❌ Error en replay_cards_from_csv_loop: {e}")

        # Esperar antes del siguiente ciclo de lectura del CSV
        await asyncio.sleep(CHECK_INTERVAL)

# ---------- END: CSV-REPLAY ----------

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

async def notify_bot2(card_data: str, source_info: str) -> bool:
    try:
        if not BOT_TOKEN_2 or not BOT2_CHAT_ID:
            logger.warning("❌ Variables BOT_TOKEN_2 o BOT2_CHAT_ID no configuradas")
            return False

        message_text = f"💳|{card_data}|{source_info}"

        await app.send_message(BOT2_CHAT_ID, message_text)

        logger.info(f"✅ Datos enviados exitosamente al Bot 2: {source_info[:50]}...")
        return True

    except Exception as e:
        logger.error(f"❌ Error al notificar al Bot 2: {e}")
        return False

# --- Clases y Funciones Auxiliares ---

class SimpleDB:
    """
    Maneja la persistencia de datos del bot en una base SQLite ubicada en el
    volumen persistente de Railway (DB_VOLUME, por defecto /data).
    """

    def __init__(self, db_volume: str = DB_VOLUME, db_filename: str = DB_FILENAME):
        self.db_path = self._resolve_db_path(db_volume, db_filename)
        self.data = self._load()

    @staticmethod
    def _resolve_db_path(db_volume: str, db_filename: str) -> str:
        """Resuelve DB_VOLUME como directorio persistente y devuelve el archivo SQLite."""
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
        """Abre una conexión SQLite con filas accesibles por nombre."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _migrate_database(self, conn: sqlite3.Connection) -> None:
        """Realiza migraciones necesarias en la base de datos."""
        try:
            # Verificar si la columna 'country' existe en processed_cards
            cursor = conn.execute("PRAGMA table_info(processed_cards)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'country' not in columns:
                logger.info("Migrando: Agregando columna 'country' a tabla processed_cards...")
                conn.execute("ALTER TABLE processed_cards ADD COLUMN country TEXT DEFAULT 'Desconocido'")
                conn.commit()
                logger.info("✅ Migración completada: columna 'country' agregada")
        except Exception as e:
            logger.warning(f"⚠️ Error durante migración: {e}")

    def _load(self) -> Dict[str, Any]:
        """Inicializa la DB SQLite y devuelve una vista cacheada compatible con el bot."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS last_ids (
                        chat_id TEXT PRIMARY KEY,
                        message_id INTEGER NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stats (
                        key TEXT PRIMARY KEY,
                        value INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_cards (
                        card_data TEXT PRIMARY KEY,
                        processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        source_info TEXT,
                        country TEXT DEFAULT 'Desconocido'
                    )
                    """
                )
                # Tablas adicionales referenciadas por el snapshot / comandos
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS country_cards (
                        country_data TEXT,
                        processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_links (
                        url TEXT PRIMARY KEY,
                        processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_cards', 0)")
                conn.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_scans', 0)")
                conn.commit()
                
                # Ejecutar migraciones
                self._migrate_database(conn)
        except sqlite3.Error as e:
            logger.error(f"❌ Error inicializando DB SQLite en '{self.db_path}': {e}")

        return self._snapshot()

    def _snapshot(self) -> Dict[str, Any]:
        """Lee los datos actuales de SQLite en el formato usado por los comandos."""
        snapshot: Dict[str, Any] = {
            "last_ids": {},
            "stats": {"total_cards": 0, "total_scans": 0},
            "processed_cards": [],
            "processed_links": [],
            "country_cards": [],
        }

        try:
            with self._connect() as conn:
                snapshot["last_ids"] = {
                    row["chat_id"]: row["message_id"]
                    for row in conn.execute("SELECT chat_id, message_id FROM last_ids")
                }
                snapshot["stats"] = {
                    row["key"]: row["value"]n                    for row in conn.execute("SELECT key, value FROM stats")
                }
                snapshot["country_cards"] = [
                    row["country_data"]
                    for row in conn.execute(
                        "SELECT country_data FROM country_cards ORDER BY processed_at DESC LIMIT 10000"
                    )
                ]
                snapshot["processed_links"] = [
                    row["url"]
                    for row in conn.execute(
                        "SELECT url FROM processed_links ORDER BY processed_at DESC LIMIT 10000"
                    )
                ]
                snapshot["processed_cards"] = [
                    row["card_data"]
                    for row in conn.execute(
                        "SELECT card_data FROM processed_cards ORDER BY processed_at DESC LIMIT 10000"
                    )
                ]
        except sqlite3.Error as e:
            logger.error(f"❌ Error leyendo DB SQLite desde '{self.db_path}': {e}")

        return snapshot

    def _refresh_cache(self) -> None:
        """Sincroniza la vista en memoria después de cada escritura."""
        self.data = self._snapshot()

    def get_last_id(self, chat_id: int) -> int:
        """Obtiene el último ID de mensaje procesado para un chat específico."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT message_id FROM last_ids WHERE chat_id = ?",
                    (str(chat_id),),
                ).fetchone()
            return int(row["message_id"]) if row else 0
        except sqlite3.Error as e:
            logger.error(f"❌ Error consultando last_id para chat {chat_id}: {e}")
            return 0

    def set_last_id(self, chat_id: int, message_id: int) -> None:
        """Establece el último ID de mensaje procesado para un chat específico."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO last_ids (chat_id, message_id)
                    VALUES (?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET message_id = excluded.message_id
                    """,
                    (str(chat_id), message_id),
                )
                conn.commit()
            self._refresh_cache()
        except sqlite3.Error as e:
            logger.error(f"❌ Error guardando last_id para chat {chat_id}: {e}")

    def is_card_processed(self, card_data: str) -> bool:
        """Verifica si la tarjeta ya fue procesada usando los datos originales."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 FROM processed_cards WHERE card_data = ?",
                    (card_data,),
                ).fetchone()
            return row is not None
        except sqlite3.Error as e:
            logger.error(f"❌ Error verificando tarjeta procesada: {e}")
            return card_data in self.data.get("processed_cards", [])

    def mark_card_processed(self, card_data: str, source_info: str = "", country: str = "Desconocido") -> None:
        """Marca una tarjeta como procesada guardando los datos originales y el país."""
        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO processed_cards (card_data, source_info, country) VALUES (?, ?, ?)",
                    (card_data, source_info, country),
                )
                conn.commit()
            self._refresh_cache()
        except sqlite3.Error as e:
            logger.error(f"❌ Error marcando tarjeta procesada: {e}")

    def add_cards_stats(self, count: int = 1) -> None:
        """Actualiza las estadísticas de tarjetas procesadas y escaneos."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO stats (key, value) VALUES ('total_cards', ?)
                    ON CONFLICT(key) DO UPDATE SET value = value + excluded.value
                    """,
                    (count,),
                )
                conn.execute(
                    """
                    INSERT INTO stats (key, value) VALUES ('total_scans', 1)
                    ON CONFLICT(key) DO UPDATE SET value = value + 1
                    """
                )
                conn.commit()
            self._refresh_cache()
        except sqlite3.Error as e:
            logger.error(f"❌ Error actualizando estadísticas: {e}")

    def export_csv(self) -> str:
        """Exporta toda la información persistida a un CSV temporal dentro de DB_VOLUME."""
        export_dir = os.path.join(os.path.dirname(self.db_path), "exports")
        os.makedirs(export_dir, exist_ok=True)

        fd, export_path = tempfile.mkstemp(
            prefix="scrapp_db_",
            suffix=".csv",
            dir=export_dir,
            text=True,
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as f, self._connect() as conn:
                writer = csv.writer(f)
                writer.writerow(["Card Data", "Source Info", "Country", "Processed At"])

                # Usar COALESCE para manejar valores NULL
                for row in conn.execute(
                    """SELECT card_data, COALESCE(source_info, '') as source_info, 
                              COALESCE(country, 'Desconocido') as country, processed_at 
                       FROM processed_cards ORDER BY processed_at DESC"""
                ):
                    card_data = row["card_data"]
                    source_info = row["source_info"]
                    country = row["country"]
                    processed_at = row["processed_at"]
                    writer.writerow([card_data, source_info, country, processed_at])
                
                logger.info(f"✅ CSV exportado exitosamente a: {export_path}")

        except Exception as e:
            logger.error(f"❌ Error durante la exportación CSV: {e}")
            try:
                os.close(fd)
            except OSError:
                pass
            if os.path.exists(export_path):
                os.remove(export_path)
            raise

        return export_path

    def get_cards_by_country(self, country: str = None) -> List[Dict[str, str]]:
        """Obtiene las tarjetas procesadas filtradas por país. Si country es None, obtiene todas."""
        try:
            with self._connect() as conn:
                if country:
                    rows = conn.execute(
                        """SELECT card_data, COALESCE(source_info, '') as source_info, 
                                  COALESCE(country, 'Desconocido') as country, processed_at 
                           FROM processed_cards WHERE country = ? ORDER BY processed_at DESC LIMIT 10000""",
                        (country,)
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """SELECT card_data, COALESCE(source_info, '') as source_info, 
                                  COALESCE(country, 'Desconocido') as country, processed_at 
                           FROM processed_cards ORDER BY processed_at DESC LIMIT 10000"""
                    ).fetchall()
                
                return [
                    {
                        "card_data": row["card_data"],
                        "source_info": row["source_info"],
                        "country": row["country"],
                        "processed_at": row["processed_at"]
                    }
                    for row in rows
                ]
        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo tarjetas por país: {e}")
            return []

    def get_country_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas de tarjetas agrupadas por país."""
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """SELECT COALESCE(country, 'Desconocido') as country, COUNT(*) as count 
                       FROM processed_cards GROUP BY country ORDER BY count DESC"""
                ).fetchall()
                
                return {row["country"]: row["count"] for row in rows}
        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo estadísticas por país: {e}")
            return {}

def load_bin_database(csv_path: str = CSV_FILE) -> Dict[str, Dict[str, str]]:
    """
    Carga la base de datos de BINs desde un archivo CSV.
    El CSV debe tener columnas como 'bin', 'brand', 'tipo', 'nivel', 'Banco', 'país'.
    """
    bin_db: Dict[str, Dict[str, str]] = {}

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                bin_code = row.get("bin", "").strip()

                if bin_code:
                    bin_db[bin_code] = {
                        "brand": row.get("brand", "Desconocido").strip(),
                        "tipo": row.get("tipo", "Desconocido").strip(),
                        "nivel": row.get("nivel", "").strip(),
                        "banco": row.get("Banco", "Desconocido").strip(),
                        "pais": row.get("país", "Desconocido").strip(),
                        "bin": bin_code,
                    }

        logger.info(f"✅ Base de datos BIN cargada: {len(bin_db)} entradas")

    except FileNotFoundError:
        logger.warning(
            f"⚠️ Archivo CSV de BINs no encontrado: '{csv_path}'. "
            "El bot funcionará sin información de BIN."
        )

    except csv.Error as e:
        logger.warning(
            f"⚠️ Error al leer el archivo CSV de BINs '{csv_path}': {e}. "
            "El bot funcionará sin información de BIN."
        )

    except Exception:
        logger.exception(
            f"⚠️ Error inesperado al cargar BINs desde '{csv_path}'. "
            "El bot funcionará sin información de BIN."
        )

    return bin_db


def mask_card_number(card_number: str) -> str:
    """Enmascara una tarjeta guardando los primeros dígitos visibles."""
    if 'x' in card_number.lower():
        # Ya está enmascarada parcialmente, devolver como está
        return card_number
    
    if len(card_number) <= 4:
        return "X" * len(card_number)
    
    # Mostrar primeros 6-12 dígitos y enmascarar el resto
    show_digits = min(12, len(card_number) - 4)
    masked = card_number[:show_digits] + "X" * (len(card_number) - show_digits)
    return masked


def get_bin_info(card_number: str, bin_database: Dict[str, Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Obtiene información del BIN desde la base de datos proporcionada."""
    # Limpiar 'x' del número de tarjeta para extraer BIN
    clean_number = re.sub(r'[^0-9]', '', card_number.split('|')[0])
    
    # Intentar con BINs de longitud 6, 5 y 4.
    for length in [6, 5, 4]:
        if len(clean_number) >= length:
            bin_code = clean_number[:length]
            if bin_code in bin_database:
                return bin_database[bin_code]
    return None

def format_card_message(card_data: str, bin_database: Dict[str, Dict[str, str]]) -> Optional[tuple[str, str]]:
    """
    Formatea el mensaje de la tarjeta con la información del BIN.
    Maneja tanto tarjetas con CVV como sin CVV.
    Retorna una tupla (mensaje, país) o (None, None) si hay error.
    """
    parts = card_data.split("|")

    # Manejar tanto tarjetas con CVV como sin CVV
    if len(parts) == 4:
        card_num, month, year, cvv = parts
        has_cvv = True
    elif len(parts) == 3:
        card_num, month, year = parts
        cvv = "xxx"
        has_cvv = False
    else:
        logger.warning(f"Formato de tarjeta inválido: {card_data}")
        return None, None

    # Validaciones básicas
    if len(card_num) < 12 or len(month) != 2:
        logger.warning(f"Datos de tarjeta no válidos: {card_data}")
        return None, None

    # Extraer información del BIN (solo si la tarjeta tiene suficientes dígitos)
    bin_info = None
    clean_card_num = re.sub(r'[^0-9]', '', card_num.split('|')[0])
    if len(clean_card_num) >= 4:
        bin_info = get_bin_info(card_num, bin_database)

    # Censurar la tarjeta para mostrarla
    censored_card_num = mask_card_number(card_num)
    display_year = f"20{year}" if len(year) == 2 else year

    if has_cvv and cvv != "xxx":
        censored = f"{censored_card_num}|{month}|{display_year}|{cvv[:3]}"
        cvv_display = f"{cvv[:3]}"
    else:
        censored = f"{censored_card_num}|{month}|{display_year}|xxx"
        cvv_display = "No disponible"

    # Extraer información del BIN, con valores por defecto
    tipo = "Desconocido"
    brand = "Desconocido"
    nivel = ""
    banco = "Desconocido"
    pais = "Desconocido"
    bin_code_found = "Desconocido"

    if bin_info:
        tipo = bin_info.get("tipo", "Desconocido")
        brand = bin_info.get("brand", "Desconocido")
        nivel = bin_info.get("nivel", "")
        banco = bin_info.get("banco", "Desconocido")
        pais = bin_info.get("pais", "Desconocido")
        bin_code_found = bin_info.get("bin", "Desconocido")

    country_with_flag = f"{pais} {country_flag(pais)}".strip()

    message = (
        f"<b>OLIMPO SCRAPPER</b>\n\n"
        f"<b>#<code>{html.escape(bin_code_found)}</code></b>\n"
        f"<b>━━━━━━━━</b>\n"
        f"<b>Serie= <code>{html.escape(censored)}</code></b>\n"
        f"<b>Bin= <code>{html.escape(bin_code_found)}</code></b>\n"
        f"<b>Banco= {html.escape(banco)}</b>\n"
        f"<b>Marca= {html.escape(brand)}</b>\n"
        f"<b>Tipo= {html.escape(tipo)}</b>\n"
        f"<b>Nivel= {html.escape(nivel)}</b>\n"
        f"<b>País= {html.escape(country_with_flag)}</b>\n"
        f"<b>━━━━━━━━</b>\n"
        f"<b>DESARROLLADO POR\n"
        f"<b><code>@MrMxyzptlk04</code></b>\n" 
        f"<b><code>@Chack0071</code></b>\n"
        f"<b>━━━━━━━━</b>\n"
    )

    return message, pais

def extract_urls(text: str) -> List[str]:
    """Extrae URLs HTTP/HTTPS de un texto."""
    if not text:
        return []

    # Patrón mejorado para capturar URLs con diferentes formatos
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[^
