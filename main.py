import os
import asyncio
import logging
import time
import json
import aiohttp
import asyncpg
from aiohttp import web
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

logging.basicConfig(level=logging.INFO)

# ========== ПЕРЕМЕННЫЕ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFFY_API_KEY = os.getenv("TRAFFY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")

DEFAULT_MAX_SPONSORS = 14
MAX_LIMIT = 30

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pool

# ========== FSM ==========
class AdminState(StatesGroup):
    waiting_for_max_sponsors = State()
    waiting_for_sponsor_reward = State()
    waiting_for_broadcast = State()

class WithdrawState(StatesGroup):
    waiting_for_username = State()

# ========== БАЗА ДАННЫХ ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            balance DOUBLE PRECISION DEFAULT 0.0,
            total_earned DOUBLE PRECISION DEFAULT 0.0,
            referrer_id BIGINT,
            referrals_count INTEGER DEFAULT 0,
            is_activated INTEGER DEFAULT 0,
            created_at DOUBLE PRECISION,
            last_bonus TEXT DEFAULT ''
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            service TEXT,
            assignment_id TEXT,
            link TEXT,
            status TEXT DEFAULT 'unsubscribed',
            signature TEXT,
            need_check INTEGER DEFAULT 1,
            created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_log (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            service TEXT,
            link TEXT,
            status TEXT,
            created_at DOUBLE PRECISION
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_status (
            user_id BIGINT PRIMARY KEY,
            status TEXT,
            reason TEXT,
            updated_at DOUBLE PRECISION
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_cache (
            cache_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            expires_at DOUBLE PRECISION NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            amount DOUBLE PRECISION,
            status TEXT DEFAULT 'pending',
            created_at DOUBLE PRECISION
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS manual_checks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            sponsors TEXT,
            status TEXT DEFAULT 'pending',
            created_at DOUBLE PRECISION
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            service TEXT,
            error_type TEXT,
            message TEXT,
            created_at DOUBLE PRECISION
        )""")
        await db.execute("INSERT INTO settings (key, value) VALUES ('max_sponsors', '14') ON CONFLICT (key) DO NOTHING")
        await db.execute("INSERT INTO settings (key, value) VALUES ('sponsor_reward', '0.5') ON CONFLICT (key) DO NOTHING")
    logging.info("✅ БД инициализирована")

async def get_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return dict(row) if row else None

async def get_max_sponsors():
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key='max_sponsors'")
        return int(row["value"]) if row else DEFAULT_MAX_SPONSORS

async def get_sponsor_reward():
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key='sponsor_reward'")
        return float(row["value"]) if row else 0.5

async def log_error(user_id: int, service: str, error_type: str, message: str):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO error_log (user_id, service, error_type, message, created_at) VALUES ($1, $2, $3, $4, $5)",
                user_id, service, error_type, message, time.time()
            )
    except Exception as e:
        logging.error(f"log_error: {e}")

async def register_user(user_id: int, username: str, referrer_id: int = None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id = $1", user_id)
        if exists:
            return
        now = time.time()
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1, $2, $3, $4)",
            user_id, username, referrer_id, now
        )

async def update_balance(user_id: int, amount: float):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            UPDATE users 
            SET balance = balance + $1, 
                total_earned = total_earned + CASE WHEN $1 > 0 THEN $1 ELSE 0 END 
            WHERE user_id = $2
        """, amount, user_id)

async def log_sponsor(user_id: int, service: str, link: str, status: str):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_log (user_id, service, link, status, created_at) VALUES ($1, $2, $3, $4, $5)",
                user_id, service, link, status, time.time()
            )
    except Exception as e:
        logging.error(f"log_sponsor: {e}")

async def log_sponsor_status(user_id: int, status: str, reason: str = ""):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO sponsor_status (user_id, status, reason, updated_at) 
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET status=$2, reason=$3, updated_at=$4
            """, user_id, status, reason, time.time())
    except Exception as e:
        logging.error(f"log_sponsor_status: {e}")

async def db_execute(query, params):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(query, *params)# ========== API СПОНСОРОВ ==========
async def get_piarflow_sponsors(user_id: int, chat_id: int, max_sponsors: int = DEFAULT_MAX_SPONSORS):
    if not PIARFLOW_API_KEY:
        await log_error(user_id, "piarflow", "no_api_key", "API ключ не установлен")
        return []
    url = "https://piarflow.com/v1/sponsors"
    headers = {"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
    payload = {"user_id": user_id, "chat_id": chat_id, "max_sponsors": max_sponsors}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "ok":
                        return data.get("sponsors", [])
    except Exception as e:
        await log_error(user_id, "piarflow", "api_error", str(e))
        logging.error(f"Piarflow error: {e}")
    return []

async def check_piarflow_sponsors(user_id: int, links: list):
    if not PIARFLOW_API_KEY or not links:
        return []
    url = "https://piarflow.com/v1/sponsors/check"
    headers = {"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
    payload = {"user_id": user_id, "links": links}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "ok":
                        return data.get("sponsors", [])
    except Exception as e:
        logging.error(f"Piarflow check error: {e}")
    return []

async def get_flyer_tasks(user_id: int, language_code: str = "ru"):
    if not FLYER_API_KEY:
        await log_error(user_id, "flyer", "no_api_key", "API ключ не установлен")
        return []
    url = "https://api.flyerhubs.com/v1/tasks"
    payload = {"key": FLYER_API_KEY, "user_id": user_id, "language_code": language_code, "limit": DEFAULT_MAX_SPONSORS}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if not data.get("error"):
                        return data.get("result", [])
    except Exception as e:
        await log_error(user_id, "flyer", "api_error", str(e))
        logging.error(f"Flyer error: {e}")
    return []

async def check_flyer_task(user_id: int, signature: str):
    if not FLYER_API_KEY or not signature:
        return False
    url = "https://api.flyerhubs.com/v1/check_task"
    payload = {"key": FLYER_API_KEY, "signature": signature}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if not data.get("error"):
                        return data.get("result") in ["completed", "subscribed"]
    except Exception as e:
        logging.error(f"Flyer check error: {e}")
    return False

async def get_tgrass_offers(user_id: int, username: str, lang: str = "ru", is_premium: bool = False):
    if not TGRASS_API_KEY:
        await log_error(user_id, "tgrass", "no_api_key", "API ключ не установлен")
        return []
    url = "https://tgrass.space/offers"
    headers = {"accept": "application/json", "Content-Type": "application/json", "Auth": TGRASS_API_KEY}
    payload = {"tg_user_id": user_id, "tg_login": username or "", "lang": lang or "ru", "is_premium": is_premium}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "not_ok":
                        return data.get("offers", [])
    except Exception as e:
        await log_error(user_id, "tgrass", "api_error", str(e))
        logging.error(f"TGrass error: {e}")
    return []

async def check_tgrass_subscription(user_id: int):
    if not TGRASS_API_KEY:
        return False
    url = "https://tgrass.space/offers"
    headers = {"accept": "application/json", "Content-Type": "application/json", "Auth": TGRASS_API_KEY}
    payload = {"tg_user_id": user_id}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("status") == "ok"
    except Exception as e:
        logging.error(f"TGrass check error: {e}")
    return False

async def get_traffy_tasks(user_id: int, limit: int = DEFAULT_MAX_SPONSORS, first_name: str = None, username: str = None, language_code: str = "ru"):
    if not TRAFFY_API_KEY:
        await log_error(user_id, "traffy", "no_api_key", "API ключ не установлен")
        return []
    url = "https://traffy.ai/publisher/tasks"
    headers = {"x-publisher-api-key": TRAFFY_API_KEY, "Content-Type": "application/json"}
    payload = {"telegram_id": user_id, "limit": limit}
    if first_name: payload["first_name"] = first_name
    if username: payload["username"] = username
    if language_code: payload["language_code"] = language_code
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status in [200, 201]:
                    data = await resp.json()
                    if data.get("ok") and data.get("tasks"):
                        return data.get("tasks", [])
    except Exception as e:
        await log_error(user_id, "traffy", "api_error", str(e))
        logging.error(f"Traffy error: {e}")
    return []

async def check_traffy_tasks(user_id: int, assignment_ids: list):
    if not TRAFFY_API_KEY or not assignment_ids:
        return []
    url = "https://traffy.ai/publisher/tasks/check"
    headers = {"x-publisher-api-key": TRAFFY_API_KEY, "Content-Type": "application/json"}
    payload = {"telegram_id": user_id, "assignment_ids": assignment_ids}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        return data.get("results", [])
    except Exception as e:
        logging.error(f"Traffy check error: {e}")
    return []

# ========== BOTOHUB (ПОЧИНЕННЫЙ) ==========
async def get_botohub_tasks(user_id: int):
    """Botohub — универсальный парсер как в KryptoBobus."""
    if not BOTOHUB_API_KEY:
        await log_error(user_id, "botohub", "no_api_key", "API ключ не установлен")
        return []
    
    url = "https://botohub.me/get-tasks"
    headers = {"Content-Type": "application/json", "Auth": BOTOHUB_API_KEY}
    payload = {"chat_id": user_id}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=15) as resp:
                body = await resp.text()
                logging.info(f"Botohub RAW: status={resp.status}, body={body[:500]}")
                
                if resp.status != 200:
                    await log_error(user_id, "botohub", "http_error", f"HTTP {resp.status}: {body[:200]}")
                    return []
                
                data = await resp.json()
                
                if data.get("skip") or data.get("completed"):
                    logging.info(f"Botohub: skip={data.get('skip')}, completed={data.get('completed')}")
                    return []
                
                # Универсальный поиск массива
                tasks = None
                for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
                    v = data.get(field)
                    if isinstance(v, list):
                        tasks = v
                        break
                
                if not tasks:
                    logging.warning(f"Botohub: массив не найден в ответе: {data}")
                    return []
                
                # Обрабатываем — строка или объект
                links_out = []
                pool = await get_pool()
                for t in tasks:
                    link = None
                    aid = None
                    if isinstance(t, str):
                        link = t
                        aid = t
                    elif isinstance(t, dict):
                        link = t.get("link") or t.get("url") or t.get("target_link") or t.get("target")
                        aid = t.get("id") or t.get("task_id") or t.get("assignment_id") or link
                    
                    if link:
                        links_out.append(link)
                        try:
                            async with pool.acquire() as db:
                                await db.execute(
                                    "INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) "
                                    "VALUES ($1, $2, $3, $4, $5, $6) "
                                    "ON CONFLICT (user_id, service, assignment_id) DO NOTHING",
                                    user_id, "botohub", str(aid) if aid else link, link, "unsubscribed", time.time()
                                )
                        except Exception as e:
                            logging.error(f"Botohub insert: {e}")
                        await log_sponsor(user_id, "botohub", link, "выдан")
                
                logging.info(f"Botohub: выдано {len(links_out)} заданий")
                return links_out
    except Exception as e:
        await log_error(user_id, "botohub", "api_error", str(e))
        logging.error(f"Botohub error: {e}")
    return []

async def check_botohub_tasks(user_id: int):
    if not BOTOHUB_API_KEY:
        return False
    url = "https://botohub.me/get-tasks"
    headers = {"Content-Type": "application/json", "Auth": BOTOHUB_API_KEY}
    payload = {"chat_id": user_id}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return bool(data.get("completed"))
    except Exception as e:
        logging.error(f"Botohub check error: {e}")
    return False

# ========== TRAFSLY ==========
async def get_trafsly_sponsors(user_id: int, chat_id: int = None, first_name: str = None, username: str = None, language_code: str = "ru", is_premium: bool = False, max_sponsors: int = DEFAULT_MAX_SPONSORS):
    if not TRAFSLY_API_KEY:
        await log_error(user_id, "trafsly", "no_api_key", "API ключ не установлен")
        return []
    
    url = "https://api.trafsly.com/api/v1/get-sponsors"
    headers = {"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
    payload = {
        "user_id": user_id,
        "max_sponsors": max_sponsors,
        "language_code": language_code,
        "is_premium": is_premium
    }
    if chat_id: payload["chat_id"] = chat_id
    if first_name: payload["first_name"] = first_name
    if username: payload["username"] = username
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Trafsly ответ: {data}")
                    
                    if data.get("status") == "ok":
                        return []
                    
                    if data.get("status") == "warning":
                        sponsors = data.get("sponsors", [])
                        pool = await get_pool()
                        for s in sponsors:
                            link = s.get("link")
                            ads_id = s.get("ads_id")
                            need_check = 1 if ads_id else 0
                            if link:
                                try:
                                    async with pool.acquire() as db:
                                        await db.execute(
                                            "INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, need_check, created_at) "
                                            "VALUES ($1, $2, $3, $4, $5, $6, $7) "
                                            "ON CONFLICT (user_id, service, assignment_id) DO NOTHING",
                                            user_id, "trafsly", str(ads_id) if ads_id else link, link, "unsubscribed", need_check, time.time()
                                        )
                                except Exception as e:
                                    logging.error(f"Trafsly insert: {e}")
                        return sponsors
    except Exception as e:
        await log_error(user_id, "trafsly", "api_error", str(e))
        logging.error(f"Ошибка Trafsly: {e}")
    return []

async def check_trafsly_sponsors(user_id: int, assignment_ids: list):
    if not TRAFSLY_API_KEY or not assignment_ids:
        return []
    
    url = "https://api.trafsly.com/api/v1/confirm-subscription"
    headers = {"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
    pool = await get_pool()
    
    results = []
    for ads_id in assignment_ids:
        payload = {"user_id": user_id, "ads_id": int(ads_id)}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logging.info(f"Trafsly проверка ads_id {ads_id}: {data}")
                        
                        if data.get("subscribed") == True:
                            async with pool.acquire() as db:
                                await db.execute(
                                    "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'trafsly' AND assignment_id = $2",
                                    user_id, str(ads_id)
                                )
                            results.append({"ads_id": ads_id, "status": "subscribed"})
                        elif data.get("subscribed") == False:
                            if data.get("status") == "error" or "not verified" in data.get("message", "").lower() or "too many" in data.get("message", "").lower():
                                logging.warning(f"Trafsly ошибка для {ads_id}, засчитываем принудительно")
                                async with pool.acquire() as db:
                                    await db.execute(
                                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'trafsly' AND assignment_id = $2",
                                        user_id, str(ads_id)
                                    )
                                results.append({"ads_id": ads_id, "status": "subscribed"})
                            else:
                                results.append({"ads_id": ads_id, "status": "unsubscribed"})
                        elif data.get("message") == "User is banned":
                            async with pool.acquire() as db:
                                await db.execute(
                                    "DELETE FROM sponsor_tasks WHERE user_id = $1 AND service = 'trafsly'",
                                    user_id
                                )
                            return []
        except Exception as e:
            logging.error(f"Ошибка проверки Trafsly ads_id {ads_id}: {e}")
    
    return results

# ========== ДИАГНОСТИКА API ==========
async def diagnose_service(name: str, key: str, url: str, payload: dict, headers: dict = None):
    if not key:
        return f"❌ <b>{name}</b> — ключ не задан"
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    t0 = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=h, timeout=aiohttp.ClientTimeout(total=10)) as r:
                dt = int((time.time() - t0) * 1000)
                body = await r.text()
                try:
                    data = json.loads(body)
                except Exception:
                    data = None
                
                if r.status not in (200, 201):
                    return f"⚠️ <b>{name}</b> — HTTP {r.status} ({dt}мс)\n<code>{body[:150]}</code>"
                
                count = None
                if isinstance(data, dict):
                    for field in ("tasks", "sponsors", "offers", "result", "results", "data", "items"):
                        v = data.get(field)
                        if isinstance(v, list):
                            count = len(v)
                            break
                
                if count is not None:
                    if count > 0:
                        return f"✅ <b>{name}</b> — {count} заданий ({dt}мс)"
                    return f"🟡 <b>{name}</b> — ключ ок, 0 заданий ({dt}мс)"
                
                return f"🟡 <b>{name}</b> — {r.status} OK, формат не распознан ({dt}мс)\n<code>{body[:150]}</code>"
    except asyncio.TimeoutError:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — timeout ({dt}мс)"
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — {str(e)[:100]} ({dt}мс)"

async def run_diagnostics():
    test_uid = ADMIN_ID if ADMIN_ID else 1
    mx = await get_max_sponsors()
    results = await asyncio.gather(
        diagnose_service("Piarflow", PIARFLOW_API_KEY,
            "https://piarflow.com/v1/sponsors",
            {"user_id": test_uid, "chat_id": test_uid, "max_sponsors": mx},
            {"Authorization": f"Bearer {PIARFLOW_API_KEY}"}),
        diagnose_service("Flyer", FLYER_API_KEY,
            "https://api.flyerhubs.com/v1/tasks",
            {"key": FLYER_API_KEY, "user_id": test_uid, "language_code": "ru", "limit": mx}),
        diagnose_service("TGrass", TGRASS_API_KEY,
            "https://tgrass.space/offers",
            {"tg_user_id": test_uid, "tg_login": "", "lang": "ru", "is_premium": False},
            {"Auth": TGRASS_API_KEY}),
        diagnose_service("Traffy", TRAFFY_API_KEY,
            "https://traffy.ai/publisher/tasks",
            {"telegram_id": test_uid, "limit": mx, "first_name": "Test", "username": "", "language_code": "ru"},
            {"x-publisher-api-key": TRAFFY_API_KEY}),
        diagnose_service("Botohub", BOTOHUB_API_KEY,
            "https://botohub.me/get-tasks",
            {"chat_id": test_uid},
            {"Auth": BOTOHUB_API_KEY}),
        diagnose_service("Trafsly", TRAFSLY_API_KEY,
            "https://api.trafsly.com/api/v1/get-sponsors",
            {"user_id": test_uid, "max_sponsors": mx, "language_code": "ru", "is_premium": False},
            {"Auth": TRAFSLY_API_KEY}),
    )
    header = f"🔎 <b>Диагностика API</b>\n🆔 user_id: <code>{test_uid}</code>\n\n"
    return header + "\n\n".join(results)# ========== ОСНОВНАЯ ЛОГИКА ==========
async def get_all_sponsors(user: types.User, force_refresh: bool = False):
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    lang = user.language_code or "ru"
    is_premium = getattr(user, "is_premium", False) or False
    max_sponsors = await get_max_sponsors()
    
    all_sponsors = []
    pool = await get_pool()
    
    if not force_refresh:
        async with pool.acquire() as db:
            row = await db.fetchrow(
                "SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = $1",
                f"all_sponsors_{user_id}"
            )
            if row:
                payload, expires_at = row["payload"], row["expires_at"]
                if expires_at > time.time():
                    return json.loads(payload)
    
    logging.info(f"Запрос спонсоров для {user_id}")
    
    # Piarflow
    piarflow = await get_piarflow_sponsors(user_id, user_id, max_sponsors)
    for s in piarflow:
        await log_sponsor(user_id, "piarflow", s.get("link"), "выдан")
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id, service, assignment_id) DO NOTHING",
                user_id, "piarflow", s.get("link"), s.get("link"), "unsubscribed", time.time()
            )
    all_sponsors.extend(piarflow)
    
    # Flyer
    flyer = await get_flyer_tasks(user_id, lang)
    for s in flyer:
        await log_sponsor(user_id, "flyer", s.get("link"), "выдан")
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, signature, status, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (user_id, service, assignment_id) DO NOTHING",
                user_id, "flyer", str(s.get("id")), s.get("link"), s.get("signature"), "unsubscribed", time.time()
            )
    all_sponsors.extend(flyer)
    
    # TGrass
    tgrass = await get_tgrass_offers(user_id, username, lang, is_premium)
    for s in tgrass:
        await log_sponsor(user_id, "tgrass", s.get("link"), "выдан")
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id, service, assignment_id) DO NOTHING",
                user_id, "tgrass", str(s.get("offer_id")), s.get("link"), "unsubscribed", time.time()
            )
    all_sponsors.extend(tgrass)
    
    # Traffy
    traffy = await get_traffy_tasks(user_id, max_sponsors, first_name, username, lang)
    for s in traffy:
        await log_sponsor(user_id, "traffy", s.get("target_link"), "выдан")
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) "
                "VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id, service, assignment_id) DO NOTHING",
                user_id, "traffy", str(s.get("assignment_id")), s.get("target_link"), "unsubscribed", time.time()
            )
    all_sponsors.extend(traffy)
    
    # Botohub (уже сохраняет в БД)
    botohub = await get_botohub_tasks(user_id)
    all_sponsors.extend([{"link": link} for link in botohub])
    
    # Trafsly
    trafsly = await get_trafsly_sponsors(user_id, user_id, first_name, username, lang, is_premium, max_sponsors)
    for s in trafsly:
        await log_sponsor(user_id, "trafsly", s.get("link"), "выдан")
    all_sponsors.extend(trafsly)
    
    all_sponsors = all_sponsors[:max_sponsors]
    logging.info(f"Всего спонсоров: {len(all_sponsors)}")
    
    if not all_sponsors:
        await log_sponsor_status(user_id, "not_issued", "Нет активных заданий")
    else:
        await log_sponsor_status(user_id, "issued", f"Выдано {len(all_sponsors)} спонсоров")
    
    if all_sponsors:
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_cache (cache_key, payload, expires_at) VALUES ($1, $2, $3) "
                "ON CONFLICT (cache_key) DO UPDATE SET payload=$2, expires_at=$3",
                f"all_sponsors_{user_id}", json.dumps(all_sponsors), time.time() + 300
            )
    
    return all_sponsors

async def check_all_subscriptions(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        tasks = await db.fetch(
            "SELECT service, assignment_id, link, signature FROM sponsor_tasks "
            "WHERE user_id = $1 AND status != 'subscribed'",
            user_id
        )
    
    if not tasks:
        await log_sponsor_status(user_id, "subscribed", "Все задания выполнены")
        return True
    
    all_done = True
    piarflow_links = []
    traffy_ids = []
    
    for t in tasks:
        if t["service"] == "piarflow" and t["link"]:
            piarflow_links.append(t["link"])
        elif t["service"] == "traffy" and t["assignment_id"]:
            traffy_ids.append(t["assignment_id"])
    
    if piarflow_links:
        results = await check_piarflow_sponsors(user_id, piarflow_links)
        for r in results:
            if r.get("status") not in ["subscribed", "not_counted"]:
                all_done = False
    
    if traffy_ids:
        results = await check_traffy_tasks(user_id, traffy_ids)
        for r in results:
            if r.get("status") != "completed":
                all_done = False
    
    for t in tasks:
        service = t["service"]
        assignment_id = t["assignment_id"]
        signature = t["signature"]
        
        if service == "flyer" and signature:
            done = await check_flyer_task(user_id, signature)
            if done:
                async with pool.acquire() as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'flyer' AND assignment_id = $2",
                        user_id, assignment_id
                    )
            else:
                all_done = False
        elif service == "tgrass":
            done = await check_tgrass_subscription(user_id)
            if done:
                async with pool.acquire() as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'tgrass'",
                        user_id
                    )
            else:
                all_done = False
        elif service == "botohub":
            done = await check_botohub_tasks(user_id)
            if done:
                async with pool.acquire() as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'botohub'",
                        user_id
                    )
            else:
                all_done = False
    
    async with pool.acquire() as db:
        trafsly_tasks = await db.fetch(
            "SELECT assignment_id, need_check FROM sponsor_tasks "
            "WHERE user_id = $1 AND service = 'trafsly' AND status != 'subscribed'",
            user_id
        )
    
    for t in trafsly_tasks:
        assignment_id = t["assignment_id"]
        need_check = t["need_check"]
        if need_check == 0:
            async with pool.acquire() as db:
                await db.execute(
                    "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'trafsly' AND assignment_id = $2",
                    user_id, assignment_id
                )
        else:
            try:
                await check_trafsly_sponsors(user_id, [int(assignment_id)])
            except:
                pass
    
    if not all_done:
        await log_sponsor_status(user_id, "not_subscribed", "Пользователь не подписался на все каналы")
    else:
        await log_sponsor_status(user_id, "subscribed", "Все задания выполнены")
    
    async with pool.acquire() as db:
        count = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id = $1 AND status != 'subscribed'",
            user_id
        )
        return count["c"] == 0

async def activate_user(user_id: int, username: str = "Unknown"):
    is_all_done = await check_all_subscriptions(user_id)
    if not is_all_done:
        return False
    
    pool = await get_pool()
    async with pool.acquire() as db:
        referrer_row = await db.fetchrow("SELECT referrer_id FROM users WHERE user_id = $1", user_id)
        referrer_id = referrer_row["referrer_id"] if referrer_row else None
        
        await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = $1", user_id)
        
        if referrer_id and referrer_id != user_id:
            count_row = await db.fetchrow(
                "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id = $1 AND status = 'subscribed'",
                user_id
            )
            sponsors_count = count_row["c"] if count_row else 0
            
            sponsor_reward = await get_sponsor_reward()
            reward = sponsors_count * sponsor_reward
            
            if reward > 0:
                await db.execute("""
                    UPDATE users 
                    SET balance = balance + $1, 
                        total_earned = total_earned + $1, 
                        referrals_count = referrals_count + 1 
                    WHERE user_id = $2
                """, reward, referrer_id)
                
                balance_row = await db.fetchrow("SELECT balance FROM users WHERE user_id = $1", referrer_id)
                new_balance = balance_row["balance"] if balance_row else 0
                
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🆗 *Реферал подтвердил подписку!*\n"
                        f"👤 Реферал: @{username or 'нет'}\n"
                        f"💬 Награда: +{reward:.1f}⭐ ({sponsors_count} × {sponsor_reward})\n"
                        f"📊 Спонсоров у реферала: {sponsors_count}\n"
                        f"💎 *Твой баланс: {new_balance:.1f}⭐*",
                        parse_mode="Markdown"
                    )
                except:
                    pass
        
        return True

# ========== ПРОВЕРКА ПЕРЕД ДЕЙСТВИЕМ ==========
async def check_sponsors_before_action(message: types.Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await register_user(user_id, message.from_user.username or "Unknown")
        user = await get_user(user_id)
    
    if user and user.get('is_activated') == 1:
        return True
    
    sponsors = await get_all_sponsors(message.from_user)
    
    if sponsors:
        await message.answer(
            "📌 *Подпишись на каналы, затем нажми 'Проверить подписки':*",
            reply_markup=sponsors_keyboard(sponsors, 1),
            parse_mode="Markdown"
        )
        return False
    else:
        await activate_user(user_id, message.from_user.username or "Unknown")
        await message.answer(
            "🎉 *Добро пожаловать!*",
            reply_markup=main_menu(user_id),
            parse_mode="Markdown"
        )
        return True

# ========== КЛАВИАТУРЫ ==========
def main_menu(user_id: int = 0):
    builder = ReplyKeyboardBuilder()
    builder.row(
        types.KeyboardButton(text="⭐ Заработать звёзды"),
        types.KeyboardButton(text="👤 Профиль")
    )
    builder.row(
        types.KeyboardButton(text="🎁 Бонус за спонсоров"),
        types.KeyboardButton(text="💎 Вывод")
    )
    if user_id == ADMIN_ID and ADMIN_ID != 0:
        builder.row(types.KeyboardButton(text="👑 Админ"))
    return builder.as_markup(resize_keyboard=True)

def sponsors_keyboard(sponsors, page=1):
    builder = InlineKeyboardBuilder()
    per_page = 15
    total_pages = (len(sponsors) + per_page - 1) // per_page if sponsors else 1
    start = (page - 1) * per_page
    end = start + per_page
    page_sponsors = sponsors[start:end]
    
    for idx, sp in enumerate(page_sponsors, start + 1):
        if isinstance(sp, dict):
            link = sp.get("link") or sp.get("target_link")
        else:
            link = sp
        if link:
            builder.row(types.InlineKeyboardButton(text=f"📢 Канал #{idx}", url=link))
    
    nav_row = []
    if page > 1:
        nav_row.append(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"sponsors_page_{page - 1}"))
    if page < total_pages:
        nav_row.append(types.InlineKeyboardButton(text="➡️ Далее", callback_data=f"sponsors_page_{page + 1}"))
    if nav_row:
        builder.row(*nav_row)
    
    builder.row(types.InlineKeyboardButton(text="✅ Проверить подписки", callback_data="check_subs"))
    return builder.as_markup()

def admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="📋 Статус", callback_data="admin_sponsor_status"),
        types.InlineKeyboardButton(text="📋 Логи", callback_data="admin_logs")
    )
    builder.row(
        types.InlineKeyboardButton(text="⚙ Награда", callback_data="admin_set_sponsor_reward"),
        types.InlineKeyboardButton(text="⚙ Максимум", callback_data="admin_set_max_sponsors")
    )
    builder.row(
        types.InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        types.InlineKeyboardButton(text="🔎 Диагностика", callback_data="admin_diag")
    )
    builder.row(types.InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close"))
    return builder.as_markup()

# ========== ХЕНДЛЕРЫ ==========
@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    
    referrer_id = None
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])
    
    await register_user(user_id, username, referrer_id)
    
    sponsors = await get_all_sponsors(message.from_user)
    
    if referrer_id and referrer_id != user_id and sponsors:
        try:
            await bot.send_message(
                referrer_id,
                f"🔔 *НОВЫЙ РЕФЕРАЛ!*\n"
                f"🛫 Реферал: {username or 'Без юзернейма'}\n"
                f"🗂 Ему выдано спонсоров: {len(sponsors)}\n"
                f"🖥 *После подписки вы получите награду!*",
                parse_mode="Markdown"
            )
        except:
            pass
    
    if sponsors:
        await message.answer(
            "📌 *Подпишись на каналы, затем нажми 'Проверить подписки':*",
            reply_markup=sponsors_keyboard(sponsors, 1),
            parse_mode="Markdown"
        )
    else:
        await activate_user(user_id, username)
        await message.answer(
            "🎉 *Добро пожаловать!*",
            reply_markup=main_menu(user_id),
            parse_mode="Markdown"
        )

@dp.callback_query(F.data.startswith("sponsors_page_"))
async def sponsors_page(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT payload FROM sponsor_cache WHERE cache_key = $1", f"all_sponsors_{user_id}")
    
    if not row:
        await callback.answer("❌ Спонсоры устарели!")
        return
    
    sponsors = json.loads(row["payload"])
    try:
        await callback.message.edit_reply_markup(reply_markup=sponsors_keyboard(sponsors, page))
    except:
        await callback.answer("❌ Ошибка!")
    await callback.answer()

@dp.callback_query(F.data == "check_subs")
async def check_subs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer("🔄 Проверяю...")
    pool = await get_pool()
    
    # Piarflow
    async with pool.acquire() as db:
        piarflow_tasks = await db.fetch(
            "SELECT link FROM sponsor_tasks WHERE user_id = $1 AND service = 'piarflow' AND status != 'subscribed'",
            user_id
        )
    if piarflow_tasks:
        links = [t["link"] for t in piarflow_tasks]
        results = await check_piarflow_sponsors(user_id, links)
        for r in results:
            if r.get("status") in ["subscribed", "not_counted"]:
                async with pool.acquire() as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'piarflow' AND link = $2",
                        user_id, r.get("link")
                    )
    
    # Flyer
    async with pool.acquire() as db:
        flyer_tasks = await db.fetch(
            "SELECT signature, assignment_id FROM sponsor_tasks WHERE user_id = $1 AND service = 'flyer' AND status != 'subscribed'",
            user_id
        )
    for t in flyer_tasks:
        signature = t["signature"]
        assignment_id = t["assignment_id"]
        if signature:
            done = await check_flyer_task(user_id, signature)
            if done:
                async with pool.acquire() as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'flyer' AND assignment_id = $2",
                        user_id, assignment_id
                    )
        else:
            async with pool.acquire() as db:
                await db.execute(
                    "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'flyer' AND assignment_id = $2",
                    user_id, assignment_id
                )
    
    # TGrass
    if await check_tgrass_subscription(user_id):
        async with pool.acquire() as db:
            await db.execute(
                "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'tgrass'",
                user_id
            )
    
    # Traffy
    async with pool.acquire() as db:
        traffy_tasks = await db.fetch(
            "SELECT assignment_id FROM sponsor_tasks WHERE user_id = $1 AND service = 'traffy' AND status != 'subscribed'",
            user_id
        )
    if traffy_tasks:
        assignment_ids = [t["assignment_id"] for t in traffy_tasks]
        results = await check_traffy_tasks(user_id, assignment_ids)
        for r in results:
            if r.get("status") == "completed":
                async with pool.acquire() as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'traffy' AND assignment_id = $2",
                        user_id, r.get("assignment_id")
                    )
    
    # Botohub
    if await check_botohub_tasks(user_id):
        async with pool.acquire() as db:
            await db.execute(
                "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'botohub'",
                user_id
            )
    
    # Trafsly
    async with pool.acquire() as db:
        trafsly_tasks = await db.fetch(
            "SELECT assignment_id, need_check FROM sponsor_tasks WHERE user_id = $1 AND service = 'trafsly' AND status != 'subscribed'",
            user_id
        )
    for t in trafsly_tasks:
        assignment_id = t["assignment_id"]
        need_check = t["need_check"]
        if need_check == 0:
            async with pool.acquire() as db:
                await db.execute(
                    "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND service = 'trafsly' AND assignment_id = $2",
                    user_id, assignment_id
                )
        else:
            try:
                await check_trafsly_sponsors(user_id, [int(assignment_id)])
            except:
                pass
    
    # Финальная проверка
    async with pool.acquire() as db:
        count = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id = $1 AND status != 'subscribed'",
            user_id
        )
        all_done = count["c"] == 0
    
    if all_done:
        await activate_user(user_id, callback.from_user.username or "Unknown")
        try:
            await callback.message.delete()
        except:
            pass
        await callback.message.answer(
            "🎉 *Все подписки подтверждены!*",
            reply_markup=main_menu(user_id),
            parse_mode="Markdown"
        )
        await callback.answer("✅ Готово!")
    else:
        async with pool.acquire() as db:
            unsubscribed = await db.fetch(
                "SELECT link, service FROM sponsor_tasks WHERE user_id = $1 AND status != 'subscribed'",
                user_id
            )
        
        if unsubscribed:
            sponsors_list = [{"link": t["link"], "service": t["service"]} for t in unsubscribed]
            async with pool.acquire() as db:
                await db.execute(
                    "INSERT INTO manual_checks (user_id, username, sponsors, created_at) VALUES ($1, $2, $3, $4)",
                    user_id, callback.from_user.username or "Unknown", json.dumps(sponsors_list), time.time()
                )
            
            kb = InlineKeyboardBuilder()
            kb.row(
                types.InlineKeyboardButton(text="✅ Разрешить", callback_data=f"manual_allow_{user_id}"),
                types.InlineKeyboardButton(text="❌ Отказать", callback_data=f"manual_deny_{user_id}")
            )
            
            await bot.send_message(
                ADMIN_ID,
                f"🔔 Ручная проверка!\n\n"
                f"👤 @{callback.from_user.username or 'нет'} (ID: {user_id})\n"
                f"📊 Неподтверждённых: {len(unsubscribed)}\n\n"
                f"📌 Список:\n" + "\n".join([f"  • {t['link']} ({t['service']})" for t in unsubscribed]),
                reply_markup=kb.as_markup()
            )
            
            try:
                await callback.answer(f"❌ Осталось {len(unsubscribed)} подписок!", show_alert=True)
            except:
                pass
            
            user = types.User(id=user_id, is_bot=False, first_name="User", language_code="ru")
            sponsors = await get_all_sponsors(user, force_refresh=True)
            if sponsors:
                await callback.message.edit_reply_markup(reply_markup=sponsors_keyboard(sponsors, 1))
        else:
            await callback.answer("❌ Ошибка проверки!", show_alert=True)

@dp.callback_query(F.data.startswith("manual_allow_"))
async def manual_allow(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    user_id = int(callback.data.split("_")[2])
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = $1 AND status != 'subscribed'", user_id)
        await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = $1", user_id)
        await db.execute("UPDATE manual_checks SET status = 'approved' WHERE user_id = $1 AND status = 'pending'", user_id)
    try:
        await bot.send_message(user_id, "🎉 *Доступ разрешён!*", reply_markup=main_menu(user_id), parse_mode="Markdown")
    except:
        pass
    await callback.message.edit_text(f"✅ Доступ разрешён для {user_id}")
    await callback.answer("✅")

@dp.callback_query(F.data.startswith("manual_deny_"))
async def manual_deny(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    user_id = int(callback.data.split("_")[2])
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE manual_checks SET status = 'denied' WHERE user_id = $1 AND status = 'pending'", user_id)
    try:
        await bot.send_message(user_id, "❌ *Доступ отклонён.*", parse_mode="Markdown")
    except:
        pass
    await callback.message.edit_text(f"❌ Доступ отклонён для {user_id}")
    await callback.answer("❌")

@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user_id = message.from_user.id
    user = await get_user(user_id)
    if user:
        pool = await get_pool()
        async with pool.acquire() as db:
            active_refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id = $1 AND is_activated = 1", user_id))["c"]
            total_refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id = $1", user_id))["c"]
        
        await message.answer(
            f"👤 *Твой профиль*\n\n"
            f"🆔 ID: `{user['user_id']}`\n"
            f"💰 Баланс: {user['balance']:.1f} ⭐\n"
            f"👥 Рефералов: {total_refs}\n"
            f"✅ Активных: {active_refs}\n"
            f"📊 Всего заработано: {user['total_earned']:.1f} ⭐",
            parse_mode="Markdown"
        )

@dp.message(F.text == "⭐ Заработать звёзды")
async def earn_stars(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user or not user.get('is_activated'):
        return
    bot_info = await bot.get_me()
    sponsor_reward = await get_sponsor_reward()
    await message.answer(
        f"🔗 *Твоя реферальная ссылка:*\n"
        f"https://t.me/{bot_info.username}?start={user_id}\n\n"
        f"Приглашай друзей и получай *{sponsor_reward} ⭐* за каждого спонсора!",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎁 Бонус за спонсоров")
async def sponsor_bonus(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user_id = message.from_user.id
    today = datetime.now().date().isoformat()
    user = await get_user(user_id)
    if not user:
        return
    
    if user.get("last_bonus") == today:
        await message.answer("⏳ *Уже забрал сегодня!*", parse_mode="Markdown")
        return
    
    pool = await get_pool()
    async with pool.acquire() as db:
        count = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id = $1 AND status = 'subscribed'", user_id)
        sponsors_count = count["c"] if count else 0
    
    if sponsors_count == 0:
        await message.answer("❌ Нет выполненных спонсоров!", parse_mode="Markdown")
        return
    
    sponsor_reward = await get_sponsor_reward()
    bonus = sponsors_count * sponsor_reward
    
    await update_balance(user_id, bonus)
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET last_bonus = $1 WHERE user_id = $2", today, user_id)
    
    await message.answer(
        f"🎁 *Бонус!*\n\n"
        f"📊 Спонсоров: *{sponsors_count}*\n"
        f"💰 Награда: *{sponsor_reward} ⭐*\n"
        f"💎 Получено: *+{bonus:.1f} ⭐*",
        parse_mode="Markdown"
    )

@dp.message(F.text == "💎 Вывод")
async def withdraw_start(message: types.Message, state: FSMContext):
    if not await check_sponsors_before_action(message):
        return
    user = await get_user(message.from_user.id)
    if not user:
        return
    if user["balance"] < 15:
        await message.answer(f"❌ Минимум 15 ⭐. Баланс: {user['balance']:.1f} ⭐")
        return
    
    await state.set_state(WithdrawState.waiting_for_username)
    await message.answer(
        "💎 *Вывод*\n\nВведи username (без @):\nПример: `ivan_durov`\n\nЕсли нет — отправь `-`",
        parse_mode="Markdown"
    )

@dp.message(WithdrawState.waiting_for_username)
async def withdraw_username(message: types.Message, state: FSMContext):
    username = message.text.strip()
    if username == "-":
        username = "Не указан"
    else:
        username = username.replace("@", "")
    
    user_id = message.from_user.id
    amount = 15
    
    await update_balance(user_id, -amount)
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO withdraw_requests (user_id, username, amount, created_at) VALUES ($1, $2, $3, $4)",
            user_id, username, amount, time.time()
        )
    
    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔔 *Заявка на вывод!*\n\n"
                f"👤 ID: {user_id}\n"
                f"💰 Сумма: {amount} ⭐\n"
                f"📌 Username: @{username}",
                parse_mode="Markdown"
            )
        except:
            pass
    
    await message.answer(f"✅ Заявка на {amount} ⭐ принята!\nПодарок придёт на @{username} в течение 24ч.")
    await state.clear()

# ========== АДМИН-ПАНЕЛЬ ==========
@dp.message(F.text == "👑 Админ")
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("👑 *Админ-панель*", reply_markup=admin_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_sponsor_status")
async def admin_sponsor_status(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT user_id, status, reason, updated_at FROM sponsor_status ORDER BY updated_at DESC LIMIT 20"
        )
    if not rows:
        await callback.message.edit_text("📋 Нет данных.", reply_markup=admin_keyboard())
        return
    text = "📋 *Статус (20):*\n\n"
    for r in rows:
        date = datetime.fromtimestamp(r["updated_at"]).strftime("%d.%m %H:%M")
        emoji = "✅" if r["status"] in ["issued", "subscribed"] else "❌"
        text += f"{emoji} [{date}] ID {r['user_id']} → *{r['status']}*\n"
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_keyboard())

@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        logs = await db.fetch(
            "SELECT user_id, service, link, created_at FROM sponsor_log ORDER BY created_at DESC LIMIT 20"
        )
    if not logs:
        await callback.message.edit_text("📋 Нет логов.", reply_markup=admin_keyboard())
        return
    text = "📋 *Последние выдачи:*\n\n"
    for l in logs:
        date = datetime.fromtimestamp(l["created_at"]).strftime("%d.%m %H:%M")
        text += f"• [{date}] {l['service']} → {l['user_id']}\n"
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_keyboard())

@dp.callback_query(F.data == "admin_set_sponsor_reward")
async def admin_set_sponsor_reward_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminState.waiting_for_sponsor_reward)
    current = await get_sponsor_reward()
    await callback.message.edit_text(f"⚙ Награда за спонсора (текущая: {current} ⭐):\n\nВведи число:")

@dp.message(AdminState.waiting_for_sponsor_reward)
async def admin_set_sponsor_reward_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        value = float(message.text.replace(",", "."))
        if value <= 0:
            await message.answer("❌ Больше 0!")
            return
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("INSERT INTO settings (key, value) VALUES ('sponsor_reward', $1) ON CONFLICT (key) DO UPDATE SET value=$1", str(value))
        await message.answer(f"✅ Награда: {value} ⭐")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введи число!")

@dp.callback_query(F.data == "admin_set_max_sponsors")
async def admin_set_max_sponsors_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminState.waiting_for_max_sponsors)
    current_max = await get_max_sponsors()
    await callback.message.edit_text(f"⚙ Максимум спонсоров (текущий: {current_max}):\n\n1-30:")

@dp.message(AdminState.waiting_for_max_sponsors)
async def admin_set_max_sponsors_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        value = int(message.text.strip())
        if value < 1 or value > 30:
            await message.answer("❌ 1-30!")
            return
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("INSERT INTO settings (key, value) VALUES ('max_sponsors', $1) ON CONFLICT (key) DO UPDATE SET value=$1", str(value))
        await message.answer(f"✅ Максимум: {value}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Целое число!")

# ========== РАССЫЛКА ==========
@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminState.waiting_for_broadcast)
    await callback.message.edit_text(
        "📢 *Рассылка*\n\n"
        "Отправь сообщение для рассылки.\n\n"
        "❌ Отмена: `/cancel`",
        parse_mode="Markdown"
    )

@dp.message(AdminState.waiting_for_broadcast)
async def admin_broadcast_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    
    pool = await get_pool()
    async with pool.acquire() as db:
        users = await db.fetch("SELECT user_id FROM users")
    
    total = len(users)
    await message.answer(f"📢 Рассылка на {total} юзеров…")
    
    ok = 0
    fail = 0
    for u in users:
        try:
            await message.copy_to(u["user_id"])
            ok += 1
        except:
            fail += 1
        await asyncio.sleep(0.05)
    
    await message.answer(
        f"✅ *Готово!*\n\n📤 {ok}\n❌ {fail}",
        parse_mode="Markdown",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()

@dp.callback_query(F.data == "admin_diag")
async def admin_diag(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer("Проверяю…")
    try:
        await callback.message.edit_text("🔎 Диагностика…")
    except:
        pass
    report = await run_diagnostics()
    if len(report) > 4000:
        report = report[:4000] + "…"
    try:
        await callback.message.edit_text(report, parse_mode="HTML", reply_markup=admin_keyboard())
    except:
        try:
            await callback.message.answer(report, parse_mode="HTML", reply_markup=admin_keyboard())
        except:
            pass

@dp.callback_query(F.data == "admin_close")
async def admin_close(callback: types.CallbackQuery):
    await callback.message.delete()

# ========== ВЕБ-СЕРВЕР + MAIN ==========
async def handle(request):
    return web.Response(text="Bot is running!")

async def main():
    await init_db()
    
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
