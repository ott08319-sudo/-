import os
import asyncio
import logging
import time
import json
import random
import aiosqlite
import aiohttp
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

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFFY_API_KEY = os.getenv("TRAFFY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")

DB_PATH = "bot.db"
DEFAULT_MAX_SPONSORS = 14
MAX_LIMIT = 30

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== FSM ==========
class AdminState(StatesGroup):
    waiting_for_reward = State()
    waiting_for_balance = State()
    waiting_for_user_id = State()
    waiting_for_max_sponsors = State()
    waiting_for_sponsor_reward = State()

class WithdrawState(StatesGroup):
    waiting_for_username = State()

# ========== БАЗА ДАННЫХ ==========
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL DEFAULT 0.0,
            total_earned REAL DEFAULT 0.0,
            referrer_id INTEGER,
            referrals_count INTEGER DEFAULT 0,
            is_activated INTEGER DEFAULT 0,
            created_at REAL,
            last_bonus TEXT DEFAULT ''
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            assignment_id TEXT,
            link TEXT,
            status TEXT DEFAULT 'unsubscribed',
            signature TEXT,
            need_check INTEGER DEFAULT 1,
            created_at REAL,
            UNIQUE(user_id, service, assignment_id)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            link TEXT,
            status TEXT,
            created_at REAL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_status (
            user_id INTEGER PRIMARY KEY,
            status TEXT,
            reason TEXT,
            updated_at REAL
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
            expires_at REAL NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            amount REAL,
            status TEXT DEFAULT 'pending',
            created_at REAL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS manual_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            sponsors TEXT,
            status TEXT DEFAULT 'pending',
            created_at REAL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS error_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            error_type TEXT,
            message TEXT,
            created_at REAL
        )""")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_sponsors', '14')")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('sponsor_reward', '0.5')")
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

async def get_max_sponsors():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key='max_sponsors'") as c:
            row = await c.fetchone()
            return int(row[0]) if row else DEFAULT_MAX_SPONSORS

async def get_sponsor_reward():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key='sponsor_reward'") as c:
            row = await c.fetchone()
            return float(row[0]) if row else 0.5

async def log_error(user_id: int, service: str, error_type: str, message: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO error_log (user_id, service, error_type, message, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, service, error_type, message, time.time())
        )
        await db.commit()

async def register_user(user_id: int, username: str, referrer_id: int = None):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as c:
            if await c.fetchone():
                return
        now = time.time()
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES (?, ?, ?, ?)",
            (user_id, username, referrer_id, now)
        )
        await db.commit()

async def update_balance(user_id: int, amount: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE users 
            SET balance = balance + ?, 
                total_earned = total_earned + CASE WHEN ? > 0 THEN ? ELSE 0 END 
            WHERE user_id = ?
        """, (amount, amount, amount, user_id))
        await db.commit()

async def log_sponsor(user_id: int, service: str, link: str, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sponsor_log (user_id, service, link, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, service, link, status, time.time())
        )
        await db.commit()

async def log_sponsor_status(user_id: int, status: str, reason: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO sponsor_status (user_id, status, reason, updated_at) VALUES (?, ?, ?, ?)",
            (user_id, status, reason, time.time())
        )
        await db.commit()

async def db_execute(query, params):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()

# ========== API СПОНСОРОВ ==========
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
                else:
                    await log_error(user_id, "piarflow", "http_error", f"Status: {resp.status}")
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
                    else:
                        await log_error(user_id, "flyer", "api_error", data.get("error"))
                else:
                    await log_error(user_id, "flyer", "http_error", f"Status: {resp.status}")
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
                else:
                    await log_error(user_id, "tgrass", "http_error", f"Status: {resp.status}")
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
                else:
                    await log_error(user_id, "traffy", "http_error", f"Status: {resp.status}")
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

async def get_botohub_tasks(user_id: int):
    if not BOTOHUB_API_KEY:
        await log_error(user_id, "botohub", "no_api_key", "API ключ не установлен")
        return []
    url = "https://botohub.me/get-tasks"
    headers = {"Content-Type": "application/json", "Auth": BOTOHUB_API_KEY}
    payload = {"chat_id": user_id}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Botohub ответ: {data}")
                    if not data.get("skip") and not data.get("completed"):
                        tasks = data.get("tasks", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for link in tasks:
                                if link:
                                    await db.execute(
                                        "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                        (user_id, "botohub", link, link, "unsubscribed", time.time())
                                    )
                            await db.commit()
                        return tasks
                else:
                    await log_error(user_id, "botohub", "http_error", f"Status: {resp.status}")
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
                    return data.get("completed", False)
    except Exception as e:
        logging.error(f"Botohub check error: {e}")
    return False

# ========== TRAFSLY API ==========
async def get_trafsly_sponsors(user_id: int, chat_id: int = None, first_name: str = None, username: str = None, language_code: str = "ru", is_premium: bool = False, max_sponsors: int = DEFAULT_MAX_SPONSORS):
    if not TRAFSLY_API_KEY:
        await log_error(user_id, "trafsly", "no_api_key", "API ключ не установлен")
        return []
    
    url = "https://api.trafsly.com/api/v1/get-sponsors"
    headers = {
        "Auth": TRAFSLY_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "user_id": user_id,
        "max_sponsors": max_sponsors,
        "language_code": language_code,
        "is_premium": is_premium
    }
    if chat_id:
        payload["chat_id"] = chat_id
    if first_name:
        payload["first_name"] = first_name
    if username:
        payload["username"] = username
    
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
                        async with aiosqlite.connect(DB_PATH) as db:
                            for s in sponsors:
                                link = s.get("link")
                                ads_id = s.get("ads_id")
                                need_check = 1 if ads_id else 0
                                if link:
                                    await db.execute(
                                        "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, need_check, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (user_id, "trafsly", str(ads_id) if ads_id else link, link, "unsubscribed", need_check, time.time())
                                    )
                            await db.commit()
                        return sponsors
                else:
                    await log_error(user_id, "trafsly", "http_error", f"Status: {resp.status}")
    except Exception as e:
        await log_error(user_id, "trafsly", "api_error", str(e))
        logging.error(f"Ошибка Trafsly: {e}")
    return []

async def check_trafsly_sponsors(user_id: int, assignment_ids: list):
    if not TRAFSLY_API_KEY or not assignment_ids:
        return []
    
    url = "https://api.trafsly.com/api/v1/confirm-subscription"
    headers = {
        "Auth": TRAFSLY_API_KEY,
        "Content-Type": "application/json"
    }
    
    results = []
    for ads_id in assignment_ids:
        payload = {
            "user_id": user_id,
            "ads_id": int(ads_id)
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logging.info(f"Trafsly проверка ads_id {ads_id}: {data}")
                        
                        if data.get("subscribed") == True:
                            async with aiosqlite.connect(DB_PATH) as db:
                                await db.execute(
                                    "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'trafsly' AND assignment_id = ?",
                                    (user_id, str(ads_id))
                                )
                                await db.commit()
                            results.append({"ads_id": ads_id, "status": "subscribed"})
                        elif data.get("subscribed") == False:
                            results.append({"ads_id": ads_id, "status": "unsubscribed"})
                        elif data.get("message") == "User is banned":
                            logging.warning(f"Пользователь {user_id} забанен в Trafsly")
                            async with aiosqlite.connect(DB_PATH) as db:
                                await db.execute(
                                    "DELETE FROM sponsor_tasks WHERE user_id = ? AND service = 'trafsly'",
                                    (user_id,)
                                )
                                await db.commit()
                            return []
                        elif "Order not found" in data.get("message", "") or "Sponsor was not shown" in data.get("message", ""):
                            logging.info(f"Задание {ads_id} для пользователя {user_id} просрочено, удаляем")
                            async with aiosqlite.connect(DB_PATH) as db:
                                await db.execute(
                                    "DELETE FROM sponsor_tasks WHERE user_id = ? AND service = 'trafsly' AND assignment_id = ?",
                                    (user_id, str(ads_id))
                                )
                                await db.commit()
        except Exception as e:
            logging.error(f"Ошибка проверки Trafsly ads_id {ads_id}: {e}")
    
    return results

# ========== ФИЛЬТР ТОЛЬКО ДЛЯ BOTOHUB ==========
def is_telegram_link(link):
    if not link:
        return False
    if isinstance(link, str):
        return link.startswith("https://t.me/") or link.startswith("http://t.me/")
    return False

# ========== ОСНОВНАЯ ЛОГИКА ==========
async def get_all_sponsors(user: types.User, force_refresh: bool = False):
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    lang = user.language_code or "ru"
    is_premium = user.is_premium or False
    max_sponsors = await get_max_sponsors()
    
    all_sponsors = []
    
    if not force_refresh:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = ?", (f"all_sponsors_{user_id}",)) as c:
                row = await c.fetchone()
                if row:
                    payload, expires_at = row
                    if expires_at > time.time():
                        return json.loads(payload)
    
    logging.info(f"Запрос спонсоров для {user_id}")
    
    # Piarflow (все ссылки)
    piarflow = await get_piarflow_sponsors(user_id, user_id, max_sponsors)
    for s in piarflow:
        await log_sponsor(user_id, "piarflow", s.get("link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "piarflow", s.get("link"), s.get("link"), "unsubscribed", time.time())
        )
    all_sponsors.extend(piarflow)
    
    # Flyer (все ссылки)
    flyer = await get_flyer_tasks(user_id, lang)
    for s in flyer:
        await log_sponsor(user_id, "flyer", s.get("link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, signature, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, "flyer", str(s.get("id")), s.get("link"), s.get("signature"), "unsubscribed", time.time())
        )
    all_sponsors.extend(flyer)
    
    # TGrass (все ссылки)
    tgrass = await get_tgrass_offers(user_id, username, lang, is_premium)
    for s in tgrass:
        await log_sponsor(user_id, "tgrass", s.get("link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "tgrass", str(s.get("offer_id")), s.get("link"), "unsubscribed", time.time())
        )
    all_sponsors.extend(tgrass)
    
    # Traffy (все ссылки)
    traffy = await get_traffy_tasks(user_id, max_sponsors, first_name, username, lang)
    for s in traffy:
        await log_sponsor(user_id, "traffy", s.get("target_link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "traffy", s.get("assignment_id"), s.get("target_link"), "unsubscribed", time.time())
        )
    all_sponsors.extend(traffy)
    
    # Botohub (ТОЛЬКО t.me)
    botohub = await get_botohub_tasks(user_id)
    botohub = [link for link in botohub if is_telegram_link(link)]
    for link in botohub:
        await log_sponsor(user_id, "botohub", link, "выдан")
    all_sponsors.extend(botohub)
    
    # Trafsly (все ссылки)
    trafsly = await get_trafsly_sponsors(
        user_id, user_id, first_name, username, lang, is_premium, max_sponsors
    )
    for s in trafsly:
        await log_sponsor(user_id, "trafsly", s.get("link"), "выдан")
    all_sponsors.extend(trafsly)
    
    # Обрезаем по лимиту
    all_sponsors = all_sponsors[:max_sponsors]
    
    logging.info(f"Всего спонсоров: {len(all_sponsors)}")
    
    if not all_sponsors:
        await log_sponsor_status(user_id, "not_issued", "Нет активных заданий")
    else:
        await log_sponsor_status(user_id, "issued", f"Выдано {len(all_sponsors)} спонсоров")
    
    if all_sponsors:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO sponsor_cache (cache_key, payload, expires_at) VALUES (?, ?, ?)",
                (f"all_sponsors_{user_id}", json.dumps(all_sponsors), time.time() + 300))
            await db.commit()
    
    return all_sponsors

async def check_all_subscriptions(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT service, assignment_id, link, signature FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'", (user_id,)) as c:
            tasks = await c.fetchall()
    
    if not tasks:
        await log_sponsor_status(user_id, "subscribed", "Все задания выполнены")
        return True
    
    all_done = True
    
    piarflow_links = []
    traffy_ids = []
    
    for service, assignment_id, link, signature in tasks:
        if service == "piarflow" and link:
            piarflow_links.append(link)
        elif service == "traffy" and assignment_id:
            traffy_ids.append(assignment_id)
    
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
    
    for service, assignment_id, link, signature in tasks:
        if service == "flyer" and signature:
            done = await check_flyer_task(user_id, signature)
            if done:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'flyer' AND assignment_id = ?", (user_id, assignment_id))
                    await db.commit()
            else:
                all_done = False
        elif service == "tgrass":
            done = await check_tgrass_subscription(user_id)
            if done:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'tgrass'", (user_id,))
                    await db.commit()
            else:
                all_done = False
        elif service == "botohub":
            done = await check_botohub_tasks(user_id)
            if done:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'botohub'", (user_id,))
                    await db.commit()
            else:
                all_done = False
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT assignment_id, need_check FROM sponsor_tasks WHERE user_id = ? AND service = 'trafsly' AND status != 'subscribed'",
            (user_id,)
        ) as c:
            trafsly_tasks = await c.fetchall()
    
    if trafsly_tasks:
        for assignment_id, need_check in trafsly_tasks:
            if need_check == 0:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'trafsly' AND assignment_id = ?",
                        (user_id, assignment_id)
                    )
                    await db.commit()
            else:
                try:
                    await check_trafsly_sponsors(user_id, [int(assignment_id)])
                except:
                    pass
    
    if not all_done:
        await log_sponsor_status(user_id, "not_subscribed", "Пользователь не подписался на все каналы")
    else:
        await log_sponsor_status(user_id, "subscribed", "Все задания выполнены")
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'", (user_id,)) as c:
            count = await c.fetchone()
            return count[0] == 0

async def activate_user(user_id: int, username: str = "Unknown"):
    is_all_done = await check_all_subscriptions(user_id)
    if not is_all_done:
        return False
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            referrer_id = row[0] if row else None
        
        await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = ?", (user_id,))
        
        if referrer_id and referrer_id != user_id:
            async with db.execute(
                "SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND status = 'subscribed'",
                (user_id,)
            ) as c:
                count = await c.fetchone()
                sponsors_count = count[0] if count else 0
            
            sponsor_reward = await get_sponsor_reward()
            reward = sponsors_count * sponsor_reward
            
            if reward > 0:
                await db.execute("""
                    UPDATE users 
                    SET balance = balance + ?, 
                        total_earned = total_earned + ?, 
                        referrals_count = referrals_count + 1 
                    WHERE user_id = ?
                """, (reward, reward, referrer_id))
                
                async with db.execute("SELECT balance FROM users WHERE user_id = ?", (referrer_id,)) as c:
                    new_balance = (await c.fetchone())[0]
                
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
        
        await db.commit()
        return True

# ========== ПРОВЕРКА СПОНСОРОВ ПЕРЕД КНОПКОЙ ==========
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
    
    logging.info(f"Показываем спонсоров: {len(page_sponsors)} шт.")
    
    for idx, sp in enumerate(page_sponsors, start + 1):
        if isinstance(sp, dict):
            link = sp.get("link") or sp.get("target_link")
        else:
            link = sp
        logging.info(f"Спонсор #{idx}: {link}")
        if link:
            builder.row(types.InlineKeyboardButton(
                text=f"📢 Канал #{idx}",
                url=link
            ))
    
    nav_row = []
    if page > 1:
        nav_row.append(types.InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"sponsors_page_{page - 1}"
        ))
    if page < total_pages:
        nav_row.append(types.InlineKeyboardButton(
            text="➡️ Далее",
            callback_data=f"sponsors_page_{page + 1}"
        ))
    if nav_row:
        builder.row(*nav_row)
    
    builder.row(types.InlineKeyboardButton(
        text="✅ Проверить подписки",
        callback_data="check_subs"
    ))
    
    return builder.as_markup()

def admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="📋 Статус", callback_data="admin_sponsor_status"),
        types.InlineKeyboardButton(text="📋 Логи", callback_data="admin_logs")
    )
    builder.row(types.InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close"))
    return builder.as_markup()

def withdraw_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="⭐ 15", callback_data="withdraw_15"),
        types.InlineKeyboardButton(text="⭐ 25", callback_data="withdraw_25"),
        types.InlineKeyboardButton(text="⭐ 50", callback_data="withdraw_50")
    )
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
                f"🛫 Реферал: {username or 'Без юзернейма'} (@{username or 'нет'})\n"
                f"🗂 Ему выдано спонсоров: {len(sponsors)}\n"
                f"🖥 *После подписки на спонсоров вы получите награду!*\n"
                f"📊 Награда зависит от количества спонсоров!",
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
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT payload FROM sponsor_cache WHERE cache_key = ?",
            (f"all_sponsors_{user_id}",)
        ) as c:
            row = await c.fetchone()
    
    if not row:
        await callback.answer("❌ Спонсоры устарели, обновите страницу!")
        return
    
    sponsors = json.loads(row[0])
    try:
        await callback.message.edit_reply_markup(
            reply_markup=sponsors_keyboard(sponsors, page)
        )
    except Exception as e:
        await callback.answer("❌ Ошибка при переключении страницы!")
    await callback.answer()

@dp.callback_query(F.data == "check_subs")
async def check_subs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer("🔄 Проверяю подписки...")
    
    logging.info(f"Начинаю проверку для {user_id}")
    
    # 1. Piarflow
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT link FROM sponsor_tasks WHERE user_id = ? AND service = 'piarflow' AND status != 'subscribed'", (user_id,)) as c:
            piarflow_tasks = await c.fetchall()
    
    if piarflow_tasks:
        links = [t[0] for t in piarflow_tasks]
        results = await check_piarflow_sponsors(user_id, links)
        logging.info(f"Piarflow результаты: {results}")
        
        for r in results:
            if r.get("status") in ["subscribed", "not_counted"]:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'piarflow' AND link = ?",
                        (user_id, r.get("link"))
                    )
                    await db.commit()
    
    # 2. Flyer
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT signature, assignment_id FROM sponsor_tasks WHERE user_id = ? AND service = 'flyer' AND status != 'subscribed'", (user_id,)) as c:
            flyer_tasks = await c.fetchall()
    
    for signature, assignment_id in flyer_tasks:
        if signature:
            done = await check_flyer_task(user_id, signature)
            if done:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'flyer' AND assignment_id = ?",
                        (user_id, assignment_id)
                    )
                    await db.commit()
        else:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'flyer' AND assignment_id = ?",
                    (user_id, assignment_id)
                )
                await db.commit()
            logging.info(f"✅ Задание Flyer без проверки засчитано: {assignment_id} para {user_id}")
    
    # 3. TGrass
    if await check_tgrass_subscription(user_id):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'tgrass'",
                (user_id,)
            )
            await db.commit()
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND service = 'tgrass' AND status != 'subscribed'",
                (user_id,)
            ) as c:
                count = await c.fetchone()
                if count and count[0] > 0:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'tgrass'",
                        (user_id,)
                    )
                    await db.commit()
                    logging.info(f"✅ TGrass принудительно засчитан для {user_id}")
    
    # 4. Traffy
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT assignment_id FROM sponsor_tasks WHERE user_id = ? AND service = 'traffy' AND status != 'subscribed'", (user_id,)) as c:
            traffy_tasks = await c.fetchall()
    
    if traffy_tasks:
        assignment_ids = [t[0] for t in traffy_tasks]
        results = await check_traffy_tasks(user_id, assignment_ids)
        for r in results:
            if r.get("status") == "completed":
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'traffy' AND assignment_id = ?",
                        (user_id, r.get("assignment_id"))
                    )
                    await db.commit()
    
    # 5. Botohub
    if await check_botohub_tasks(user_id):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'botohub'",
                (user_id,)
            )
            await db.commit()
    
    # 6. Trafsly
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT assignment_id, need_check FROM sponsor_tasks WHERE user_id = ? AND service = 'trafsly' AND status != 'subscribed'",
            (user_id,)
        ) as c:
            trafsly_tasks = await c.fetchall()
    
    if trafsly_tasks:
        for assignment_id, need_check in trafsly_tasks:
            if need_check == 0:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'trafsly' AND assignment_id = ?",
                        (user_id, assignment_id)
                    )
                    await db.commit()
            else:
                try:
                    await check_trafsly_sponsors(user_id, [int(assignment_id)])
                except:
                    pass
    
    # ФИНАЛЬНАЯ ПРОВЕРКА
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'", (user_id,)) as c:
            count = await c.fetchone()
            all_done = count[0] == 0
    
    if all_done:
        await activate_user(user_id, callback.from_user.username or "Unknown")
        try:
            await callback.message.delete()
        except:
            pass
        await callback.message.answer(
            "🎉 *Все подписки подтверждены! Добро пожаловать!*",
            reply_markup=main_menu(user_id),
            parse_mode="Markdown"
        )
        await callback.answer("✅ Все задания выполнены!")
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT link, service FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'",
                (user_id,)
            ) as c:
                unsubscribed = await c.fetchall()
        
        if unsubscribed:
            sponsors_list = [{"link": link, "service": service} for link, service in unsubscribed]
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO manual_checks (user_id, username, sponsors, created_at) VALUES (?, ?, ?, ?)",
                    (user_id, callback.from_user.username or "Unknown", json.dumps(sponsors_list), time.time())
                )
                await db.commit()
            
            kb = InlineKeyboardBuilder()
            kb.row(
                types.InlineKeyboardButton(text="✅ Разрешить доступ", callback_data=f"manual_allow_{user_id}"),
                types.InlineKeyboardButton(text="❌ Отказать", callback_data=f"manual_deny_{user_id}")
            )
            
            await bot.send_message(
                ADMIN_ID,
                f"🔔 *Ручная проверка подписок!*\n\n"
                f"👤 Пользователь: @{callback.from_user.username or 'нет'} (ID: {user_id})\n"
                f"📊 Неподтверждённых спонсоров: {len(unsubscribed)}\n\n"
                f"📌 Список:\n" + "\n".join([f"  • {link} ({service})" for link, service in unsubscribed]),
                reply_markup=kb.as_markup(),
                parse_mode="Markdown"
            )
            
            try:
                await callback.answer(f"❌ Осталось {len(unsubscribed)} подписок! Отправлено админу.", show_alert=True)
            except:
                pass
            
            user = types.User(id=user_id, is_bot=False, first_name="User", last_name=None, username=None, language_code="ru")
            sponsors = await get_all_sponsors(user, force_refresh=True)
            if sponsors:
                await callback.message.edit_reply_markup(
                    reply_markup=sponsors_keyboard(sponsors, 1)
                )
        else:
            await callback.answer("❌ Ошибка проверки!", show_alert=True)

@dp.callback_query(F.data.startswith("manual_allow_"))
async def manual_allow(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    
    user_id = int(callback.data.split("_")[2])
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT username FROM manual_checks WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
            (user_id,)
        ) as c:
            row = await c.fetchone()
            username = row[0] if row else "Unknown"
        
        await db.execute(
            "UPDATE manual_checks SET status = 'approved' WHERE user_id = ? AND status = 'pending'",
            (user_id,)
        )
        await db.commit()
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = ?", (user_id,))
        await db.commit()
    
    try:
        await bot.send_message(
            user_id,
            "🎉 *Доступ разрешён администратором!*\n\n"
            "Добро пожаловать в бота!",
            reply_markup=main_menu(user_id),
            parse_mode="Markdown"
        )
    except:
        pass
    
    await callback.message.edit_text(
        f"✅ *Доступ разрешён для пользователя {user_id}*",
        parse_mode="Markdown"
    )
    await callback.answer("✅ Доступ разрешён!")

@dp.callback_query(F.data.startswith("manual_deny_"))
async def manual_deny(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    
    user_id = int(callback.data.split("_")[2])
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE manual_checks SET status = 'denied' WHERE user_id = ? AND status = 'pending'",
            (user_id,)
        )
        await db.commit()
    
    try:
        await bot.send_message(
            user_id,
            "❌ *Доступ отклонён администратором.*\n\n"
            "Проверь подписки и попробуй снова.",
            parse_mode="Markdown"
        )
    except:
        pass
    
    await callback.message.edit_text(
        f"❌ *Доступ отклонён для пользователя {user_id}*",
        parse_mode="Markdown"
    )
    await callback.answer("❌ Доступ отклонён!")

@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user_id = message.from_user.id
    user = await get_user(user_id)
    if user:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE referrer_id = ? AND is_activated = 1",
                (user_id,)
            ) as c:
                active_refs = (await c.fetchone())[0]
            
            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE referrer_id = ?",
                (user_id,)
            ) as c:
                total_refs = (await c.fetchone())[0]
        
        await message.answer(
            f"👤 *Твой профиль*\n\n"
            f"🆔 ID: `{user['user_id']}`\n"
            f"💰 Баланс: {user['balance']:.1f} ⭐\n"
            f"👥 Рефералов (всего): {total_refs}\n"
            f"✅ Активных рефералов: {active_refs}\n"
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
        f"Приглашай друзей и получай *{sponsor_reward} ⭐* за каждого спонсора!\n"
        f"📊 Чем больше спонсоров — тем больше награда!",
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
        await message.answer("❌ Ты не зарегистрирован!")
        return
    
    if user.get("last_bonus") == today:
        await message.answer(
            "⏳ *Ты уже забирал бонус сегодня!*\n"
            "🔄 Приходи завтра — будет новый бонус!",
            parse_mode="Markdown"
        )
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND status = 'subscribed'",
            (user_id,)
        ) as c:
            count = await c.fetchone()
            sponsors_count = count[0] if count else 0
    
    if sponsors_count == 0:
        await message.answer(
            "📭 *Бонус за спонсоров*\n\n"
            "❌ У тебя пока нет выполненных спонсоров.\n\n"
            "📌 Как только выполнишь задания через /start — бонус появится!\n"
            "💡 Подписывайся на каналы и получай звёзды!",
            parse_mode="Markdown"
        )
        return
    
    sponsor_reward = await get_sponsor_reward()
    bonus = sponsors_count * sponsor_reward
    
    await update_balance(user_id, bonus)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET last_bonus = ? WHERE user_id = ?", (today, user_id))
        await db.commit()
    
    await message.answer(
        f"🎁 *Бонус за спонсоров!*\n\n"
        f"📊 Ты выполнил *{sponsors_count}* спонсоров.\n"
        f"💰 Награда за одного: *{sponsor_reward} ⭐*\n"
        f"💎 Ты получил: *+{bonus:.1f} ⭐*\n\n"
        f"📌 Завтра бонус будет от новых спонсоров!",
        parse_mode="Markdown"
    )

@dp.message(F.text == "💎 Вывод")
async def withdraw_start(message: types.Message, state: FSMContext):
    if not await check_sponsors_before_action(message):
        return
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ты не зарегистрирован!")
        return
    if user["balance"] < 15:
        await message.answer(f"❌ Минимальная сумма вывода — 15 ⭐. Твой баланс: {user['balance']:.1f} ⭐")
        return
    
    await state.set_state(WithdrawState.waiting_for_username)
    await message.answer(
        "💎 *Вывод звёзд*\n\n"
        "Введи свой *username* (без @), на который отправить подарок.\n"
        "Пример: `ivan_durov`\n\n"
        "Если не хочешь указывать — отправь `-`",
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
    user = await get_user(user_id)
    amount = 15
    
    await update_balance(user_id, -amount)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO withdraw_requests (user_id, username, amount, created_at) VALUES (?, ?, ?, ?)",
            (user_id, username, amount, time.time())
        )
        await db.commit()
    
    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔔 *Новая заявка на вывод!*\n\n"
                f"👤 Пользователь: ID {user_id}\n"
                f"💰 Сумма: {amount} ⭐\n"
                f"📌 Username: @{username}\n"
                f"🔗 Ссылка: tg://user?id={user_id}",
                parse_mode="Markdown"
            )
        except:
            pass
    
    await message.answer(
        f"✅ *Заявка на вывод {amount} ⭐ принята!*\n"
        f"Подарок будет отправлен на @{username} в течение 24 часов.\n\n"
        f"Если username указан неверно — свяжись с админом.",
        parse_mode="Markdown"
    )
    await state.clear()

@dp.callback_query(F.data.startswith("withdraw_"))
async def process_withdraw(callback: types.CallbackQuery):
    amount = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user or user["balance"] < amount:
        await callback.answer(f"❌ Недостаточно средств!", show_alert=True)
        return
    await update_balance(user_id, -amount)
    try:
        await callback.message.edit_text(
            f"✅ *Заявка на вывод {amount} ⭐ принята!*\n"
            f"Администратор отправит тебе подарок в течение 24 часов.",
            parse_mode="Markdown"
        )
    except:
        await callback.message.answer(
            f"✅ *Заявка на вывод {amount} ⭐ принята!*\n"
            f"Администратор отправит тебе подарок в течение 24 часов.",
            parse_mode="Markdown"
        )
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, f"🔔 Новая заявка на вывод!\nПользователь: ID {user_id}\nСумма: {amount} ⭐")
        except:
            pass

# ========== АДМИН-ПАНЕЛЬ ==========
@dp.message(F.text == "👑 Админ")
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У тебя нет доступа!")
        return
    await message.answer(
        "👑 *Админ-панель*",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_sponsor_status")
async def admin_sponsor_status(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, status, reason, updated_at FROM sponsor_status ORDER BY updated_at DESC LIMIT 20"
        ) as c:
            rows = await c.fetchall()
    if not rows:
        await callback.message.edit_text("📋 *Статус спонсоров:*\n\nНет данных.", parse_mode="Markdown", reply_markup=admin_keyboard())
        return
    text = "📋 *Статус спонсоров (последние 20):*\n\n"
    for user_id, status, reason, updated_at in rows:
        date = datetime.fromtimestamp(updated_at).strftime("%d.%m %H:%M")
        emoji = "✅" if status in ["issued", "subscribed"] else "❌"
        text += f"{emoji} [{date}] ID {user_id} → *{status}*\n"
        if reason:
            text += f"   📌 {reason}\n"
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_keyboard())

@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, service, link, created_at FROM sponsor_log ORDER BY created_at DESC LIMIT 20") as c:
            logs = await c.fetchall()
    if not logs:
        await callback.message.edit_text("📋 *Логи спонсоров:*\n\nНет записей.", parse_mode="Markdown", reply_markup=admin_keyboard())
        return
    text = "📋 *Последние выдачи:*\n\n"
    for user_id, service, link, created_at in logs:
        date = datetime.fromtimestamp(created_at).strftime("%d.%m %H:%M")
        text += f"• [{date}] {service} → user {user_id}\n"
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_keyboard())

@dp.callback_query(F.data == "admin_close")
async def admin_close(callback: types.CallbackQuery):
    await callback.message.delete()

# ========== ВЕБ-СЕРВЕР ==========
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
