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
DEFAULT_MAX_SPONSORS = 20

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== FSM ==========
class AdminState(StatesGroup):
    waiting_for_reward = State()
    waiting_for_balance = State()
    waiting_for_user_id = State()
    waiting_for_max_sponsors = State()

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
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ref_reward', '3.0')")
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('max_sponsors', '20')")
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

async def get_ref_reward():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key='ref_reward'") as c:
            row = await c.fetchone()
            return float(row[0]) if row else 3.0

async def get_max_sponsors():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key='max_sponsors'") as c:
            row = await c.fetchone()
            return int(row[0]) if row else DEFAULT_MAX_SPONSORS

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
        if referrer_id and referrer_id != user_id:
            reward = await get_ref_reward()
            await db.execute("""
                UPDATE users 
                SET balance = balance + ?, 
                    total_earned = total_earned + ?, 
                    referrals_count = referrals_count + 1 
                WHERE user_id = ?
            """, (reward, reward, referrer_id))
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
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok") and data.get("tasks"):
                        return data.get("tasks", [])
    except Exception as e:
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
        return []
    url = "https://botohub.me/get-tasks"
    headers = {"Content-Type": "application/json", "Auth": BOTOHUB_API_KEY}
    payload = {"chat_id": user_id}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if not data.get("skip") and not data.get("completed"):
                        return data.get("tasks", [])
    except Exception as e:
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
    except Exception as e:
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

# ========== ТЕСТЫ СПОНСОРОВ ==========
async def test_piarflow(user_id: int):
    if not PIARFLOW_API_KEY:
        return "❌ Ключ не установлен"
    try:
        sponsors = await get_piarflow_sponsors(user_id, user_id, 5)
        if sponsors:
            return f"✅ {len(sponsors)} спонсоров:\n" + "\n".join([f"  • {s.get('link')}" for s in sponsors[:5]])
        return "❌ 0 спонсоров (нет заданий или ошибка)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

async def test_traffy(user_id: int):
    if not TRAFFY_API_KEY:
        return "❌ Ключ не установлен"
    try:
        tasks = await get_traffy_tasks(user_id, 5)
        if tasks:
            return f"✅ {len(tasks)} заданий:\n" + "\n".join([f"  • {t.get('target_link')}" for t in tasks[:5]])
        return "❌ 0 заданий (нет заданий или ошибка)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

async def test_flyer(user_id: int):
    if not FLYER_API_KEY:
        return "❌ Ключ не установлен"
    try:
        tasks = await get_flyer_tasks(user_id, "ru")
        if tasks:
            return f"✅ {len(tasks)} заданий:\n" + "\n".join([f"  • {t.get('link')}" for t in tasks[:5]])
        return "❌ 0 заданий (нет заданий или ошибка)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

async def test_tgrass(user_id: int):
    if not TGRASS_API_KEY:
        return "❌ Ключ не установлен"
    try:
        offers = await get_tgrass_offers(user_id, "test", "ru", False)
        if offers:
            return f"✅ {len(offers)} офферов:\n" + "\n".join([f"  • {o.get('link')}" for o in offers[:5]])
        return "❌ 0 офферов (нет заданий или ошибка)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

async def test_botohub(user_id: int):
    if not BOTOHUB_API_KEY:
        return "❌ Ключ не установлен"
    try:
        tasks = await get_botohub_tasks(user_id)
        if tasks:
            return f"✅ {len(tasks)} заданий:\n" + "\n".join([f"  • {t}" for t in tasks[:5]])
        return "❌ 0 заданий (нет заданий или ошибка)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

async def test_trafsly(user_id: int):
    if not TRAFSLY_API_KEY:
        return "❌ Ключ не установлен"
    try:
        sponsors = await get_trafsly_sponsors(user_id, user_id, max_sponsors=5)
        if sponsors:
            return f"✅ {len(sponsors)} спонсоров:\n" + "\n".join([f"  • {s.get('link')}" for s in sponsors[:5]])
        return "❌ 0 спонсоров (нет заданий или ошибка)"
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

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
    
    # Piarflow
    piarflow = await get_piarflow_sponsors(user_id, user_id, max_sponsors)
    for s in piarflow:
        await log_sponsor(user_id, "piarflow", s.get("link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "piarflow", s.get("link"), s.get("link"), "unsubscribed", time.time())
        )
    all_sponsors.extend(piarflow)
    
    # Flyer
    flyer = await get_flyer_tasks(user_id, lang)
    for s in flyer:
        await log_sponsor(user_id, "flyer", s.get("link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, signature, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, "flyer", str(s.get("id")), s.get("link"), s.get("signature"), "unsubscribed", time.time())
        )
    all_sponsors.extend(flyer)
    
    # TGrass
    tgrass = await get_tgrass_offers(user_id, username, lang, is_premium)
    for s in tgrass:
        await log_sponsor(user_id, "tgrass", s.get("link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "tgrass", str(s.get("offer_id")), s.get("link"), "unsubscribed", time.time())
        )
    all_sponsors.extend(tgrass)
    
    # Traffy
    traffy = await get_traffy_tasks(user_id, max_sponsors, first_name, username, lang)
    for s in traffy:
        await log_sponsor(user_id, "traffy", s.get("target_link"), "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "traffy", s.get("assignment_id"), s.get("target_link"), "unsubscribed", time.time())
        )
    all_sponsors.extend(traffy)
    
    # Botohub
    botohub = await get_botohub_tasks(user_id)
    for s in botohub:
        await log_sponsor(user_id, "botohub", s, "выдан")
        await db_execute(
            "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, "botohub", s, s, "unsubscribed", time.time())
        )
    all_sponsors.extend(botohub)
    
    # Trafsly
    trafsly = await get_trafsly_sponsors(
        user_id, user_id, first_name, username, lang, is_premium, max_sponsors
    )
    for s in trafsly:
        await log_sponsor(user_id, "trafsly", s.get("link"), "выдан")
    all_sponsors.extend(trafsly)
    
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
    
    if not all_done:
        await log_sponsor_status(user_id, "not_subscribed", "Пользователь не подписался на все каналы")
    else:
        await log_sponsor_status(user_id, "subscribed", "Все задания выполнены")
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'", (user_id,)) as c:
            count = await c.fetchone()
            return count[0] == 0

async def activate_user(user_id: int):
    is_all_done = await check_all_subscriptions(user_id)
    if not is_all_done:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = ?", (user_id,))
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
            "📌 *Выполни задания для доступа к боту:*",
            reply_markup=sponsors_keyboard(sponsors, 1),
            parse_mode="Markdown"
        )
        return False
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = ?", (user_id,))
            await db.commit()
        await message.answer("🎉 *Добро пожаловать!*", reply_markup=main_menu(), parse_mode="Markdown")
        return True

# ========== КЛАВИАТУРЫ ==========
def main_menu():
    builder = ReplyKeyboardBuilder()
    builder.row(
        types.KeyboardButton(text="⭐ Заработать звёзды"),
        types.KeyboardButton(text="👤 Профиль")
    )
    builder.row(
        types.KeyboardButton(text="🎁 Бонус"),
        types.KeyboardButton(text="💎 Вывод")
    )
    builder.row(types.KeyboardButton(text="👑 Админ"))
    return builder.as_markup(resize_keyboard=True)

def sponsors_keyboard(sponsors, page=1):
    builder = InlineKeyboardBuilder()
    
    per_page = 15
    total_pages = (len(sponsors) + per_page - 1) // per_page if sponsors else 1
    start = (page - 1) * per_page
    end = start + per_page
    page_sponsors = sponsors[start:end]
    
    row_buttons = []
    for sp in page_sponsors:
        if not isinstance(sp, dict):
            continue
        link = sp.get("link") or sp.get("target_link")
        if link and link.startswith("http"):
            row_buttons.append(types.InlineKeyboardButton(
                text=f"📢 Подписаться",
                url=link
            ))
    
    for i in range(0, len(row_buttons), 2):
        builder.row(*row_buttons[i:i+2])
    
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
        text="✅ Проверить",
        callback_data="check_subs"
    ))
    return builder.as_markup()

def admin_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        types.InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        types.InlineKeyboardButton(text="📋 Статус", callback_data="admin_sponsor_status")
    )
    builder.row(
        types.InlineKeyboardButton(text="📋 Логи", callback_data="admin_logs"),
        types.InlineKeyboardButton(text="⚙ Награда", callback_data="admin_set_reward")
    )
    builder.row(
        types.InlineKeyboardButton(text="💰 Баланс", callback_data="admin_give_balance"),
        types.InlineKeyboardButton(text="🧪 Тест", callback_data="admin_test_sponsors")
    )
    builder.row(
        types.InlineKeyboardButton(text="🔍 Тест для юзера", callback_data="admin_test_user"),
        types.InlineKeyboardButton(text="⚙ Максимум спонсоров", callback_data="admin_set_max_sponsors")
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
    
    if sponsors:
        await message.answer(
            "📌 *Выполни задания для доступа к боту:*",
            reply_markup=sponsors_keyboard(sponsors, 1),
            parse_mode="Markdown"
        )
    else:
        await activate_user(user_id)
        await message.answer(
            "🎉 *Добро пожаловать!*",
            reply_markup=main_menu(),
            parse_mode="Markdown"
        )

@dp.callback_query(F.data == "check_subs")
async def check_subs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer("🔄 Проверяю...")
    
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
        async with db.execute("SELECT signature FROM sponsor_tasks WHERE user_id = ? AND service = 'flyer' AND status != 'subscribed'", (user_id,)) as c:
            flyer_tasks = await c.fetchall()
    
    for row in flyer_tasks:
        signature = row[0]
        if signature:
            done = await check_flyer_task(user_id, signature)
            if done:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'flyer' AND signature = ?",
                        (user_id, signature)
                    )
                    await db.commit()
    
    # 3. TGrass
    if await check_tgrass_subscription(user_id):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'tgrass'",
                (user_id,)
            )
            await db.commit()
    
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
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = ?", (user_id,))
            await db.commit()
        
        await callback.message.delete()
        await callback.message.answer(
            "🎉 *Все задания выполнены! Добро пожаловать!*",
            reply_markup=main_menu(),
            parse_mode="Markdown"
        )
    else:
        await callback.answer("❌ Выполни не все задания!", show_alert=True)
        
        user = types.User(id=user_id, is_bot=False, first_name="User", last_name=None, username=None, language_code="ru")
        sponsors = await get_all_sponsors(user, force_refresh=True)
        if sponsors:
            await callback.message.edit_reply_markup(
                reply_markup=sponsors_keyboard(sponsors, 1)
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

@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user = await get_user(message.from_user.id)
    if user:
        await message.answer(
            f"👤 *Твой профиль*\n\n"
            f"🆔 ID: `{user['user_id']}`\n"
            f"💰 Баланс: {user['balance']:.1f} ⭐\n"
            f"👥 Рефералов: {user['referrals_count']}\n"
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
        await message.answer("❌ Сначала выполни задания через /start!")
        return
    bot_info = await bot.get_me()
    reward = await get_ref_reward()
    await message.answer(
        f"🔗 *Твоя реферальная ссылка:*\n"
        f"https://t.me/{bot_info.username}?start={user_id}\n\n"
        f"Приглашай друзей и получай *{reward} ⭐* за каждого!",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎁 Бонус")
async def daily_bonus(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user_id = message.from_user.id
    today = datetime.now().date().isoformat()
    user = await get_user(user_id)
    if not user:
        await message.answer("❌ Ты не зарегистрирован!")
        return
    if user.get("last_bonus") == today:
        await message.answer("❌ Ты уже забирал бонус сегодня! Приходи завтра.")
        return
    await update_balance(user_id, 0.5)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET last_bonus = ? WHERE user_id = ?", (today, user_id))
        await db.commit()
    await message.answer("🎁 *Ты получил 0.5 ⭐!* Приходи завтра снова.", parse_mode="Markdown")

@dp.message(F.text == "💎 Вывод")
async def withdraw_start(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ты не зарегистрирован!")
        return
    if user["balance"] < 15:
        await message.answer(f"❌ Минимальная сумма вывода — 15 ⭐. Твой баланс: {user['balance']:.1f} ⭐")
        return
    await message.answer(
        f"💎 *Вывод звёзд*\n\nТвой баланс: {user['balance']:.1f} ⭐\nВыбери сумму:",
        reply_markup=withdraw_keyboard(),
        parse_mode="Markdown"
    )

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

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total = (await c.fetchone())[0]
        async with db.execute("SELECT SUM(balance) FROM users") as c:
            total_balance = (await c.fetchone())[0] or 0
        async with db.execute("SELECT value FROM settings WHERE key='ref_reward'") as c:
            row = await c.fetchone()
            ref_reward = row[0] if row else "Не установлена"
        max_sponsors = await get_max_sponsors()
    await callback.message.edit_text(
        f"📊 *Статистика*\n\n"
        f"👥 Пользователей: {total}\n"
        f"💰 Всего звёзд: {total_balance:.1f}\n"
        f"⚙ Награда за реферала: {ref_reward} ⭐\n"
        f"⚙ Максимум спонсоров: {max_sponsors}",
        parse_mode="Markdown",
        reply_markup=admin_keyboard()
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

@dp.callback_query(F.data == "admin_set_reward")
async def admin_set_reward_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    await state.set_state(AdminState.waiting_for_reward)
    await callback.message.edit_text(
        "⚙ *Изменить награду за реферала*\n\n"
        "Введи новую сумму (например: `5` или `2.5`):",
        parse_mode="Markdown"
    )

@dp.message(AdminState.waiting_for_reward)
async def admin_set_reward_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        reward = float(message.text.replace(",", "."))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('ref_reward', ?)", (str(reward),))
            await db.commit()
        await message.answer(f"✅ Награда за реферала обновлена: {reward} ⭐")
        await state.clear()
    except Exception as e:
        await message.answer("❌ Ошибка! Введи число. Пример: `5`")
        logging.error(f"Admin reward error: {e}")

@dp.callback_query(F.data == "admin_give_balance")
async def admin_give_balance_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    await state.set_state(AdminState.waiting_for_balance)
    await callback.message.edit_text(
        "💰 *Выдать или забрать баланс*\n\n"
        "Введи `ID_ПОЛЬЗОВАТЕЛЯ СУММА`\n"
        "Для списания используй отрицательное число.\n\n"
        "Пример: `123456789 10` или `123456789 -5`",
        parse_mode="Markdown"
    )

@dp.message(AdminState.waiting_for_balance)
async def admin_give_balance_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        parts = message.text.strip().split()
        user_id = int(parts[0])
        amount = float(parts[1].replace(",", "."))
        
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as c:
                row = await c.fetchone()
                if not row:
                    await message.answer("❌ Пользователь не найден!")
                    await state.clear()
                    return
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
            await db.commit()
        
        await message.answer(f"✅ Баланс пользователя `{user_id}` изменён на `{amount}` ⭐")
        await state.clear()
    except Exception as e:
        await message.answer("❌ Ошибка! Формат: `ID СУММА`")
        logging.error(f"Admin balance error: {e}")

@dp.callback_query(F.data == "admin_test_sponsors")
async def admin_test_sponsors(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    
    user_id = callback.from_user.id
    await callback.message.edit_text("🔄 *Тестирую спонсоров...*\n\nЭто может занять до 10 секунд.", parse_mode="Markdown")
    
    results = []
    
    piarflow = await test_piarflow(user_id)
    results.append(f"*Piarflow:* {piarflow}")
    
    traffy = await test_traffy(user_id)
    results.append(f"*Traffy:* {traffy}")
    
    flyer = await test_flyer(user_id)
    results.append(f"*Flyer:* {flyer}")
    
    tgrass = await test_tgrass(user_id)
    results.append(f"*TGrass:* {tgrass}")
    
    botohub = await test_botohub(user_id)
    results.append(f"*Botohub:* {botohub}")
    
    trafsly = await test_trafsly(user_id)
    results.append(f"*Trafsly:* {trafsly}")
    
    text = "🧪 *Результаты теста спонсоров:*\n\n" + "\n\n".join(results)
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_keyboard())

@dp.callback_query(F.data == "admin_test_user")
async def admin_test_user_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    await state.set_state(AdminState.waiting_for_user_id)
    await callback.message.edit_text(
        "🔍 *Тест спонсоров для пользователя*\n\n"
        "Введи Telegram ID пользователя:",
        parse_mode="Markdown"
    )

@dp.message(AdminState.waiting_for_user_id)
async def admin_test_user_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введи число! Пример: `123456789`")
        return
    
    await message.answer(f"🔄 Проверяю спонсоров для `{user_id}`...", parse_mode="Markdown")
    
    user = await get_user(user_id)
    if not user:
        await message.answer(f"❌ Пользователь `{user_id}` не найден в базе!", parse_mode="Markdown")
        await state.clear()
        return
    
    results = []
    
    piarflow = await test_piarflow(user_id)
    results.append(f"*Piarflow:* {piarflow}")
    
    traffy = await test_traffy(user_id)
    results.append(f"*Traffy:* {traffy}")
    
    flyer = await test_flyer(user_id)
    results.append(f"*Flyer:* {flyer}")
    
    tgrass = await test_tgrass(user_id)
    results.append(f"*TGrass:* {tgrass}")
    
    botohub = await test_botohub(user_id)
    results.append(f"*Botohub:* {botohub}")
    
    trafsly = await test_trafsly(user_id)
    results.append(f"*Trafsly:* {trafsly}")
    
    status_text = "✅ Активирован" if user.get('is_activated') == 1 else "❌ Не активирован"
    results.append(f"*Статус:* {status_text}")
    
    text = f"🔍 *Результаты для пользователя `{user_id}`:*\n\n" + "\n\n".join(results)
    await message.answer(text, parse_mode="Markdown")
    await state.clear()

@dp.callback_query(F.data == "admin_set_max_sponsors")
async def admin_set_max_sponsors_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    await state.set_state(AdminState.waiting_for_max_sponsors)
    current_max = await get_max_sponsors()
    await callback.message.edit_text(
        f"⚙ *Максимум спонсоров*\n\n"
        f"Введи число от 1 до 50 (текущий: {current_max}):",
        parse_mode="Markdown"
    )

@dp.message(AdminState.waiting_for_max_sponsors)
async def admin_set_max_sponsors_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        value = int(message.text.strip())
        if value < 1 or value > 50:
            await message.answer("❌ Введи число от 1 до 50!")
            return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('max_sponsors', ?)", (str(value),))
            await db.commit()
        await message.answer(f"✅ Максимум спонсоров установлен: {value}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введи целое число!")

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
