
import os
import asyncio
import logging
import time
import json
import aiosqlite
import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

logging.basicConfig(level=logging.INFO)

# ========== ПЕРЕМЕННЫЕ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFFY_API_KEY = os.getenv("TRAFFY_API_KEY", "")

DB_PATH = "bot.db"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

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
            created_at REAL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_tasks (
            user_id INTEGER,
            service TEXT,
            assignment_id TEXT,
            link TEXT,
            status TEXT DEFAULT 'unsubscribed',
            signature TEXT,
            created_at REAL,
            PRIMARY KEY (user_id, service, assignment_id)
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_cache (
            cache_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            expires_at REAL NOT NULL
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            reward REAL,
            uses_left INTEGER
        )""")
        await db.execute("INSERT OR IGNORE INTO promo_codes VALUES ('START2026', 10.0, 100)")
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return dict(row) if row else None

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
            await db.execute("""
                UPDATE users 
                SET balance = balance + 3.0, 
                    total_earned = total_earned + 3.0, 
                    referrals_count = referrals_count + 1 
                WHERE user_id = ?
            """, (referrer_id,))
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

# ========== PIARFLOW API ==========
async def get_piarflow_sponsors(user_id: int, chat_id: int, max_sponsors: int = 5):
    if not PIARFLOW_API_KEY:
        return []

    url = "https://piarflow.com/v1/sponsors"
    headers = {
        "Authorization": f"Bearer {PIARFLOW_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "user_id": user_id,
        "chat_id": chat_id,
        "max_sponsors": max_sponsors
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Piarflow ответ: {data}")
                    
                    if data.get("status") == "ok":
                        sponsors = data.get("sponsors", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for s in sponsors:
                                link = s.get("link")
                                await db.execute(
                                    "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                    (user_id, "piarflow", link, link, "unsubscribed", time.time())
                                )
                            await db.commit()
                        return sponsors
    except Exception as e:
        logging.error(f"Ошибка Piarflow: {e}")
    
    return []

async def check_piarflow_sponsors(user_id: int, links: list):
    if not PIARFLOW_API_KEY or not links:
        return []

    url = "https://piarflow.com/v1/sponsors/check"
    headers = {
        "Authorization": f"Bearer {PIARFLOW_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "user_id": user_id,
        "links": links
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Piarflow проверка: {data}")
                    
                    if data.get("status") == "ok":
                        results = data.get("sponsors", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for r in results:
                                link = r.get("link")
                                status = r.get("status", "unsubscribed")
                                await db.execute(
                                    "UPDATE sponsor_tasks SET status = ? WHERE user_id = ? AND service = 'piarflow' AND link = ?",
                                    (status, user_id, link)
                                )
                            await db.commit()
                        return results
    except Exception as e:
        logging.error(f"Ошибка проверки Piarflow: {e}")
    
    return []

# ========== FLYER API ==========
async def get_flyer_tasks(user_id: int, language_code: str = "ru"):
    if not FLYER_API_KEY:
        return []

    url = "https://api.flyerhubs.com/v1/tasks"
    headers = {"Content-Type": "application/json"}
    payload = {
        "key": FLYER_API_KEY,
        "user_id": user_id,
        "language_code": language_code,
        "limit": 10
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Flyer ответ: {data}")
                    
                    if data.get("error"):
                        return []
                    
                    tasks = data.get("result", [])
                    async with aiosqlite.connect(DB_PATH) as db:
                        for task in tasks:
                            link = task.get("link") or task.get("url")
                            signature = task.get("signature")
                            assignment_id = str(task.get("id"))
                            await db.execute(
                                "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, signature, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (user_id, "flyer", assignment_id, link, signature, "unsubscribed", time.time())
                            )
                        await db.commit()
                    return tasks
    except Exception as e:
        logging.error(f"Ошибка Flyer: {e}")
    
    return []

async def check_flyer_task(user_id: int, signature: str):
    if not FLYER_API_KEY or not signature:
        return False

    url = "https://api.flyerhubs.com/v1/check_task"
    headers = {"Content-Type": "application/json"}
    payload = {
        "key": FLYER_API_KEY,
        "signature": signature
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("error"):
                        return False
                    result = data.get("result")
                    return result == "completed" or result == "subscribed"
    except Exception as e:
        logging.error(f"Ошибка Flyer check: {e}")
    
    return False

# ========== TGRASS API ==========
async def get_tgrass_offers(user_id: int, username: str, lang: str = "ru", is_premium: bool = False):
    if not TGRASS_API_KEY:
        return []

    url = "https://tgrass.space/offers"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Auth": TGRASS_API_KEY
    }
    payload = {
        "tg_user_id": user_id,
        "tg_login": username or "",
        "lang": lang or "ru",
        "is_premium": is_premium
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"TGrass ответ: {data}")
                    
                    if data.get("status") == "ok":
                        return []
                    
                    if data.get("status") == "not_ok":
                        offers = data.get("offers", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for o in offers:
                                assignment_id = str(o.get("offer_id"))
                                link = o.get("link")
                                await db.execute(
                                    "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                    (user_id, "tgrass", assignment_id, link, "unsubscribed", time.time())
                                )
                            await db.commit()
                        return offers
    except Exception as e:
        logging.error(f"Ошибка TGrass: {e}")
    
    return []

async def check_tgrass_subscription(user_id: int):
    if not TGRASS_API_KEY:
        return False

    url = "https://tgrass.space/offers"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Auth": TGRASS_API_KEY
    }
    payload = {
        "tg_user_id": user_id
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("status") == "ok"
    except Exception as e:
        logging.error(f"Ошибка TGrass check: {e}")
    
    return False

# ========== TRAFFY API ==========
async def get_traffy_tasks(user_id: int, limit: int = 5, first_name: str = None, username: str = None, language_code: str = "ru"):
    if not TRAFFY_API_KEY:
        return []

    url = "https://traffy.ai/publisher/tasks"
    headers = {
        "x-publisher-api-key": TRAFFY_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "telegram_id": user_id,
        "limit": limit
    }
    if first_name:
        payload["first_name"] = first_name
    if username:
        payload["username"] = username
    if language_code:
        payload["language_code"] = language_code
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Traffy ответ: {data}")
                    
                    if data.get("ok") and data.get("tasks"):
                        tasks = data.get("tasks", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for t in tasks:
                                assignment_id = t.get("assignment_id")
                                link = t.get("target_link")
                                if link:
                                    await db.execute(
                                        "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                        (user_id, "traffy", assignment_id, link, "unsubscribed", time.time())
                                    )
                            await db.commit()
                        return tasks
                    else:
                        return []
    except Exception as e:
        logging.error(f"Ошибка Traffy: {e}")
    
    return []

async def check_traffy_tasks(user_id: int, assignment_ids: list):
    if not TRAFFY_API_KEY or not assignment_ids:
        return []

    url = "https://traffy.ai/publisher/tasks/check"
    headers = {
        "x-publisher-api-key": TRAFFY_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "telegram_id": user_id,
        "assignment_ids": assignment_ids
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logging.info(f"Traffy проверка: {data}")
                    
                    if data.get("ok"):
                        results = data.get("results", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for r in results:
                                assignment_id = r.get("assignment_id")
                                status = "subscribed" if r.get("status") == "completed" else "unsubscribed"
                                await db.execute(
                                    "UPDATE sponsor_tasks SET status = ? WHERE user_id = ? AND service = 'traffy' AND assignment_id = ?",
                                    (status, user_id, assignment_id)
                                )
                            await db.commit()
                        return results
    except Exception as e:
        logging.error(f"Ошибка Traffy check: {e}")
    
    return []

# ========== ОСНОВНАЯ ЛОГИКА ==========
async def get_all_sponsors(user: types.User, force_refresh: bool = False):
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    lang = user.language_code or "ru"
    is_premium = user.is_premium or False
    
    all_sponsors = []
    
    if not force_refresh:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = ?",
                (f"all_sponsors_{user_id}",)
            ) as c:
                row = await c.fetchone()
                if row:
                    payload, expires_at = row
                    if expires_at > time.time():
                        return json.loads(payload)
    
    piarflow_sponsors = await get_piarflow_sponsors(user_id, user_id, max_sponsors=5)
    all_sponsors.extend(piarflow_sponsors)
    
    flyer_tasks = await get_flyer_tasks(user_id, lang)
    all_sponsors.extend(flyer_tasks)
    
    tgrass_offers = await get_tgrass_offers(user_id, username, lang, is_premium)
    all_sponsors.extend(tgrass_offers)
    
    traffy_tasks = await get_traffy_tasks(user_id, limit=5, first_name=first_name, username=username, language_code=lang)
    all_sponsors.extend(traffy_tasks)
    
    if all_sponsors:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO sponsor_cache (cache_key, payload, expires_at) VALUES (?, ?, ?)",
                (f"all_sponsors_{user_id}", json.dumps(all_sponsors), time.time() + 300)
            )
            await db.commit()
    
    return all_sponsors

async def check_all_subscriptions(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT service, assignment_id, link, signature FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'",
            (user_id,)
        ) as c:
            tasks = await c.fetchall()
    
    if not tasks:
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
            if r.get("status") != "subscribed":
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
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'flyer' AND assignment_id = ?",
                        (user_id, assignment_id)
                    )
                    await db.commit()
            else:
                all_done = False
        
        elif service == "tgrass":
            done = await check_tgrass_subscription(user_id)
            if done:
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE sponsor_tasks SET status = 'subscribed' WHERE user_id = ? AND service = 'tgrass'",
                        (user_id,)
                    )
                    await db.commit()
            else:
                all_done = False
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed'",
            (user_id,)
        ) as c:
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

# ========== КЛАВИАТУРЫ ==========
def main_menu():
    builder = ReplyKeyboardBuilder()
    builder.row(
        types.KeyboardButton(text="⭐ Заработать звёзды"),
        types.KeyboardButton(text="💰 Баланс")
    )
    builder.row(
        types.KeyboardButton(text="👤 Профиль"),
        types.KeyboardButton(text="🎟 Ввести промокод")
    )
    return builder.as_markup(resize_keyboard=True)

def sponsors_keyboard(sponsors):
    builder = InlineKeyboardBuilder()
    for idx, sp in enumerate(sponsors[:5], 1):
        link = sp.get("link") or sp.get("target_link")
        if link:
            builder.row(types.InlineKeyboardButton(
                text=f"📢 Задание #{idx}",
                url=link
            ))
    builder.row(types.InlineKeyboardButton(
        text="✅ Проверить подписки",
        callback_data="check_subs"
    ))
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
            "📌 Выполните задания для доступа к боту:",
            reply_markup=sponsors_keyboard(sponsors)
        )
    else:
        await activate_user(user_id)
        await message.answer("🎉 Добро пожаловать!", reply_markup=main_menu())

@dp.callback_query(F.data == "check_subs")
async def check_subs_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    if await activate_user(user_id):
        await callback.message.delete()
        await callback.message.answer("🎉 Все задания выполнены!", reply_markup=main_menu())
    else:
        await callback.answer("❌ Вы выполнили не все задания!", show_alert=True)
        sponsors = await get_all_sponsors(callback.from_user, force_refresh=True)
        if sponsors:
            await callback.message.edit_reply_markup(
                reply_markup=sponsors_keyboard(sponsors)
            )

@dp.message(F.text == "💰 Баланс")
async def balance_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        await message.answer(f"💳 Баланс: {user['balance']} ⭐")

@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        await message.answer(
            f"👤 *Профиль*\n🆔 ID: `{user['user_id']}`\n💰 Баланс: {user['balance']} ⭐\n👥 Рефералов: {user['referrals_count']}",
            parse_mode="Markdown"
        )

@dp.message(F.text == "⭐ Заработать звёзды")
async def earn_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if not user or not user.get('is_activated'):
        await message.answer("❌ Сначала выполните задания!")
        return
    
    bot_info = await bot.get_me()
    await message.answer(
        f"🔗 Реферальная ссылка:\nhttps://t.me/{bot_info.username}?start={message.from_user.id}\n\n+3 ⭐ за друга!"
    )

@dp.message(F.text == "🎟 Ввести промокод")
async def promo_start(message: types.Message):
    await message.answer("🎟 Введите промокод:")

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
