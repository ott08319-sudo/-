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

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
TGRASS_API_KEY = os.getenv("TGRASS_API_KEY", "")
TRAFFY_API_KEY = os.getenv("TRAFFY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")

DB_PATH = "bot.db"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

class CasinoState(StatesGroup):
    waiting_for_bet = State()

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
        CREATE TABLE IF NOT EXISTS sponsor_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            service TEXT,
            link TEXT,
            status TEXT,
            created_at REAL
        )""")
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

async def log_sponsor(user_id: int, service: str, link: str, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sponsor_log (user_id, service, link, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, service, link, status, time.time())
        )
        await db.commit()

# ========== API СПОНСОРОВ ==========
async def get_piarflow_sponsors(user_id: int, chat_id: int, max_sponsors: int = 5):
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
    payload = {"key": FLYER_API_KEY, "user_id": user_id, "language_code": language_code, "limit": 10}
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

async def get_traffy_tasks(user_id: int, limit: int = 5, first_name: str = None, username: str = None, language_code: str = "ru"):
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

# ========== ОСНОВНАЯ ЛОГИКА СПОНСОРОВ ==========
async def get_all_sponsors(user: types.User, force_refresh: bool = False):
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    lang = user.language_code or "ru"
    is_premium = user.is_premium or False
    
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
    
    piarflow = await get_piarflow_sponsors(user_id, user_id, 5)
    for s in piarflow:
        await log_sponsor(user_id, "piarflow", s.get("link"), "выдан")
    all_sponsors.extend(piarflow)
    
    flyer = await get_flyer_tasks(user_id, lang)
    for s in flyer:
        await log_sponsor(user_id, "flyer", s.get("link"), "выдан")
    all_sponsors.extend(flyer)
    
    tgrass = await get_tgrass_offers(user_id, username, lang, is_premium)
    for s in tgrass:
        await log_sponsor(user_id, "tgrass", s.get("link"), "выдан")
    all_sponsors.extend(tgrass)
    
    traffy = await get_traffy_tasks(user_id, 5, first_name, username, lang)
    for s in traffy:
        await log_sponsor(user_id, "traffy", s.get("target_link"), "выдан")
    all_sponsors.extend(traffy)
    
    botohub = await get_botohub_tasks(user_id)
    for s in botohub:
        await log_sponsor(user_id, "botohub", s, "выдан")
    all_sponsors.extend(botohub)
    
    logging.info(f"Всего спонсоров: {len(all_sponsors)}")
    
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

# ========== МЕНЮ ==========
def main_menu():
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text="⭐ Заработать звёзды"), types.KeyboardButton(text="💰 Баланс"))
    builder.row(types.KeyboardButton(text="👤 Профиль"), types.KeyboardButton(text="🎁 Ежедневный бонус"))
    builder.row(types.KeyboardButton(text="🏆 Топ пользователей"), types.KeyboardButton(text="🎲 Казино (кубик)"))
    builder.row(types.KeyboardButton(text="💎 Вывести звёзды"), types.KeyboardButton(text="👑 Админ-панель"))
    return builder.as_markup(resize_keyboard=True)

def sponsors_keyboard(sponsors):
    builder = InlineKeyboardBuilder()
    for idx, sp in enumerate(sponsors[:5], 1):
        link = sp.get("link") or sp.get("target_link")
        if link:
            builder.row(types.InlineKeyboardButton(text=f"📢 Задание #{idx}", url=link))
    builder.row(types.InlineKeyboardButton(text="✅ Проверить подписки", callback_data="check_subs"))
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
        await message.answer("📌 Выполни задания для доступа к боту:", reply_markup=sponsors_keyboard(sponsors))
    else:
        await activate_user(user_id)
        await message.answer("🎉 Добро пожаловать!", reply_markup=main_menu())

@dp.callback_query(F.data == "check_subs")
async def check_subs(callback: types.CallbackQuery):
    if await activate_user(callback.from_user.id):
        await callback.message.delete()
        await callback.message.answer("🎉 Все задания выполнены!", reply_markup=main_menu())
    else:
        await callback.answer("❌ Ты выполнил не все задания!", show_alert=True)

@dp.message(F.text == "💰 Баланс")
async def balance_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        await message.answer(f"💳 Твой баланс: {user['balance']:.1f} ⭐")

@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        await message.answer(
            f"👤 *Твой профиль*\n\n🆔 ID: `{user['user_id']}`\n💰 Баланс: {user['balance']:.1f} ⭐\n👥 Рефералов: {user['referrals_count']}\n📊 Всего заработано: {user['total_earned']:.1f} ⭐",
            parse_mode="Markdown"
        )

@dp.message(F.text == "⭐ Заработать звёзды")
async def earn_stars(message: types.Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user or not user.get('is_activated'):
        await message.answer("❌ Сначала выполни задания через /start!")
        return
    bot_info = await bot.get_me()
    await message.answer(
        f"🔗 Твоя реферальная ссылка:\nhttps://t.me/{bot_info.username}?start={user_id}\n\nПриглашай друзей и получай 3 ⭐ за каждого!",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎁 Ежедневный бонус")
async def daily_bonus(message: types.Message):
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
    await message.answer("🎁 Ты получил 0.5 ⭐! Приходи завтра снова.")

@dp.message(F.text == "🏆 Топ пользователей")
async def top_users(message: types.Message):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username, balance FROM users ORDER BY balance DESC LIMIT 10") as c:
            rows = await c.fetchall()
    if not rows:
        await message.answer("❌ Пока нет пользователей.")
        return
    text = "🏆 *Топ пользователей по балансу:*\n\n"
    for idx, (user_id, username, balance) in enumerate(rows, 1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(idx, f"{idx}.")
        text += f"{medal} {username or f'ID{user_id}'} — {balance:.1f} ⭐\n"
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "🎲 Казино (кубик)")
async def casino_start(message: types.Message, state: FSMContext):
    await state.set_state(CasinoState.waiting_for_bet)
    await message.answer(
        "🎲 *Кубик Казино*\n\nВведи сумму ставки и вариант (чёт/нечет):\nПример: `10 чёт`",
        parse_mode="Markdown"
    )

@dp.message(CasinoState.waiting_for_bet)
async def casino_bet(message: types.Message, state: FSMContext):
    try:
        parts = message.text.lower().split()
        amount = float(parts[0].replace(",", "."))
        choice = parts[1] if len(parts) > 1 else None
        if choice not in ["чёт", "нечет"]:
            await message.answer("❌ Введи 'чёт' или 'нечет' после суммы. Пример: `10 чёт`")
            return
        user_id = message.from_user.id
        user = await get_user(user_id)
        if not user or user["balance"] < amount:
            await message.answer(f"❌ Недостаточно средств! Твой баланс: {user['balance']:.1f} ⭐")
            return
        await update_balance(user_id, -amount)
        roll = random.randint(1, 6)
        is_even = roll % 2 == 0
        win = (choice == "чёт" and is_even) or (choice == "нечет" and not is_even)
        if win:
            win_amount = amount * 1.9
            await update_balance(user_id, win_amount)
            await message.answer(
                f"🎲 Выпало: *{roll}*\n\n✅ Ты выиграл! +{win_amount:.1f} ⭐ (x1.9)\n💰 Новый баланс: {user['balance'] + win_amount - amount:.1f} ⭐",
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                f"🎲 Выпало: *{roll}*\n\n❌ Ты проиграл! -{amount:.1f} ⭐\n💰 Новый баланс: {user['balance'] - amount:.1f} ⭐",
                parse_mode="Markdown"
            )
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка! Пример: `10 чёт`")
        logging.error(f"Casino error: {e}")

@dp.message(F.text == "💎 Вывести звёзды")
async def withdraw_start(message: types.Message):
    user = await get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ты не зарегистрирован!")
        return
    if user["balance"] < 15:
        await message.answer(f"❌ Минимальная сумма вывода — 15 ⭐. Твой баланс: {user['balance']:.1f} ⭐")
        return
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="⭐ 15 Звёзд", callback_data="withdraw_15"))
    kb.row(types.InlineKeyboardButton(text="⭐ 25 Звёзд", callback_data="withdraw_25"))
    kb.row(types.InlineKeyboardButton(text="⭐ 50 Звёзд", callback_data="withdraw_50"))
    await message.answer(
        f"💎 *Вывод звёзд*\n\nТвой баланс: {user['balance']:.1f} ⭐\nВыбери сумму:",
        reply_markup=kb.as_markup(),
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
    await callback.message.edit_text(
        f"✅ Заявка на вывод {amount} ⭐ принята!\nАдминистратор отправит тебе подарок в течение 24 часов.",
        parse_mode="Markdown"
    )
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, f"🔔 Новая заявка на вывод!\nПользователь: ID {user_id}\nСумма: {amount} ⭐")
        except:
            pass

@dp.message(F.text == "👑 Админ-панель")
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У тебя нет доступа!")
        return
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"))
    kb.row(types.InlineKeyboardButton(text="📋 Логи спонсоров", callback_data="admin_logs"))
    kb.row(types.InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close"))
    await message.answer("👑 *Админ-панель*", reply_markup=kb.as_markup(), parse_mode="Markdown")

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
    await callback.message.edit_text(
        f"📊 *Статистика*\n\n👥 Пользователей: {total}\n💰 Всего звёзд: {total_balance:.1f}",
        parse_mode="Markdown"
    )

@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, service, link, created_at FROM sponsor_log ORDER BY created_at DESC LIMIT 20") as c:
            logs = await c.fetchall()
    if not logs:
        await callback.message.edit_text("📋 *Логи спонсоров:*\n\nНет записей.", parse_mode="Markdown")
        return
    text = "📋 *Последние выдачи:*\n\n"
    for user_id, service, link, created_at in logs:
        date = datetime.fromtimestamp(created_at).strftime("%d.%m %H:%M")
        text += f"• [{date}] {service} → user {user_id}\n"
    await callback.message.edit_text(text, parse_mode="Markdown")

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
