import os
import asyncio
import logging
import time
import json
import random
import aiosqlite
import aiohttp
from aiohttp import web
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

logging.basicConfig(level=logging.INFO)

# ========== ПРОВЕРКА КЛЮЧЕЙ ==========
print("🔑 ПРОВЕРКА API КЛЮЧЕЙ:")
print(f"PIARFLOW_API_KEY: {'✅ ЕСТЬ' if os.getenv('PIARFLOW_API_KEY') else '❌ НЕТ'}")
print(f"TRAFFY_API_KEY: {'✅ ЕСТЬ' if os.getenv('TRAFFY_API_KEY') else '❌ НЕТ'}")
print(f"FLYER_API_KEY: {'✅ ЕСТЬ' if os.getenv('FLYER_API_KEY') else '❌ НЕТ'}")
print(f"TGRASS_API_KEY: {'✅ ЕСТЬ' if os.getenv('TGRASS_API_KEY') else '❌ НЕТ'}")
print(f"BOTOHUB_API_KEY: {'✅ ЕСТЬ' if os.getenv('BOTOHUB_API_KEY') else '❌ НЕТ'}")
print(f"BOT_TOKEN: {'✅ ЕСТЬ' if os.getenv('BOT_TOKEN') else '❌ НЕТ'}")
print("=" * 50)

# ========== ПЕРЕМЕННЫЕ ==========
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

# ========== FSM СОСТОЯНИЯ ==========
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
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            reward REAL,
            uses_left INTEGER
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

async def log_sponsor(user_id: int, service: str, link: str, status: str):
    """Логирует выдачу спонсора"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO sponsor_log (user_id, service, link, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, service, link, status, time.time())
        )
        await db.commit()
    logging.info(f"📢 СПОНСОР: user={user_id}, service={service}, link={link}, status={status}")

# ========== ВСЕ API СПОНСОРОВ (БЕЗ ИЗМЕНЕНИЙ) ==========
# [Тут вставь ВСЕ функции из твоего кода: 
# get_piarflow_sponsors, check_piarflow_sponsors,
# get_flyer_tasks, check_flyer_task,
# get_tgrass_offers, check_tgrass_subscription,
# get_traffy_tasks, check_traffy_tasks,
# get_botohub_tasks, check_botohub_tasks]

# Я сократил для читаемости, но ТЫ ВСТАВЛЯЕШЬ СВОИ ФУНКЦИИ ЦЕЛИКОМ!

# ========== ОСНОВНАЯ ЛОГИКА (С ЛОГИРОВАНИЕМ) ==========
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
    
    logging.info(f"Запрос спонсоров для {user_id}")
    
    # Piarflow
    piarflow_sponsors = await get_piarflow_sponsors(user_id, user_id, max_sponsors=5)
    for s in piarflow_sponsors:
        await log_sponsor(user_id, "piarflow", s.get("link"), "выдан")
    all_sponsors.extend(piarflow_sponsors)
    
    # Flyer
    flyer_tasks = await get_flyer_tasks(user_id, lang)
    for s in flyer_tasks:
        await log_sponsor(user_id, "flyer", s.get("link"), "выдан")
    all_sponsors.extend(flyer_tasks)
    
    # TGrass
    tgrass_offers = await get_tgrass_offers(user_id, username, lang, is_premium)
    for s in tgrass_offers:
        await log_sponsor(user_id, "tgrass", s.get("link"), "выдан")
    all_sponsors.extend(tgrass_offers)
    
    # Traffy
    traffy_tasks = await get_traffy_tasks(user_id, limit=5, first_name=first_name, username=username, language_code=lang)
    for s in traffy_tasks:
        await log_sponsor(user_id, "traffy", s.get("target_link"), "выдан")
    all_sponsors.extend(traffy_tasks)
    
    # Botohub
    botohub_tasks = await get_botohub_tasks(user_id)
    for s in botohub_tasks:
        await log_sponsor(user_id, "botohub", s, "выдан")
    all_sponsors.extend(botohub_tasks)
    
    logging.info(f"Всего спонсоров: {len(all_sponsors)}")
    
    if all_sponsors:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO sponsor_cache (cache_key, payload, expires_at) VALUES (?, ?, ?)",
                (f"all_sponsors_{user_id}", json.dumps(all_sponsors), time.time() + 300)
            )
            await db.commit()
    
    return all_sponsors

async def check_all_subscriptions(user_id: int):
    # [Тут ТВОЯ функция check_all_subscriptions без изменений]
    pass

async def activate_user(user_id: int):
    # [Тут ТВОЯ функция activate_user без изменений]
    pass

# ========== НОВЫЙ ФУНКЦИОНАЛ ==========

# 1. ЕЖЕДНЕВНЫЙ БОНУС
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

# 2. ТОП ПОЛЬЗОВАТЕЛЕЙ
@dp.message(F.text == "🏆 Топ пользователей")
async def top_users(message: types.Message):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, username, balance FROM users ORDER BY balance DESC LIMIT 10"
        ) as c:
            rows = await c.fetchall()
    
    if not rows:
        await message.answer("❌ Пока нет пользователей.")
        return
    
    text = "🏆 *Топ пользователей по балансу:*\n\n"
    for idx, (user_id, username, balance) in enumerate(rows, 1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(idx, f"{idx}.")
        text += f"{medal} {username or f'ID{user_id}'} — {balance:.1f} ⭐\n"
    
    await message.answer(text, parse_mode="Markdown")

# 3. КАЗИНО (КУБИК)
@dp.message(F.text == "🎲 Казино (кубик)")
async def casino_start(message: types.Message, state: FSMContext):
    await state.set_state(CasinoState.waiting_for_bet)
    await message.answer(
        "🎲 *Кубик Казино*\n\n"
        "Правила: ты ставишь звёзды на чёт или нечет.\n"
        "Если выпадает твой вариант — выигрываешь x1.9!\n\n"
        "Введи сумму ставки и выбери вариант:\n"
        "Пример: `10 чёт` или `5 нечет`",
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
        
        # Списываем ставку
        await update_balance(user_id, -amount)
        
        # Бросаем кубик (1-6)
        roll = random.randint(1, 6)
        is_even = roll % 2 == 0
        win = (choice == "чёт" and is_even) or (choice == "нечет" and not is_even)
        
        if win:
            win_amount = amount * 1.9
            await update_balance(user_id, win_amount)
            await message.answer(
                f"🎲 Выпало: *{roll}*\n\n"
                f"✅ Ты выиграл! +{win_amount:.1f} ⭐ (x1.9)\n"
                f"💰 Новый баланс: {user['balance'] + win_amount - amount:.1f} ⭐",
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                f"🎲 Выпало: *{roll}*\n\n"
                f"❌ Ты проиграл! -{amount:.1f} ⭐\n"
                f"💰 Новый баланс: {user['balance'] - amount:.1f} ⭐",
                parse_mode="Markdown"
            )
        
        await state.clear()
        
    except Exception as e:
        await message.answer(f"❌ Ошибка! Введи сумму и вариант. Пример: `10 чёт`")
        logging.error(f"Casino error: {e}")

# 4. АДМИН-ПАНЕЛЬ (СТАТИСТИКА ПО СПОНСОРАМ)
@dp.message(F.text == "👑 Админ-панель")
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У тебя нет доступа!")
        return
    
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="📊 Статистика спонсоров", callback_data="admin_sponsor_stats"))
    kb.row(types.InlineKeyboardButton(text="📋 Последние выдачи", callback_data="admin_recent_logs"))
    kb.row(types.InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close"))
    
    await message.answer("👑 *Админ-панель*\n\nВыбери действие:", reply_markup=kb.as_markup(), parse_mode="Markdown")

@dp.callback_query(F.data == "admin_sponsor_stats")
async def admin_sponsor_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Всего выдано спонсоров
        async with db.execute("SELECT COUNT(*) FROM sponsor_log") as c:
            total = (await c.fetchone())[0]
        
        # По сервисам
        async with db.execute(
            "SELECT service, COUNT(*) FROM sponsor_log GROUP BY service"
        ) as c:
            services = await c.fetchall()
        
        # По статусам
        async with db.execute(
            "SELECT status, COUNT(*) FROM sponsor_log GROUP BY status"
        ) as c:
            statuses = await c.fetchall()
    
    text = f"📊 *Статистика спонсоров:*\n\n"
    text += f"Всего выдано: *{total}*\n\n"
    
    if services:
        text += "*По сервисам:*\n"
        for service, count in services:
            text += f"  • {service}: {count}\n"
    
    if statuses:
        text += f"\n*По статусам:*\n"
        for status, count in statuses:
            text += f"  • {status}: {count}\n"
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "admin_recent_logs")
async def admin_recent_logs(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, service, link, status, created_at FROM sponsor_log ORDER BY created_at DESC LIMIT 20"
        ) as c:
            logs = await c.fetchall()
    
    if not logs:
        await callback.message.edit_text("📋 *Последние выдачи:*\n\nНет записей.", parse_mode="Markdown")
        return
    
    text = "📋 *Последние 20 выдач:*\n\n"
    for user_id, service, link, status, created_at in logs:
        date = datetime.fromtimestamp(created_at).strftime("%d.%m %H:%M")
        text += f"• [{date}] {service} → user {user_id} ({status})\n"
        text += f"  {link[:30]}...\n"
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "admin_close")
async def admin_close(callback: types.CallbackQuery):
    await callback.message.delete()

def admin_back_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back"))
    return kb.as_markup()

@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    await admin_panel(callback.message)  # Переиспользуем хендлер

# ========== ОБНОВЛЁННОЕ ГЛАВНОЕ МЕНЮ ==========
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
    builder.row(
        types.KeyboardButton(text="🎁 Ежедневный бонус"),
        types.KeyboardButton(text="🏆 Топ пользователей")
    )
    builder.row(
        types.KeyboardButton(text="🎲 Казино (кубик)"),
        types.KeyboardButton(text="💎 Вывести звёзды")
    )
    if ADMIN_ID and ADMIN_ID != 0:
        builder.row(types.KeyboardButton(text="👑 Админ-панель"))
    return builder.as_markup(resize_keyboard=True)

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
