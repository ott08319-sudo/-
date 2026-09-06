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
            offer_id TEXT,
            link TEXT,
            status TEXT DEFAULT 'unsubscribed',
            created_at REAL,
            PRIMARY KEY (user_id, service, offer_id, link)
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

# ========== РАБОТА СО СПОНСОРАМИ (PIARFLOW) ==========
async def get_sponsors_from_piarflow(user_id: int):
    if not PIARFLOW_API_KEY:
        return []
    
    url = "https://piarflow.com/api/v1/sponsors"
    headers = {
        "Authorization": f"Bearer {PIARFLOW_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "api_key": PIARFLOW_API_KEY,
        "telegram_id": str(user_id)
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
                                await db.execute(
                                    "INSERT OR IGNORE INTO sponsor_tasks (user_id, service, offer_id, link, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                                    (user_id, "piarflow", s.get("id"), s.get("link"), s.get("status", "unsubscribed"), time.time())
                                )
                            await db.commit()
                        return sponsors
    except Exception as e:
        logging.error(f"Ошибка Piarflow: {e}")
    return []

async def check_subscriptions_piarflow(user_id: int, sponsor_ids: list):
    if not PIARFLOW_API_KEY or not sponsor_ids:
        return []
    
    url = "https://piarflow.com/api/v1/check"
    headers = {
        "Authorization": f"Bearer {PIARFLOW_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "api_key": PIARFLOW_API_KEY,
        "telegram_id": str(user_id),
        "sponsor_ids": sponsor_ids
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "ok":
                        results = data.get("results", [])
                        async with aiosqlite.connect(DB_PATH) as db:
                            for r in results:
                                await db.execute(
                                    "UPDATE sponsor_tasks SET status = ? WHERE user_id = ? AND service = 'piarflow' AND offer_id = ?",
                                    (r.get("status", "unsubscribed"), user_id, r.get("id"))
                                )
                            await db.commit()
                        return results
    except Exception as e:
        logging.error(f"Ошибка проверки Piarflow: {e}")
    return []

async def get_user_sponsors_from_db(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM sponsor_tasks WHERE user_id = ? AND status != 'subscribed' ORDER BY created_at DESC",
            (user_id,)
        ) as c:
            rows = await c.fetchall()
            return [dict(row) for row in rows]

async def get_all_sponsors(user_id: int, force_refresh: bool = False):
    if not force_refresh:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = ?",
                (f"sponsors_{user_id}",)
            ) as c:
                row = await c.fetchone()
                if row:
                    payload, expires_at = row
                    if expires_at > time.time():
                        return json.loads(payload)
    
    sponsors = await get_sponsors_from_piarflow(user_id)
    
    if sponsors:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO sponsor_cache (cache_key, payload, expires_at) VALUES (?, ?, ?)",
                (f"sponsors_{user_id}", json.dumps(sponsors), time.time() + 300)
            )
            await db.commit()
    return sponsors

async def check_all_subscriptions(user_id: int):
    sponsors = await get_user_sponsors_from_db(user_id)
    if not sponsors:
        return True
    
    sponsor_ids = [s["offer_id"] for s in sponsors if s["service"] == "piarflow"]
    if sponsor_ids:
        results = await check_subscriptions_piarflow(user_id, sponsor_ids)
        for r in results:
            if r.get("status") != "subscribed":
                return False
    
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
        link = sp.get("link")
        if link:
            builder.row(types.InlineKeyboardButton(
                text=f"📢 Канал #{idx}",
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
    
    sponsors = await get_all_sponsors(user_id)
    
    if sponsors:
        await message.answer(
            "📌 Подпишитесь на каналы спонсоров для доступа к боту:",
            reply_markup=sponsors_keyboard(sponsors)
        )
    else:
        await activate_user(user_id)
        await message.answer(
            "🎉 Добро пожаловать! Вы уже активированы.",
            reply_markup=main_menu()
        )

@dp.callback_query(F.data == "check_subs")
async def check_subs_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    is_activated = await activate_user(user_id)
    
    if is_activated:
        await callback.message.delete()
        await callback.message.answer(
            "🎉 Все подписки подтверждены! Вы активированы.",
            reply_markup=main_menu()
        )
    else:
        await callback.answer("❌ Вы подписались не на все каналы!", show_alert=True)
        sponsors = await get_all_sponsors(user_id, force_refresh=True)
        if sponsors:
            await callback.message.edit_reply_markup(
                reply_markup=sponsors_keyboard(sponsors)
            )

@dp.message(F.text == "💰 Баланс")
async def balance_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        await message.answer(f"💳 Ваш баланс: {user['balance']} ⭐")

@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if user:
        text = (
            f"👤 *Профиль*\n"
            f"🆔 ID: `{user['user_id']}`\n"
            f"💰 Баланс: {user['balance']} ⭐\n"
            f"👥 Рефералов: {user['referrals_count']}\n"
            f"📊 Всего заработано: {user['total_earned']} ⭐"
        )
        await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "⭐ Заработать звёзды")
async def earn_cmd(message: types.Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user or user.get('is_activated') != 1:
        await message.answer("❌ Сначала активируйтесь через подписку на каналы!")
        return
    
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start={user_id}"
    await message.answer(
        f"🔗 Твоя реферальная ссылка:\n{ref_link}\n\n"
        f"Приглашай друзей и получай 3 ⭐ за каждого активированного!",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🎟 Ввести промокод")
async def promo_start(message: types.Message):
    await message.answer("🎟 Введите промокод (например: START2026):")

# ========== ВЕБ-СЕРВЕР ДЛЯ RENDER ==========
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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")
