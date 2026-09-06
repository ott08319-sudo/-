import os
import asyncio
import logging
import random
from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from database import init_db, get_user, register_user, update_balance
from sponsors import get_all_sponsors, activate_user, check_all_subscriptions

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== FSM СОСТОЯНИЯ ==========
class UserState(StatesGroup):
    waiting_for_promo_code = State()

# ========== КЛАВИАТУРЫ ==========
def main_menu():
    builder = ReplyKeyboardBuilder()
    builder.row(
        types.KeyboardButton(text="⭐ Заработать звёзды"),
        types.KeyboardButton(text="💰 Баланс")
    )
    builder.row(
        types.KeyboardButton(text="🎁 Ежедневный бонус"),
        types.KeyboardButton(text="💎 Вывести звёзды")
    )
    builder.row(
        types.KeyboardButton(text="🎰 Казино"),
        types.KeyboardButton(text="🎟 Ввести промокод")
    )
    builder.row(types.KeyboardButton(text="👤 Профиль"))
    
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
