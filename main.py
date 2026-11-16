import os
import re
import json
import time
import html
import random
import asyncio
import logging
from datetime import datetime

import asyncpg
import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

try:
    from flyerapi import Flyer
except Exception:
    Flyer = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
FLOWLY_API_KEY = os.getenv("FLOWLY_API_KEY", "")
FLYER_API_KEY = os.getenv("FLYER_API_KEY", "")
MGRAM_API_KEY = os.getenv("MGRAM_API_KEY", "")
TRAFSLY_API_KEY = os.getenv("TRAFSLY_API_KEY", "")
BOTOHUB_API_KEY = os.getenv("BOTOHUB_API_KEY", "")

DEFAULT_REWARD = 0.5
DEFAULT_REF_BONUS = 0.25
DEFAULT_MIN_WITHDRAW = 15.0
DEFAULT_MAX_SPONSORS = 6
MAX_MAX_SPONSORS = 20

CHECK_COOLDOWN = 3.0
FLOWLY_TIMEOUT = 6.0
AUTOCHECK_INTERVAL = 3600      # Поиск новых заданий для свежих юзеров
LAZY_INTERVAL = 1800           # Напоминание неактивным рефералам
LAZY_THRESHOLD = 2
AUTO_CHECK_FAST_INTERVAL = 6   # Быстрая автопроверка (сек) для активных юзеров
AUTO_CHECK_ACTIVE_WINDOW = 600 # Окно активности после получения списка (10 минут)
TASK_EXPIRY_SECONDS = 3600     # Через сколько невыполненные старые задания считаются устаревшими

SERVICES_ALL = ("piarflow", "flowly", "flyer", "mgram", "trafsly", "botohub")
INCOMPLETE_STATUSES = ("incomplete", "waiting", "pending", "not_subscribed", None, "")

DAILY_REWARDS = [0.25, 0.35, 0.5, 0.65, 0.8, 1.0, 1.5]
REF_ACTIVE_MIN_SUBS = 2
REF_MILESTONES = {
    3: 1.5,
    10: 5.0,
    25: 15.0,
    50: 35.0,
    100: 80.0,
}

NICKNAMES = [
    "солнышко", "котик", "зайка", "чемпион", "звёздочка",
    "красавчик", "умничка", "герой", "счастливчик", "легенда"
]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
flyer = Flyer(FLYER_API_KEY) if (Flyer and FLYER_API_KEY) else None

_pool: asyncpg.Pool | None = None
_http: aiohttp.ClientSession | None = None
_states: dict[int, str] = {}
_last: dict[int, float] = {}
_user_locks: dict[int, asyncio.Lock] = {}
_auto_checking: set[int] = set()
_last_sponsors_view: dict[int, float] = {}


# ========== УТИЛИТЫ ==========
def escape_html(text) -> str:
    return html.escape(str(text or ""))


def fmt_money(val) -> str:
    try:
        v = float(val or 0)
        if v.is_integer():
            return str(int(v))
        return f"{v:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return "0"


def pick_nickname() -> str:
    return random.choice(NICKNAMES)


def user_display(uid: int, username: str | None) -> str:
    if username:
        clean = username.lstrip("@")
        return f'<a href="https://t.me/{escape_html(clean)}">@{escape_html(clean)}</a> (<code>{uid}</code>)'
    return f'<a href="tg://user?id={uid}">Игрок</a> (<code>{uid}</code>)'


async def get_user_display(uid: int) -> str:
    u = await get_user(uid)
    return user_display(uid, u["username"] if u else None)


async def http() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=10)
        _http = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _http


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=15)
    return _pool


async def log_error(service: str, error_type: str, message: str):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO error_log (service, error_type, message, created_at) VALUES ($1,$2,$3,$4)",
                service, error_type, str(message)[:500], time.time()
            )
    except Exception:
        pass


# ========== УВЕДОМЛЕНИЯ ==========
async def notify_admin_new_user(uid: int, un: str, full_name: str, total_tasks: int, services_map: dict, ref_id: int | None):
    if not ADMIN_ID:
        return
    svc_str = ", ".join(f"{k}: {v}" for k, v in services_map.items()) if services_map else "нет"
    ref_str = f"<code>{ref_id}</code>" if ref_id else "нет"
    text = (
        f"👤 <b>Новый пользователь!</b>\n\n"
        f"🆔 <code>{uid}</code> {escape_html(un)}\n"
        f"📛 {escape_html(full_name)}\n"
        f"🔗 Реферер: {ref_str}\n"
        f"📦 Заданий выдано: <b>{total_tasks}</b> ({escape_html(svc_str)})"
    )
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception:
        pass


async def notify_admin_new_subs(uid: int, new_subs: list):
    if not ADMIN_ID or not new_subs:
        return
    u_disp = await get_user_display(uid)
    by_svc: dict[str, list] = {}
    total_rw = 0.0
    for svc, aid, rw, link, tt in new_subs:
        by_svc.setdefault(svc, []).append((link or aid, rw))
        total_rw += rw

    lines = [
        f"🆕 <b>Новые подписки!</b>\n",
        f"👤 {u_disp}\n",
        f"📦 <b>{len(new_subs)} заданий:</b>\n"
    ]
    for svc, items in by_svc.items():
        svc_sum = sum(x[1] for x in items)
        lines.append(f"⭐ <b>{escape_html(svc)}</b> ({len(items)} шт, +{fmt_money(svc_sum)}⭐)")
        for lnk, _ in items[:15]:
            lines.append(f"  • {escape_html(lnk)}")
        if len(items) > 15:
            lines.append(f"  <i>…и ещё {len(items) - 15}</i>")
        lines.append("")

    ts = time.strftime("%d.%m %H:%M", time.localtime())
    lines.append(f"⭐ Итого: <b>+{fmt_money(total_rw)}⭐</b>\n🕐 {ts}")
    try:
        await bot.send_message(ADMIN_ID, "\n".join(lines)[:4000], disable_web_page_preview=True)
    except Exception:
        pass


async def notify_referrer(ref_id: int, sub_username: str, count: int, total_bonus: float):
    who = f"@{escape_html(sub_username)}" if sub_username else "Твой друг"
    try:
        await bot.send_message(
            ref_id,
            f"👥 <b>Реферальный бонус!</b>\n\n"
            f"🎉 {who} выполнил заданий: <b>{count}</b>\n"
            f"⭐ Тебе начислено: <b>+{fmt_money(total_bonus)}⭐</b>"
        )
    except Exception:
        pass


async def notify_lazy_user(uid: int) -> bool:
    nick = pick_nickname()
    try:
        await bot.send_message(
            uid,
            f"👋 <b>Эй, {nick}!</b>\n\n"
            f"У тебя остались невыполненные задания — забери свои бесплатные ⭐ прямо сейчас!\n"
            f"Нажми «⭐ Получить» 👇",
            reply_markup=get_menu(uid)
        )
        return True
    except Exception:
        return False


async def notify_admin_lazy(uid: int, total_subs: int, mins: int):
    if not ADMIN_ID:
        return
    try:
        await bot.send_message(
            ADMIN_ID,
            f"😴 <b>Неактивный реферал</b>\n\n"
            f"👤 {await get_user_display(uid)}\n"
            f"⏱ Прошло: {mins} мин · Подписок: {total_subs}",
            disable_web_page_preview=True
        )
    except Exception:
        pass


# ========== БАЗА ДАННЫХ ==========
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            balance DOUBLE PRECISION DEFAULT 0,
            referrer_id BIGINT,
            referred_earned DOUBLE PRECISION DEFAULT 0,
            blocked BOOLEAN DEFAULT FALSE,
            sponsors_msg_id BIGINT,
            daily_streak INT DEFAULT 0,
            last_daily_at DOUBLE PRECISION DEFAULT 0,
            created_at DOUBLE PRECISION
        )""")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS sponsors_msg_id BIGINT")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_streak INT DEFAULT 0")
        await db.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_at DOUBLE PRECISION DEFAULT 0")

        await db.execute("""CREATE TABLE IF NOT EXISTS ref_milestones_claimed (
            user_id BIGINT,
            milestone INT,
            claimed_at DOUBLE PRECISION,
            PRIMARY KEY (user_id, milestone)
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS sponsor_tasks (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            service TEXT,
            assignment_id TEXT,
            link TEXT,
            reward DOUBLE PRECISION,
            signature TEXT,
            task_type TEXT,
            session_id TEXT,
            provider_pool TEXT,
            status TEXT DEFAULT 'unsubscribed',
            created_at DOUBLE PRECISION,
            UNIQUE (user_id, service, assignment_id)
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount DOUBLE PRECISION,
            wallet TEXT,
            status TEXT DEFAULT 'pending',
            created_at DOUBLE PRECISION
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            amount DOUBLE PRECISION,
            max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0,
            active BOOLEAN DEFAULT TRUE,
            created_at DOUBLE PRECISION
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            user_id BIGINT,
            code TEXT,
            used_at DOUBLE PRECISION,
            PRIMARY KEY (user_id, code)
        )""")

        await db.execute("""CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY,
            service TEXT,
            error_type TEXT,
            message TEXT,
            created_at DOUBLE PRECISION
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS lazy_watch (
            user_id BIGINT PRIMARY KEY,
            referrer_id BIGINT,
            created_at DOUBLE PRECISION,
            last_notified DOUBLE PRECISION DEFAULT 0,
            admin_notified BOOLEAN DEFAULT FALSE
        )""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_user_status ON sponsor_tasks (user_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_st_service ON sponsor_tasks (service)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users (balance DESC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_wd_status ON withdrawals (status)")

        await db.execute("INSERT INTO settings (key,value) VALUES ('reward',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REWARD))
        await db.execute("INSERT INTO settings (key,value) VALUES ('ref_bonus',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_REF_BONUS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('min_withdraw',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MIN_WITHDRAW))
        await db.execute("INSERT INTO settings (key,value) VALUES ('max_sponsors',$1) ON CONFLICT (key) DO NOTHING", str(DEFAULT_MAX_SPONSORS))
        await db.execute("INSERT INTO settings (key,value) VALUES ('autocheck','true') ON CONFLICT (key) DO NOTHING")


async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default


async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO settings (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2",
            key, str(value)
        )


async def get_reward():
    return float(await get_setting("reward", DEFAULT_REWARD))


async def get_ref_bonus():
    return float(await get_setting("ref_bonus", DEFAULT_REF_BONUS))


async def get_min_withdraw():
    return float(await get_setting("min_withdraw", DEFAULT_MIN_WITHDRAW))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def is_service_enabled(service: str) -> bool:
    return (await get_setting(f"service_{service}", "true")) == "true"


async def set_service_enabled(service: str, enabled: bool):
    await set_setting(f"service_{service}", "true" if enabled else "false")


async def get_enabled_services() -> set:
    result = set()
    for s in SERVICES_ALL:
        if await is_service_enabled(s):
            result.add(s)
    return result


def get_user_lock(uid: int) -> asyncio.Lock:
    if uid not in _user_locks:
        _user_locks[uid] = asyncio.Lock()
    return _user_locks[uid]


async def check_all_safe(uid: int):
    lock = get_user_lock(uid)
    if lock.locked():
        return 0, 0.0, 0
    async with lock:
        return await check_all(uid)


async def set_sponsors_msg_id(user_id: int, msg_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET sponsors_msg_id=$1 WHERE user_id=$2", msg_id, user_id)


async def get_sponsors_msg_id(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT sponsors_msg_id FROM users WHERE user_id=$1", user_id)
    return row["sponsors_msg_id"] if row else None


async def clear_sponsors_msg_id(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET sponsors_msg_id=NULL WHERE user_id=$1", user_id)


async def register_user(user_id: int, referrer_id=None, username=None) -> bool:
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", user_id)
        if exists:
            if username:
                await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
            return False
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r:
                referrer_id = None
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1,$2,$3,$4)",
            user_id, username, referrer_id, time.time()
        )
        return True


async def update_username(user_id: int, username: str | None):
    if not username:
        return
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute("UPDATE users SET username=$1 WHERE user_id=$2", username, user_id)
    except Exception:
        pass


async def get_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetchrow(
            "SELECT balance, referrer_id, referred_earned, blocked, username, daily_streak, last_daily_at FROM users WHERE user_id=$1",
            user_id
        )


async def find_user(query: str):
    query = query.strip()
    if query.isdigit():
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", int(query))
        return row["user_id"] if row else None
    clean = query.lstrip("@").lower()
    if not clean:
        return None
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id FROM users WHERE LOWER(username)=$1 LIMIT 1", clean)
    return row["user_id"] if row else None


async def add_balance(user_id: int, amount: float):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", amount, user_id)


async def add_referred_earned(user_id: int, amount: float):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET referred_earned = referred_earned + $1 WHERE user_id=$2", amount, user_id)


async def save_sponsor(user_id, service, aid, link, reward, signature=None, task_type=None, session_id=None, provider_pool=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            """INSERT INTO sponsor_tasks
            (user_id, service, assignment_id, link, reward, signature, task_type, session_id, provider_pool, status, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'unsubscribed',$10)
            ON CONFLICT (user_id, service, assignment_id) DO UPDATE SET
                created_at = CASE WHEN sponsor_tasks.status != 'subscribed' THEN EXCLUDED.created_at ELSE sponsor_tasks.created_at END,
                signature = COALESCE(EXCLUDED.signature, sponsor_tasks.signature)""",
            user_id, service, str(aid), link, reward,
            str(signature) if signature is not None else None,
            task_type,
            str(session_id) if session_id is not None else None,
            str(provider_pool) if provider_pool is not None else None,
            time.time()
        )


async def mark_subscribed(user_id, service, aid) -> bool:
    pool = await get_pool()
    async with pool.acquire() as db:
        res = await db.execute(
            "UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'",
            user_id, service, str(aid)
        )
        return res != "UPDATE 0"


async def delete_stale_task(user_id, service, aid):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "DELETE FROM sponsor_tasks WHERE user_id=$1 AND service=$2 AND assignment_id=$3 AND status!='subscribed'",
            user_id, service, str(aid)
        )


async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)


async def cleanup_expired_tasks():
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "DELETE FROM sponsor_tasks WHERE status!='subscribed' AND created_at < $1",
            time.time() - TASK_EXPIRY_SECONDS
        )


async def pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            """SELECT service, assignment_id, link, reward, signature, task_type, session_id, provider_pool
            FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' AND created_at > $2""",
            user_id, time.time() - TASK_EXPIRY_SECONDS
        )
        return [
            (r["service"], r["assignment_id"], r["link"], r["reward"], r["signature"], r["task_type"], r["session_id"], r["provider_pool"])
            for r in rows
        ]


async def pending_count(user_id) -> int:
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' AND created_at > $2",
            user_id, time.time() - TASK_EXPIRY_SECONDS
        )
        return row["c"]


async def pending_links(user_id, limit=None) -> list[str]:
    pool = await get_pool()
    async with pool.acquire() as db:
        q = "SELECT link FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' AND created_at > $2 ORDER BY id ASC"
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = await db.fetch(q, user_id, time.time() - TASK_EXPIRY_SECONDS)
        return [r["link"] for r in rows]


async def pending_by_service(user_id) -> dict[str, int]:
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT service, COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed' GROUP BY service",
            user_id
        )
        return {r["service"]: r["c"] for r in rows}


async def count_total_subs(user_id) -> int:
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id)
        return row["c"]


async def add_lazy_watch(user_id, referrer_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "INSERT INTO lazy_watch (user_id, referrer_id, created_at, last_notified, admin_notified) VALUES ($1,$2,$3,0,FALSE) ON CONFLICT (user_id) DO NOTHING",
            user_id, referrer_id, time.time()
        )


async def remove_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM lazy_watch WHERE user_id=$1", user_id)


# ========== ПРОВЕРКА ВЕХ ЗА РЕФЕРАЛОВ ==========
async def check_ref_milestones(referrer_id: int):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            """SELECT COUNT(*) as c FROM users u
               WHERE u.referrer_id = $1
               AND (SELECT COUNT(*) FROM sponsor_tasks st WHERE st.user_id = u.user_id AND st.status = 'subscribed') >= $2""",
            referrer_id, REF_ACTIVE_MIN_SUBS
        )
        active_refs = row["c"] if row else 0
        claimed_rows = await db.fetch("SELECT milestone FROM ref_milestones_claimed WHERE user_id=$1", referrer_id)
        claimed = {r["milestone"] for r in claimed_rows}

        for m_count, bonus in sorted(REF_MILESTONES.items()):
            if active_refs >= m_count and m_count not in claimed:
                await db.execute(
                    "INSERT INTO ref_milestones_claimed (user_id, milestone, claimed_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    referrer_id, m_count, time.time()
                )
                await add_balance(referrer_id, bonus)
                await add_referred_earned(referrer_id, bonus)
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🏆 <b>Достижение разблокировано!</b>\n\n"
                        f"Ты пригласил <b>{m_count} активных друзей</b>!\n"
                        f"🎁 Твой бонус: <b>+{fmt_money(bonus)}⭐</b> зачислен на баланс!"
                    )
                except Exception:
                    pass


# ========== API: PIARFLOW ==========
async def get_piarflow(user):
    if not PIARFLOW_API_KEY:
        return []
    s = await http()
    try:
        payload = {
            "user_id": user.id,
            "chat_id": user.id,
            "max_sponsors": await get_max_sponsors(),
            "language_code": getattr(user, "language_code", None) or "ru",
            "is_premium": bool(getattr(user, "is_premium", False)),
            "username": getattr(user, "username", None) or "",
            "first_name": getattr(user, "first_name", None) or "",
            "last_name": getattr(user, "last_name", None) or "",
        }
        async with s.post(
            "https://piarflow.com/v1/sponsors",
            json=payload,
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status == 404:
                return []
            if r.status != 200:
                await log_error("piarflow", f"http_{r.status}", await r.text())
                return []
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors") or []
    except Exception as e:
        await log_error("piarflow", "fetch", str(e))
    return []


async def check_piarflow(user_id, links):
    if not PIARFLOW_API_KEY or not links:
        return []
    s = await http()
    try:
        async with s.post(
            "https://piarflow.com/v1/sponsors/check",
            json={"user_id": int(user_id), "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors") or []
    except Exception as e:
        await log_error("piarflow", "check", str(e))
    return []


# ========== API: FLOWLY ==========
async def _flowly_request(user_id, lang="ru", is_premium=False):
    if not FLOWLY_API_KEY:
        return None
    s = await http()
    try:
        async with s.post(
            "https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY, "Content-Type": "application/json"},
            json={"tg_user_id": int(user_id), "lang": lang or "ru", "is_premium": bool(is_premium)},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200:
                return None
            return await r.json()
    except Exception:
        return None


async def get_flowly(user):
    lang = getattr(user, "language_code", None) or "ru"
    is_prem = bool(getattr(user, "is_premium", False))
    d = await _flowly_request(user.id, lang, is_prem)
    if not d:
        return []
    out = []
    for item in d.get("offers") or []:
        if item.get("subscribed") is True:
            continue
        link = item.get("link")
        if link:
            out.append({"id": link, "link": link, "name": item.get("name", "")})
    return out


async def check_flowly(user_id, lang="ru"):
    d = await _flowly_request(user_id, lang)
    if not d:
        return None, {}
    offers_map = {}
    for item in d.get("offers") or []:
        link = item.get("link")
        if link:
            offers_map[link] = bool(item.get("subscribed"))
    return d.get("status"), offers_map


# ========== API: FLYER ==========
async def fetch_flyer(user_id, lang="ru"):
    if not flyer:
        return []
    try:
        tasks = await flyer.get_tasks(user_id=int(user_id), language_code=lang or "ru", limit=await get_max_sponsors())
        if not tasks:
            return []
        incomplete = [t for t in tasks if t.get("status") in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get("links") or []
            sig = t.get("signature")
            if not links or not sig:
                continue
            out.append({"id": sig, "link": links[0], "signature": sig, "task": t.get("task", "")})
        return out
    except Exception as e:
        await log_error("flyer", "get_tasks", str(e))
    return []


async def check_flyer(signature):
    if not flyer or not signature:
        return None
    try:
        return await flyer.check_task(signature=signature)
    except Exception as e:
        await log_error("flyer", "check", str(e))
    return None


# ========== API: MGRAM ==========
async def get_mgram(user_id):
    if not MGRAM_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://api.mgram.biz/tasks",
            json={"user_id": int(user_id), "limit": min(await get_max_sponsors(), 10)},
            headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            data = d.get("data") or {}
            if data.get("skip"):
                return []
            out = []
            for t in data.get("tasks") or []:
                if t.get("status") == "completed":
                    continue
                link = t.get("link")
                aid = t.get("id")
                if link and aid is not None:
                    out.append({"id": aid, "link": link})
            return out
    except Exception as e:
        await log_error("mgram", "fetch", str(e))
    return []


async def check_mgram(user_id) -> bool:
    if not MGRAM_API_KEY:
        return False
    s = await http()
    try:
        async with s.post(
            "https://api.mgram.biz/tasks/check",
            json={"user_id": int(user_id)},
            headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return False
            d = await r.json()
            return (d.get("data") or {}).get("skip") is True
    except Exception as e:
        await log_error("mgram", "check", str(e))
    return False


# ========== API: TRAFSLY ==========
async def get_trafsly(user):
    if not TRAFSLY_API_KEY:
        return []
    s = await http()
    try:
        payload = {
            "user_id": int(user.id),
            "first_name": getattr(user, "first_name", "") or "",
            "username": getattr(user, "username", "") or "",
            "language_code": getattr(user, "language_code", None) or "ru",
            "is_premium": bool(getattr(user, "is_premium", False)),
        }
        async with s.post(
            "https://api.trafsly.com/api/v1/get-sponsors",
            json=payload,
            headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            out = []
            for x in d.get("sponsors") or []:
                if x.get("status") in ("subscribed", "notgetted", "completed"):
                    continue
                link = x.get("link")
                aid = x.get("ads_id") or x.get("id")
                if link and aid is not None:
                    out.append({
                        "id": str(aid),
                        "link": link,
                        "source": x.get("source"),
                        "provider_pool": x.get("provider_pool")
                    })
            return out
    except Exception as e:
        await log_error("trafsly", "fetch", str(e))
    return []


async def check_trafsly(user_id, ads_id, source=None, provider_pool=None) -> bool:
    if not TRAFSLY_API_KEY or not ads_id:
        return False
    s = await http()
    try:
        payload = {"user_id": int(user_id), "ads_id": str(ads_id)}
        if source:
            payload["source"] = source
        if provider_pool:
            payload["provider_pool"] = provider_pool
        async with s.post(
            "https://api.trafsly.com/api/v1/confirm-subscription",
            json=payload,
            headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return False
            d = await r.json()
            return d.get("subscribed") is True or d.get("status") == "subscribed"
    except Exception as e:
        await log_error("trafsly", "check", str(e))
    return False


# ========== API: BOTOHUB ==========
def _extract_botohub_items(d):
    if not isinstance(d, dict):
        return []
    if d.get("skip") is True or d.get("completed") is True:
        return []
    for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
        v = d.get(field)
        if isinstance(v, list):
            return v
    return []


async def get_botohub(user_id):
    if not BOTOHUB_API_KEY:
        return []
    s = await http()
    try:
        async with s.post(
            "https://botohub.me/get-tasks",
            json={"chat_id": int(user_id)},
            headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return []
            d = await r.json()
            return _extract_botohub_items(d)
    except Exception as e:
        await log_error("botohub", "fetch", str(e))
    return []


async def check_botohub_all_completed(user_id) -> bool:
    """
    По официальной документации botohub.me/integration:
    Эндпоинт /get-tasks возвращает {"skip": true} ТОЛЬКО когда пользователь
    выполнил ВСЕ выданные задания. Нельзя засчитывать по исчезновению ссылок
    из-за ротации пула.
    """
    if not BOTOHUB_API_KEY:
        return False
    s = await http()
    try:
        async with s.post(
            "https://botohub.me/get-tasks",
            json={"chat_id": int(user_id)},
            headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}
        ) as r:
            if r.status != 200:
                return False
            d = await r.json()
            if not isinstance(d, dict):
                return False
            return d.get("skip") is True or d.get("completed") is True
    except Exception as e:
        await log_error("botohub", "check", str(e))
    return False


# ========== СБОР СПОНСОРОВ ==========
async def collect_sponsors(user):
    uid = user.id
    lang = getattr(user, "language_code", None) or "ru"
    reward = await get_reward()
    enabled = await get_enabled_services()
    max_sp = await get_max_sponsors()

    async def _call(name, coro):
        if name in enabled:
            return await coro
        return []

    results = await asyncio.gather(
        _call("piarflow", get_piarflow(user)),
        _call("flowly", get_flowly(user)),
        _call("flyer", fetch_flyer(uid, lang)),
        _call("mgram", get_mgram(uid)),
        _call("trafsly", get_trafsly(user)),
        _call("botohub", get_botohub(uid)),
        return_exceptions=True,
    )

    piarflow, flowly, flyer_, mgram, trafsly, botohub = results

    if isinstance(piarflow, list):
        for x in piarflow[:max_sp]:
            if x.get("link"):
                await save_sponsor(uid, "piarflow", x["link"], x["link"], reward)
    if isinstance(flowly, list):
        for x in flowly[:max_sp]:
            if x.get("link"):
                await save_sponsor(uid, "flowly", x["link"], x["link"], reward, task_type=x.get("name"))
    if isinstance(flyer_, list):
        for x in flyer_[:max_sp]:
            if x.get("link") and x.get("id"):
                await save_sponsor(uid, "flyer", str(x["id"]), x["link"], reward, signature=x.get("signature"), task_type=x.get("task"))
    if isinstance(mgram, list):
        for x in mgram[:max_sp]:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "mgram", str(x["id"]), x["link"], reward)
    if isinstance(trafsly, list):
        for x in trafsly[:max_sp]:
            if x.get("link") and x.get("id") is not None:
                await save_sponsor(uid, "trafsly", str(x["id"]), x["link"], reward, signature=x.get("source"), provider_pool=x.get("provider_pool"))
    if isinstance(botohub, list):
        for t in botohub[:max_sp]:
            link = None
            aid = None
            if isinstance(t, str):
                link, aid = t, t
            elif isinstance(t, dict):
                link = t.get("link") or t.get("url") or t.get("target_link") or t.get("target")
                aid = t.get("id") or t.get("task_id") or t.get("assignment_id") or link
            if link:
                await save_sponsor(uid, "botohub", str(aid) if aid is not None else link, link, reward)


# ========== ПАРАЛЛЕЛЬНАЯ ПРОВЕРКА ПОДПИСОК ==========
async def check_all(user_id: int):
    tasks = await pending_tasks(user_id)
    if not tasks:
        return 0, 0.0, 0

    enabled = await get_enabled_services()

    pf, fl, mg, tr, bh, flowly_pending = [], [], [], [], [], []
    link_map, reward_map, task_type_map = {}, {}, {}

    for service, aid, link, reward, sig, task_type, _, pool_ in tasks:
        if service not in enabled:
            continue
        key = (service, str(aid))
        link_map[key] = link
        reward_map[key] = float(reward or 0.0)
        task_type_map[key] = task_type

        if service == "piarflow" and link:
            pf.append(link)
        elif service == "flyer" and sig:
            fl.append((aid, sig))
        elif service == "mgram" and aid:
            mg.append(aid)
        elif service == "trafsly" and aid:
            tr.append((aid, sig, pool_))
        elif service == "botohub" and aid:
            bh.append((aid, link))
        elif service == "flowly":
            flowly_pending.append((aid, link, task_type))

    async def _chk_pf():
        return await check_piarflow(user_id, pf) if pf else []

    async def _chk_mg():
        return await check_mgram(user_id) if mg else False

    async def _chk_tr():
        if not tr:
            return []
        res = await asyncio.gather(*(check_trafsly(user_id, aid, src, pp) for aid, src, pp in tr), return_exceptions=True)
        return [(tr[i][0], r is True) for i, r in enumerate(res)]

    async def _chk_bh():
        return await check_botohub_all_completed(user_id) if bh else False

    async def _chk_fl():
        if not fl:
            return []
        res = await asyncio.gather(*(check_flyer(sig) for _, sig in fl), return_exceptions=True)
        return [(fl[i][0], r) for i, r in enumerate(res)]

    async def _chk_flowly():
        return await check_flowly(user_id) if flowly_pending else (None, {})

    pf_res, mg_done, tr_res, bh_done, fl_res, flowly_res = await asyncio.gather(
        _chk_pf(), _chk_mg(), _chk_tr(), _chk_bh(), _chk_fl(), _chk_flowly(),
        return_exceptions=True
    )

    done_count = 0
    done_sum = 0.0
    skipped_count = 0
    new_subs = []

    # 1. Piarflow
    if isinstance(pf_res, list):
        for r in pf_res:
            st = r.get("status")
            lnk = r.get("link")
            if st == "subscribed" and lnk:
                if await mark_subscribed(user_id, "piarflow", lnk):
                    rw = reward_map.get(("piarflow", str(lnk)), 0.0)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("piarflow", lnk, rw, lnk, None))
            elif st == "not_counted" and lnk:
                await delete_stale_task(user_id, "piarflow", lnk)
                skipped_count += 1

    # 2. MGRAM (пакетная проверка: skip == True)
    if mg_done is True and mg:
        for aid in mg:
            if await mark_subscribed(user_id, "mgram", aid):
                rw = reward_map.get(("mgram", str(aid)), 0.0)
                done_count += 1
                done_sum += rw
                new_subs.append(("mgram", aid, rw, link_map.get(("mgram", str(aid))), None))

    # 3. Trafsly
    if isinstance(tr_res, list):
        for aid, ok in tr_res:
            if ok and await mark_subscribed(user_id, "trafsly", aid):
                rw = reward_map.get(("trafsly", str(aid)), 0.0)
                done_count += 1
                done_sum += rw
                new_subs.append(("trafsly", aid, rw, link_map.get(("trafsly", str(aid))), None))

    # 4. Botohub (безопасная проверка: только если skip == True)
    if bh_done is True and bh:
        for aid, link in bh:
            if await mark_subscribed(user_id, "botohub", aid):
                rw = reward_map.get(("botohub", str(aid)), 0.0)
                done_count += 1
                done_sum += rw
                new_subs.append(("botohub", aid, rw, link, None))

    # 5. Flyer
    if isinstance(fl_res, list):
        for aid, res_fl in fl_res:
            if res_fl in ("complete", "subscribed"):
                if await mark_subscribed(user_id, "flyer", aid):
                    rw = reward_map.get(("flyer", str(aid)), 0.0)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("flyer", aid, rw, link_map.get(("flyer", str(aid))), task_type_map.get(("flyer", str(aid)))))

    # 6. Flowly (строго по явному подтверждению API)
    if isinstance(flowly_res, tuple):
        status, offers_map = flowly_res
        if status is not None:
            for aid, link, tt in flowly_pending:
                is_sub = (status == "ok") or (offers_map.get(link) is True)
                if is_sub and await mark_subscribed(user_id, "flowly", aid):
                    rw = reward_map.get(("flowly", str(aid)), 0.0)
                    done_count += 1
                    done_sum += rw
                    new_subs.append(("flowly", aid, rw, link, tt))

    if done_sum > 0:
        await add_balance(user_id, done_sum)

    # Начисление рефереру и проверка этапов
    if done_count > 0:
        pool = await get_pool()
        async with pool.acquire() as db:
            row = await db.fetchrow("SELECT referrer_id, username FROM users WHERE user_id=$1", user_id)
        if row and row["referrer_id"] and row["referrer_id"] != user_id:
            ref_id = row["referrer_id"]
            ref_total = (await get_ref_bonus()) * done_count
            if ref_total > 0:
                await add_balance(ref_id, ref_total)
                await add_referred_earned(ref_id, ref_total)
                await notify_referrer(ref_id, row["username"] or "", done_count, ref_total)
            await check_ref_milestones(ref_id)

    if new_subs:
        await notify_admin_new_subs(user_id, new_subs)

    return done_count, done_sum, skipped_count


# ========== ВОРКЕРЫ ==========
async def autocheck_worker():
    await asyncio.sleep(300)
    while True:
        try:
            await cleanup_expired_tasks()
            if await get_setting("autocheck", "true") == "true":
                pool = await get_pool()
                async with pool.acquire() as db:
                    rows = await db.fetch(
                        "SELECT user_id, username FROM users WHERE blocked = FALSE AND created_at > $1 LIMIT 300",
                        time.time() - 86400
                    )
                for r in rows:
                    uid = r["user_id"]
                    try:
                        before = await pending_count(uid)
                        if before > 0:
                            continue
                        user = types.User(id=uid, is_bot=False, first_name="", language_code="ru", username=r["username"] or "")
                        await collect_sponsors(user)
                        after = await pending_count(uid)
                        if after > before:
                            nick = pick_nickname()
                            try:
                                await bot.send_message(
                                    uid,
                                    f"🔔 <b>Новые задания ждут тебя, {nick}!</b>\n\n⭐ Доступно: <b>{after - before}</b>\nНажми «⭐ Получить»."
                                )
                            except Exception:
                                pass
                    except Exception as e:
                        logging.error(f"autocheck {uid}: {e}")
                    await asyncio.sleep(0.5)
            await asyncio.sleep(AUTOCHECK_INTERVAL)
        except Exception as e:
            logging.error(f"autocheck_worker: {e}")
            await asyncio.sleep(600)


async def lazy_worker():
    await asyncio.sleep(60)
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("SELECT user_id, referrer_id, created_at, last_notified, admin_notified FROM lazy_watch")
            for r in rows:
                uid = r["user_id"]
                try:
                    total = await count_total_subs(uid)
                    if total >= LAZY_THRESHOLD:
                        await remove_lazy_watch(uid)
                        continue
                    if await pending_count(uid) == 0:
                        continue
                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL:
                        continue
                    if await notify_lazy_user(uid):
                        pool2 = await get_pool()
                        async with pool2.acquire() as db:
                            await db.execute("UPDATE lazy_watch SET last_notified=$1 WHERE user_id=$2", time.time(), uid)
                        if not r["admin_notified"]:
                            mins = int((time.time() - (r["created_at"] or time.time())) / 60)
                            await notify_admin_lazy(uid, total, mins)
                            pool3 = await get_pool()
                            async with pool3.acquire() as db:
                                await db.execute("UPDATE lazy_watch SET admin_notified=TRUE WHERE user_id=$1", uid)
                except Exception as e:
                    logging.error(f"lazy check {uid}: {e}")
                await asyncio.sleep(0.3)
            await asyncio.sleep(LAZY_INTERVAL)
        except Exception as e:
            logging.error(f"lazy_worker: {e}")
            await asyncio.sleep(120)


# ========== АВТОПРОВЕРКА ПОДПИСОК (БЫСТРАЯ И БЕЗ СПАМА) ==========
async def auto_check_worker():
    await asyncio.sleep(10)
    while True:
        try:
            now = time.time()
            expired_views = [u for u, ts in _last_sponsors_view.items() if now - ts > AUTO_CHECK_ACTIVE_WINDOW]
            for u in expired_views:
                _last_sponsors_view.pop(u, None)

            active_uids = list(_last_sponsors_view.keys())
            for uid in active_uids:
                if uid in _auto_checking:
                    continue
                if await pending_count(uid) == 0:
                    _last_sponsors_view.pop(uid, None)
                    continue

                _auto_checking.add(uid)
                try:
                    cnt, sm, _ = await check_all_safe(uid)
                    if cnt > 0:
                        _last_sponsors_view[uid] = time.time()

                        old_mid = await get_sponsors_msg_id(uid)
                        if old_mid:
                            try:
                                await bot.delete_message(uid, old_mid)
                            except Exception:
                                pass
                            await clear_sponsors_msg_id(uid)

                        left_links = await pending_links(uid, limit=await get_max_sponsors())
                        nick = pick_nickname()

                        if not left_links:
                            _last_sponsors_view.pop(uid, None)
                            u = await get_user(uid)
                            bal = u["balance"] if u else 0
                            min_wd = await get_min_withdraw()
                            text = (
                                f"🎉 <b>Все задания выполнены, {nick}!</b>\n\n"
                                f"⭐ Начислено: <b>+{fmt_money(sm)}⭐</b>\n"
                                f"💰 Баланс: <b>{fmt_money(bal)}⭐</b>\n"
                                f"📤 Минимум: <b>{fmt_money(min_wd)}⭐</b>\n\n"
                                f"<i>Загляни позже — будут новые!</i>"
                            )
                            try:
                                await bot.send_message(uid, text, reply_markup=get_menu(uid))
                            except Exception:
                                pass
                        else:
                            ok, _ = await send_existing_sponsors_message(uid, uid, bonus_header=f"✅ <b>Засчитано: +{fmt_money(sm)}⭐</b>\n\n")
                            if not ok:
                                _last_sponsors_view.pop(uid, None)
                except Exception as e:
                    logging.error(f"auto check {uid}: {e}")
                finally:
                    _auto_checking.discard(uid)
                await asyncio.sleep(0.25)
        except Exception as e:
            logging.error(f"auto_check_worker: {e}")
        await asyncio.sleep(AUTO_CHECK_FAST_INTERVAL)


# ========== ДИАГНОСТИКА ==========
async def diagnose_service(name, key, checker):
    if not key:
        return f"❌ <b>{name}</b> — ключ не задан"
    t0 = time.time()
    try:
        res = await checker()
        dt = int((time.time() - t0) * 1000)
        if res.get("error"):
            return f"❌ <b>{name}</b> — {res['error']} ({dt}мс)"
        cnt, extra = res.get("count", 0), res.get("extra", "")
        if cnt > 0:
            return f"✅ <b>{name}</b> — {cnt} заданий ({dt}мс){extra}"
        return f"🟡 <b>{name}</b> — 0 заданий ({dt}мс){extra}"
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return f"❌ <b>{name}</b> — {escape_html(str(e)[:100])} ({dt}мс)"


async def _diag_piarflow():
    s = await http()
    async with s.post(
        "https://piarflow.com/v1/sponsors",
        json={"user_id": ADMIN_ID, "chat_id": ADMIN_ID, "max_sponsors": 5, "language_code": "ru"},
        headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status == 404:
            return {"count": 0, "extra": " (нет офферов)"}
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("status") == "ok":
            return {"count": len(d.get("sponsors") or [])}
        return {"error": d.get("message", "unknown"), "count": 0}


async def _diag_mgram():
    if not MGRAM_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        "https://api.mgram.biz/tasks",
        json={"user_id": ADMIN_ID, "limit": 3},
        headers={"Authorization": f"Bearer {MGRAM_API_KEY}", "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        data = d.get("data") or {}
        if data.get("skip"):
            return {"count": 0, "extra": " (всё выполнено)"}
        return {"count": len([t for t in data.get("tasks") or [] if t.get("status") != "completed"])}


async def _diag_flowly():
    if not FLOWLY_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    try:
        s = await http()
        async with s.post(
            "https://flowly.guru/api/v1/offers",
            headers={"Auth": FLOWLY_API_KEY, "Content-Type": "application/json"},
            json={"tg_user_id": ADMIN_ID, "lang": "ru", "is_premium": False},
            timeout=aiohttp.ClientTimeout(total=FLOWLY_TIMEOUT)
        ) as r:
            if r.status != 200:
                return {"error": f"HTTP {r.status}", "count": 0}
            d = await r.json()
            offers = [o for o in d.get("offers") or [] if not o.get("subscribed")]
            if not offers:
                return {"count": 0, "extra": " (всё выполнено)"}
            return {"count": len(offers)}
    except asyncio.TimeoutError:
        return {"error": "timeout", "count": 0}


async def _diag_flyer():
    if not flyer:
        return {"error": "flyerapi не подключён", "count": 0}
    tasks = await flyer.get_tasks(user_id=ADMIN_ID, language_code="ru", limit=5)
    if not tasks:
        return {"count": 0}
    return {"count": len([t for t in tasks if t.get("status") in INCOMPLETE_STATUSES])}


async def _diag_trafsly():
    if not TRAFSLY_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        "https://api.trafsly.com/api/v1/get-sponsors",
        json={"user_id": ADMIN_ID, "language_code": "ru"},
        headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        return {"count": len(d.get("sponsors") or [])}


async def _diag_botohub():
    if not BOTOHUB_API_KEY:
        return {"error": "ключ не задан", "count": 0}
    s = await http()
    async with s.post(
        "https://botohub.me/get-tasks",
        json={"chat_id": ADMIN_ID},
        headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}
    ) as r:
        body = await r.text()
        if r.status != 200:
            return {"error": f"HTTP {r.status}", "count": 0}
        d = json.loads(body)
        if d.get("skip") or d.get("completed"):
            return {"count": 0, "extra": " (всё выполнено)"}
        items = _extract_botohub_items(d)
        return {"count": len(items)}


async def run_diagnostics():
    enabled = await get_enabled_services()
    checks = []
    mapping = [
        ("Piarflow", "piarflow", PIARFLOW_API_KEY, _diag_piarflow),
        ("Flowly", "flowly", FLOWLY_API_KEY, _diag_flowly),
        ("Flyer", "flyer", FLYER_API_KEY, _diag_flyer),
        ("MGRAM", "mgram", MGRAM_API_KEY, _diag_mgram),
        ("Trafsly", "trafsly", TRAFSLY_API_KEY, _diag_trafsly),
        ("Botohub", "botohub", BOTOHUB_API_KEY, _diag_botohub),
    ]
    for name, key, api, fn in mapping:
        if key not in enabled:
            checks.append(f"⏸ <b>{name}</b> — выключен")
        else:
            checks.append(await diagnose_service(name, api, fn))
    return f"🔎 <b>Диагностика</b>\n🆔 <code>{ADMIN_ID}</code>\n\n" + "\n".join(checks)


async def get_recent_errors(hours=3):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch(
            "SELECT service, error_type, message, created_at FROM error_log WHERE created_at > $1 ORDER BY created_at DESC LIMIT 30",
            time.time() - hours * 3600
        )


async def get_top_by_balance(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch(
            "SELECT user_id, username, balance FROM users WHERE blocked = FALSE ORDER BY balance DESC LIMIT $1",
            limit
        )


async def get_top_by_refs(limit=15):
    pool = await get_pool()
    async with pool.acquire() as db:
        return await db.fetch(
            """SELECT u.user_id, u.username,
               (SELECT COUNT(*) FROM users r WHERE r.referrer_id = u.user_id) AS refs_count
               FROM users u WHERE u.blocked = FALSE ORDER BY refs_count DESC, u.user_id ASC LIMIT $1""",
            limit
        )


# ========== КЛАВИАТУРЫ ==========
def user_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⭐ Получить"), KeyboardButton(text="💰 Мои звёзды")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🎁 Бонус"), KeyboardButton(text="🎟 Промокод")],
        ],
        resize_keyboard=True,
        is_persistent=True
    )


def admin_reply_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⭐ Получить"), KeyboardButton(text="💰 Мои звёзды")],
            [KeyboardButton(text="💸 Вывести"), KeyboardButton(text="👥 Рефералы")],
            [KeyboardButton(text="🎁 Бонус"), KeyboardButton(text="🛠 Админка")],
        ],
        resize_keyboard=True,
        is_persistent=True
    )


def get_menu(user_id):
    return admin_reply_menu() if user_id == ADMIN_ID else user_menu()


def sponsors_kb(links):
    kb = InlineKeyboardBuilder()
    for i, link in enumerate(links, 1):
        kb.button(text=f"🔗 {i}", url=link)
    kb.button(text="✅ Я подписался — проверить", callback_data="check_subs")
    kb.button(text="🔄 Обновить список", callback_data="rebuild_subs")
    rows = [2] * (len(links) // 2)
    if len(links) % 2:
        rows.append(1)
    rows.append(2)
    kb.adjust(*rows)
    return kb.as_markup()


def promo_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="promo_cancel")
    return kb.as_markup()


def admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Награда", callback_data="adm_reward")
    kb.button(text="👥 Макс спонсоров", callback_data="adm_max")
    kb.button(text="💸 Заявки", callback_data="adm_withdraws")
    kb.button(text="📊 Статистика", callback_data="adm_stats")
    kb.button(text="🏆 Топ игроков", callback_data="adm_top")
    kb.button(text="🔎 Найти юзера", callback_data="adm_find")
    kb.button(text="💎 Начислить", callback_data="adm_give")
    kb.button(text="🎁 Создать промокод", callback_data="adm_promo_create")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast")
    kb.button(text="💸 Мин. вывод", callback_data="adm_minw")
    kb.button(text="👥 Реф-бонус", callback_data="adm_refb")
    kb.button(text="📊 Диагностика", callback_data="adm_diag")
    kb.button(text="🔌 Сервисы", callback_data="adm_services")
    kb.adjust(3, 3, 3, 3, 1)
    return kb.as_markup()


def services_menu_kb(enabled_map):
    kb = InlineKeyboardBuilder()
    for s in SERVICES_ALL:
        emoji = "✅" if enabled_map.get(s) else "❌"
        kb.button(text=f"{emoji} {s}", callback_data=f"svc_toggle_{s}")
    kb.button(text="⬅️ Назад", callback_data="svc_back")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def diag_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="🔍 Проверить сервисы", callback_data="diag_services")
    kb.button(text="⚠️ Ошибки за 3ч", callback_data="diag_errors")
    kb.button(text="⬅️ Назад", callback_data="diag_back")
    kb.adjust(2, 1)
    return kb.as_markup()


def top_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 По звёздам", callback_data="top_balance")
    kb.button(text="👥 По рефералам", callback_data="top_refs")
    kb.button(text="⬅️ Назад", callback_data="top_back")
    kb.adjust(2, 1)
    return kb.as_markup()


def wd_kb(wid):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"wd_done_{wid}")
    kb.button(text="❌ Отклонить", callback_data=f"wd_decl_{wid}")
    kb.adjust(2)
    return kb.as_markup()


# ========== ВЫДАЧА СПОНСОРОВ ==========
async def send_existing_sponsors_message(uid: int, chat_id: int, delete_msg_id=None, bonus_header: str = ""):
    all_links = await pending_links(uid, limit=await get_max_sponsors())
    if not all_links:
        return False, "😕 <b>Заданий пока нет.</b>\n\nЗагляни через 15-20 минут — мы добавляем новых спонсоров ⭐"

    reward = await get_reward()
    total_reward = reward * len(all_links)
    min_wd = await get_min_withdraw()
    nick = pick_nickname()

    text = (
        f"{bonus_header}"
        f"⭐ <b>{nick.capitalize()}, тебе доступно {len(all_links)} заданий!</b>\n\n"
        f"💎 Шанс получить <b>{fmt_money(total_reward)}⭐</b> за минуту!\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⭐ За спонсора: <b>{fmt_money(reward)}⭐</b>\n"
        f"⚡ Награда сразу (автопроверка включена)\n"
        f"📤 Минимум: <b>{fmt_money(min_wd)}⭐</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"1️⃣ Жми на кнопки → подпишись\n"
        f"2️⃣ Подожди пару секунд или жми «✅ Я подписался»\n"
        f"3️⃣ Получи ⭐ на баланс 🎉\n\n"
        f"<i>💡 Подписывайся по очереди — список обновится сам 👇</i>"
    )
    if delete_msg_id:
        try:
            await bot.delete_message(chat_id, delete_msg_id)
        except Exception:
            pass
    try:
        sent = await bot.send_message(chat_id, text, reply_markup=sponsors_kb(all_links), disable_web_page_preview=True)
        await set_sponsors_msg_id(uid, sent.message_id)
        _last_sponsors_view[uid] = time.time()
    except Exception as e:
        await log_error("bot", "send_sponsors", str(e))
        return False, "❌ Ошибка отправки."
    return True, None


async def issue_sponsors_message(user, chat_id, delete_msg_id=None, clean=True):
    uid = user.id
    old_mid = await get_sponsors_msg_id(uid)
    if old_mid and old_mid != delete_msg_id:
        try:
            await bot.delete_message(chat_id, old_mid)
        except Exception:
            pass
        await clear_sponsors_msg_id(uid)

    if clean:
        await clear_pending_tasks(uid)
    await collect_sponsors(user)
    return await send_existing_sponsors_message(uid, chat_id, delete_msg_id=delete_msg_id)


# ========== ХЭНДЛЕРЫ ==========
@dp.message(CommandStart())
async def start_cmd(msg: types.Message):
    args = msg.text.split()
    ref_id = None
    if len(args) > 1 and args[1].startswith("ref"):
        try:
            ref_id = int(args[1].replace("ref", ""))
        except Exception:
            pass
    un = msg.from_user.username or ""
    is_new = await register_user(msg.from_user.id, ref_id, un)
    u_check = await get_user(msg.from_user.id)
    if u_check and u_check["blocked"]:
        await msg.answer("🚫 <b>Ты заблокирован.</b>")
        return
    if is_new and ref_id:
        await add_lazy_watch(msg.from_user.id, ref_id)
    nick = pick_nickname()
    try:
        loading = await msg.answer(f"⏳ <b>Собираю спонсоров, {nick}…</b>")
    except Exception:
        loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if is_new:
        try:
            services_map = await pending_by_service(msg.from_user.id)
            await notify_admin_new_user(
                msg.from_user.id,
                f"@{un}" if un else "",
                msg.from_user.full_name or "",
                sum(services_map.values()),
                services_map,
                ref_id
            )
        except Exception as e:
            logging.error(f"notify: {e}")
    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))


@dp.message(Command("admin"))
async def cmd_admin(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(F.text == "🛠 Админка")
async def msg_admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())


@dp.message(F.text == "⭐ Получить")
async def msg_earn(msg: types.Message):
    await update_username(msg.from_user.id, msg.from_user.username)
    nick = pick_nickname()
    try:
        loading = await msg.answer(f"⏳ <b>Собираю спонсоров, {nick}…</b>")
    except Exception:
        loading = None
    ok, err = await issue_sponsors_message(msg.from_user, msg.chat.id, delete_msg_id=loading.message_id if loading else None)
    if not ok:
        await msg.answer(err, reply_markup=get_menu(msg.from_user.id))


@dp.message(F.text == "💰 Мои звёзды")
async def msg_balance(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    nick = pick_nickname()
    await msg.answer(
        f"💰 <b>Мои звёзды, {nick}</b>\n\n"
        f"⭐ Доступно: <b>{fmt_money(bal)}⭐</b>\n"
        f"📤 Минимум: <b>{fmt_money(min_wd)}⭐</b>",
        reply_markup=get_menu(msg.from_user.id)
    )


@dp.message(F.text == "🎁 Бонус")
async def msg_daily_bonus(msg: types.Message):
    uid = msg.from_user.id
    u = await get_user(uid)
    if not u:
        return
    subs = await count_total_subs(uid)
    if subs < 1:
        await msg.answer(
            "🎁 <b>Ежедневный бонус</b>\n\n"
            "⚠️ Чтобы забирать ежедневный бонус, выполни хотя бы <b>1 задание</b> в разделе «⭐ Получить»!",
            reply_markup=get_menu(uid)
        )
        return

    now = time.time()
    last_at = u["last_daily_at"] or 0
    streak = u["daily_streak"] or 0
    elapsed = now - last_at

    if elapsed < 86400:
        left_sec = int(86400 - elapsed)
        hrs, rem = divmod(left_sec, 3600)
        mins = rem // 60
        await msg.answer(
            f"⏳ <b>Бонус уже получен!</b>\n\n"
            f"🔥 Твоя серия: <b>{streak} дн.</b>\n"
            f"🕒 Следующий бонус через: <b>{hrs}ч {mins}мин</b>",
            reply_markup=get_menu(uid)
        )
        return

    new_streak = (streak + 1) if (last_at > 0 and elapsed < 172800) else 1
    reward_idx = min(new_streak - 1, len(DAILY_REWARDS) - 1)
    bonus = DAILY_REWARDS[reward_idx]

    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute(
            "UPDATE users SET balance = balance + $1, daily_streak = $2, last_daily_at = $3 WHERE user_id = $4",
            bonus, new_streak, now, uid
        )

    nick = pick_nickname()
    next_bonus = DAILY_REWARDS[min(new_streak, len(DAILY_REWARDS) - 1)]
    await msg.answer(
        f"🎁 <b>Ежедневный бонус получен, {nick}!</b>\n\n"
        f"⭐ Начислено: <b>+{fmt_money(bonus)}⭐</b>\n"
        f"🔥 Серия дней подряд: <b>{new_streak}</b>\n"
        f"📅 Завтра тебя ждёт: <b>+{fmt_money(next_bonus)}⭐</b>",
        reply_markup=get_menu(uid)
    )


@dp.message(F.text == "💸 Вывести")
async def msg_withdraw(msg: types.Message):
    u = await get_user(msg.from_user.id)
    bal = u["balance"] if u else 0
    min_wd = await get_min_withdraw()
    if bal < min_wd:
        await msg.answer(
            f"❌ <b>Недостаточно</b>\n\n📤 Минимум: <b>{fmt_money(min_wd)}⭐</b>\n⭐ У тебя: <b>{fmt_money(bal)}⭐</b>",
            reply_markup=get_menu(msg.from_user.id)
        )
        return
    _states[msg.from_user.id] = "await_withdraw_username"
    await msg.answer(
        f"💸 <b>Вывод звёзд</b>\n\n"
        f"⭐ Доступно: <b>{fmt_money(bal)}⭐</b>\n"
        f"📤 Минимум: <b>{fmt_money(min_wd)}⭐</b>\n\n"
        f"Отправь свой <b>@username</b>:\nНапример: <code>@durov</code>",
        reply_markup=get_menu(msg.from_user.id)
    )


@dp.message(F.text == "👥 Рефералы")
async def msg_refs(msg: types.Message):
    uid = msg.from_user.id
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref{uid}"
    pool = await get_pool()
    async with pool.acquire() as db:
        refs_count = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", uid))["c"]
        active_refs = (await db.fetchrow(
            """SELECT COUNT(*) as c FROM users u WHERE u.referrer_id=$1
               AND (SELECT COUNT(*) FROM sponsor_tasks st WHERE st.user_id=u.user_id AND st.status='subscribed') >= $2""",
            uid, REF_ACTIVE_MIN_SUBS
        ))["c"]
        earned = (await db.fetchrow("SELECT COALESCE(referred_earned, 0) as s FROM users WHERE user_id=$1", uid))["s"]

    ref_bonus = await get_ref_bonus()
    nick = pick_nickname()

    milestones_lines = []
    for m_cnt, m_rw in sorted(REF_MILESTONES.items()):
        mark = "✅" if active_refs >= m_cnt else "▫️"
        milestones_lines.append(f"{mark} <b>{m_cnt}</b> активных друзей → <b>+{fmt_money(m_rw)}⭐</b>")

    kb = InlineKeyboardBuilder()
    kb.button(text="📤 Поделиться ссылкой", switch_inline_query=f"\n⭐ Зарабатывай звёзды Telegram бесплатно! Забирай бонус: {ref_link}")
    kb.button(text="🏆 Топ рефоводов", callback_data="public_top_refs")
    kb.adjust(1)

    await msg.answer(
        f"👥 <b>Реферальная программа, {nick}</b>\n\n"
        f"💎 Бонус: <b>{fmt_money(ref_bonus)}⭐</b> за каждую подписку друга!\n\n"
        f"🔗 Твоя ссылка:\n<code>{ref_link}</code>\n\n"
        f"👤 Всего приглашено: <b>{refs_count}</b> (активных: <b>{active_refs}</b>)\n"
        f"⭐ Заработано с друзей: <b>{fmt_money(earned)}⭐</b>\n\n"
        f"🏆 <b>Бонусы за активных друзей</b> (от {REF_ACTIVE_MIN_SUBS} подписок):\n"
        + "\n".join(milestones_lines),
        reply_markup=kb.as_markup()
    )


@dp.callback_query(F.data == "public_top_refs")
async def cb_public_top_refs(cq: types.CallbackQuery):
    await cq.answer()
    rows = await get_top_by_refs(10)
    if not rows:
        await cq.message.answer("📭 Топ пока пуст.")
        return
    lines = ["🏆 <b>Топ-10 рефоводов бота:</b>\n"]
    medals = ["🥇", "🥈", "🥉"]
    for i, r in enumerate(rows, 1):
        prefix = medals[i - 1] if i <= 3 else f"{i}."
        name = f"@{escape_html(r['username'])}" if r["username"] else f"Игрок #{str(r['user_id'])[-4:]}"
        lines.append(f"{prefix} {name} — <b>{r['refs_count'] or 0}</b> друзей")
    await cq.message.answer("\n".join(lines))


@dp.message(F.text.in_({"🎁 Промокод", "🎟 Промокод"}))
async def msg_promo(msg: types.Message):
    _states[msg.from_user.id] = "await_promo"
    await msg.answer("🎟 <b>Активация промокода</b>\n\nОтправь код:", reply_markup=promo_kb())


@dp.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cq: types.CallbackQuery):
    _states.pop(cq.from_user.id, None)
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer("Отменено")
    try:
        await cq.message.edit_text("Отменено.")
    except Exception:
        pass
    await cq.message.answer(f"⭐ Баланс: <b>{fmt_money(bal)}⭐</b>", reply_markup=get_menu(cq.from_user.id))


# ========== РУЧНАЯ ПРОВЕРКА ==========
@dp.callback_query(F.data == "check_subs")
async def cb_check(cq: types.CallbackQuery):
    uid = cq.from_user.id
    if time.time() - _last.get(uid, 0) < CHECK_COOLDOWN:
        await cq.answer("⏱ Подожди пару секунд")
        return
    _last[uid] = time.time()
    _last_sponsors_view[uid] = time.time()
    await cq.answer("Проверяю подписки…")

    cnt, sm, skipped = await check_all_safe(uid)
    left = await pending_count(uid)
    u = await get_user(uid)
    bal = u["balance"] if u else 0
    nick = pick_nickname()

    if cnt > 0:
        try:
            await cq.message.delete()
        except Exception:
            pass
        await clear_sponsors_msg_id(uid)

        if left == 0:
            _last_sponsors_view.pop(uid, None)
            min_wd = await get_min_withdraw()
            await cq.message.answer(
                f"🎉 <b>Все задания выполнены, {nick}!</b>\n\n"
                f"⭐ Начислено: <b>+{fmt_money(sm)}⭐</b>\n"
                f"💰 Баланс: <b>{fmt_money(bal)}⭐</b>\n"
                f"📤 Минимум: <b>{fmt_money(min_wd)}⭐</b>\n\n"
                f"<i>Загляни позже — будут новые!</i>",
                reply_markup=get_menu(uid)
            )
        else:
            await send_existing_sponsors_message(uid, cq.message.chat.id, bonus_header=f"✅ <b>Засчитано: +{fmt_money(sm)}⭐</b>\n\n")
        return

    if skipped > 0:
        try:
            await cq.message.delete()
        except Exception:
            pass
        await clear_sponsors_msg_id(uid)
        ok, err = await send_existing_sponsors_message(uid, cq.message.chat.id, bonus_header=f"⚠️ <b>Часть ссылок устарела ({skipped})</b>\n\n")
        if not ok:
            await cq.message.answer(err, reply_markup=get_menu(uid))
        return

    await cq.message.answer(
        f"❌ <b>Новых подписок пока не найдено, {nick}</b>\n\n"
        f"⚠️ Осталось заданий: <b>{left}</b>\n"
        f"<i>Подпишись на каналы выше и нажми «✅ Я подписался — проверить».</i>"
    )


@dp.callback_query(F.data == "rebuild_subs")
async def cb_rebuild(cq: types.CallbackQuery):
    uid = cq.from_user.id
    await cq.answer("Обновляю список…")
    await check_all_safe(uid)
    try:
        await cq.message.delete()
    except Exception:
        pass
    await clear_sponsors_msg_id(uid)
    ok, err = await issue_sponsors_message(cq.from_user, cq.message.chat.id, clean=True)
    if not ok:
        await cq.message.answer(err, reply_markup=get_menu(uid))


@dp.callback_query(F.data == "balance_inline")
async def cb_balance_inline(cq: types.CallbackQuery):
    u = await get_user(cq.from_user.id)
    bal = u["balance"] if u else 0
    await cq.answer()
    await cq.message.answer(
        f"⭐ Баланс: <b>{fmt_money(bal)}⭐</b>\n📤 Минимум: <b>{fmt_money(await get_min_withdraw())}⭐</b>",
        reply_markup=get_menu(cq.from_user.id)
    )


# ========== КНОПКА «СЕРВИСЫ» ==========
@dp.callback_query(F.data == "adm_services")
async def adm_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    text = "🔌 <b>Управление сервисами</b>\n\nНажми на сервис, чтобы включить/выключить:"
    try:
        await cq.message.edit_text(text, reply_markup=services_menu_kb(enabled_map))
    except Exception:
        await cq.message.answer(text, reply_markup=services_menu_kb(enabled_map))


@dp.callback_query(F.data.startswith("svc_toggle_"))
async def svc_toggle(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    service = cq.data.replace("svc_toggle_", "")
    if service not in SERVICES_ALL:
        await cq.answer("Неизвестный сервис")
        return
    cur = await is_service_enabled(service)
    await set_service_enabled(service, not cur)
    await cq.answer(f"{service}: {'выкл' if cur else 'вкл'}")
    enabled_map = {s: await is_service_enabled(s) for s in SERVICES_ALL}
    text = "🔌 <b>Управление сервисами</b>\n\nНажми на сервис, чтобы включить/выключить:"
    try:
        await cq.message.edit_text(text, reply_markup=services_menu_kb(enabled_map))
    except Exception:
        pass


@dp.callback_query(F.data == "svc_back")
async def svc_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception:
        pass


# ========== ТЕКСТОВЫЙ ХЭНДЛЕР ==========
@dp.message(F.text & ~F.text.startswith("/"))
async def text_handler(msg: types.Message):
    uid = msg.from_user.id
    state = _states.get(uid)

    if state == "await_promo":
        code = msg.text.strip()
        pool = await get_pool()
        async with pool.acquire() as db:
            promo = await db.fetchrow("SELECT * FROM promo_codes WHERE code=$1 AND active=TRUE", code)
            if not promo:
                await msg.answer("❌ Неверный промокод", reply_markup=get_menu(uid))
                _states.pop(uid, None)
                return
            if promo["used_count"] >= promo["max_uses"]:
                await msg.answer("❌ Промокод закончился", reply_markup=get_menu(uid))
                _states.pop(uid, None)
                return
            if await db.fetchrow("SELECT 1 FROM promo_uses WHERE user_id=$1 AND code=$2", uid, code):
                await msg.answer("❌ Ты уже активировал этот код", reply_markup=get_menu(uid))
                _states.pop(uid, None)
                return
            async with db.transaction():
                await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", promo["amount"], uid)
                await db.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE code=$1", code)
                await db.execute("INSERT INTO promo_uses (user_id, code, used_at) VALUES ($1,$2,$3)", uid, code, time.time())
        _states.pop(uid, None)
        await msg.answer(f"✅ +<b>{fmt_money(promo['amount'])}⭐</b>", reply_markup=get_menu(uid))
        return

    if state == "await_withdraw_username":
        uname = msg.text.strip().lstrip("@")
        if not uname or len(uname) < 4:
            await msg.answer("❌ Неверный username", reply_markup=get_menu(uid))
            return
        u = await get_user(uid)
        bal = u["balance"] if u else 0
        min_wd = await get_min_withdraw()
        if bal < min_wd:
            _states.pop(uid, None)
            await msg.answer(f"❌ Нужно {fmt_money(min_wd)}⭐", reply_markup=get_menu(uid))
            return
        amount = bal
        _states.pop(uid, None)
        pool = await get_pool()
        async with pool.acquire() as db:
            async with db.transaction():
                res = await db.execute(
                    "UPDATE users SET balance = balance - $1 WHERE user_id=$2 AND balance >= $1",
                    amount, uid
                )
                if res == "UPDATE 0":
                    await msg.answer("❌ Недостаточно средств")
                    return
                wid = (await db.fetchrow(
                    "INSERT INTO withdrawals (user_id, amount, wallet, status, created_at) VALUES ($1,$2,$3,'pending',$4) RETURNING id",
                    uid, amount, uname, time.time()
                ))["id"]
        await msg.answer(
            f"✅ <b>Заявка #{wid}</b>\n⭐ {fmt_money(amount)}⭐\n👤 @{escape_html(uname)}",
            reply_markup=get_menu(uid)
        )
        try:
            await bot.send_message(
                ADMIN_ID,
                f"💸 <b>Заявка #{wid}</b>\n👤 {await get_user_display(uid)}\n⭐ {fmt_money(amount)}⭐\n📌 @{escape_html(uname)}",
                reply_markup=wd_kb(wid)
            )
        except Exception:
            pass
        return

    if uid != ADMIN_ID:
        return

    if state == "adm_reward":
        try:
            v = float(msg.text.replace(",", "."))
            if v <= 0:
                await msg.answer("❌ > 0")
                return
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("reward", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(v)}⭐", reply_markup=admin_menu())
        return

    if state == "adm_max":
        try:
            v = int(msg.text)
        except Exception:
            await msg.answer("❌ Целое")
            return
        if v > MAX_MAX_SPONSORS or v < 1:
            await msg.answer(f"❌ 1-{MAX_MAX_SPONSORS}")
            return
        await set_setting("max_sponsors", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {v}", reply_markup=admin_menu())
        return

    if state == "adm_minw":
        try:
            v = float(msg.text.replace(",", "."))
            if v <= 0:
                await msg.answer("❌ > 0")
                return
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("min_withdraw", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(v)}⭐", reply_markup=admin_menu())
        return

    if state == "adm_refb":
        try:
            v = float(msg.text.replace(",", "."))
            if v < 0:
                await msg.answer("❌ ≥ 0")
                return
        except Exception:
            await msg.answer("❌ Число")
            return
        await set_setting("ref_bonus", v)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(v)}⭐", reply_markup=admin_menu())
        return

    if state == "adm_broadcast":
        _states.pop(uid, None)
        await msg.answer("📢…")
        pool = await get_pool()
        async with pool.acquire() as db:
            rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
        ok = 0
        fail = 0
        for r in rows:
            try:
                await msg.copy_to(r["user_id"])
                ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05)
        await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu())
        return

    if state == "adm_promo_create":
        parts = [p.strip() for p in msg.text.split("|")]
        if len(parts) != 3:
            await msg.answer("❌ <code>КОД | СУММА | АКТ</code>")
            return
        code, a_s, u_s = parts
        try:
            a = float(a_s.replace(",", "."))
            u = int(u_s)
        except Exception:
            await msg.answer("❌ Числа")
            return
        pool = await get_pool()
        async with pool.acquire() as db:
            try:
                await db.execute(
                    "INSERT INTO promo_codes (code, amount, max_uses, created_at) VALUES ($1,$2,$3,$4)",
                    code, a, u, time.time()
                )
            except Exception:
                await msg.answer("❌ Уже есть")
                return
        _states.pop(uid, None)
        await msg.answer(f"✅ <code>{code}</code>: {fmt_money(a)}⭐ × {u}", reply_markup=admin_menu())
        return

    if state == "adm_find":
        query = msg.text.strip()
        _states.pop(uid, None)
        target = await find_user(query)
        if not target:
            await msg.answer("❌ Не найден", reply_markup=admin_menu())
            return
        u = await get_user(target)
        pool = await get_pool()
        async with pool.acquire() as db:
            refs = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", target))["c"]
            subs = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", target))["c"]
        await msg.answer(
            f"🔎 {user_display(target, u['username'])}\n⭐ {fmt_money(u['balance'])}⭐\n📊 {subs}\n👥 {refs}",
            reply_markup=admin_menu()
        )
        return

    if state == "adm_give":
        parts = msg.text.strip().split()
        if len(parts) != 2:
            await msg.answer("❌ <code>ID СУММА</code>")
            return
        target = await find_user(parts[0])
        if not target:
            await msg.answer("❌ Не найден")
            return
        try:
            a = float(parts[1].replace(",", "."))
        except Exception:
            await msg.answer("❌ Сумма")
            return
        await add_balance(target, a)
        _states.pop(uid, None)
        await msg.answer(f"✅ {fmt_money(a)}⭐ → {await get_user_display(target)}", reply_markup=admin_menu())
        try:
            await bot.send_message(target, f"🎁 +<b>{fmt_money(a)}⭐</b>")
        except Exception:
            pass
        return


# ========== КОМАНДЫ ==========
@dp.message(Command("ban"))
async def cmd_ban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        q = msg.text.split()[1]
    except Exception:
        await msg.answer("Формат: /ban ID")
        return
    t = await find_user(q)
    if not t:
        await msg.answer("❌")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=TRUE WHERE user_id=$1", t)
    await msg.answer(f"🚫 {await get_user_display(t)}", reply_markup=admin_menu())


@dp.message(Command("unban"))
async def cmd_unban(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        q = msg.text.split()[1]
    except Exception:
        await msg.answer("Формат: /unban ID")
        return
    t = await find_user(q)
    if not t:
        await msg.answer("❌")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET blocked=FALSE WHERE user_id=$1", t)
    await msg.answer(f"✅ {await get_user_display(t)}", reply_markup=admin_menu())


@dp.message(Command("give"))
async def cmd_give(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        p = msg.text.split()
        q = p[1]
        a = float(p[2].replace(",", "."))
    except Exception:
        await msg.answer("Формат: /give ID СУММА")
        return
    t = await find_user(q)
    if not t:
        await msg.answer("❌")
        return
    await add_balance(t, a)
    await msg.answer(f"✅ {fmt_money(a)}⭐ → {await get_user_display(t)}", reply_markup=admin_menu())
    if a > 0:
        try:
            await bot.send_message(t, f"🎁 +<b>{fmt_money(a)}⭐</b>")
        except Exception:
            pass


@dp.message(Command("errors"))
async def cmd_errors(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    try:
        rows = await get_recent_errors(hours=3)
    except Exception as e:
        await msg.answer(f"❌ {e}")
        return
    if not rows:
        await msg.answer("✅ Ошибок нет")
        return
    text = "⚠️ <b>Ошибки:</b>\n\n"
    for r in rows[:15]:
        ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
        text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    await msg.answer(text[:4000])


# ========== МЕДИА ==========
@dp.message((F.from_user.id == ADMIN_ID) & (F.photo | F.video | F.document | F.animation | F.sticker | F.voice))
async def media_broadcast_handler(msg: types.Message):
    uid = msg.from_user.id
    if _states.get(uid) != "adm_broadcast":
        return
    _states.pop(uid, None)
    await msg.answer("📢…")
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("SELECT user_id FROM users WHERE blocked=FALSE")
    ok = 0
    fail = 0
    for r in rows:
        try:
            await msg.copy_to(r["user_id"])
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await msg.answer(f"✅ {ok}\n❌ {fail}", reply_markup=admin_menu())


# ========== АДМИН CALLBACKS ==========
@dp.callback_query(F.data == "adm_reward")
async def adm_reward(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_reward"
    await cq.answer()
    try:
        await cq.message.edit_text(f"⭐ Текущая: <b>{fmt_money(await get_reward())}⭐</b>\n\nНовое:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_max")
async def adm_max(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_max"
    await cq.answer()
    try:
        await cq.message.edit_text(f"👥 Текущий: <b>{await get_max_sponsors()}</b>\n\nЧисло 1-{MAX_MAX_SPONSORS}:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_minw")
async def adm_minw(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_minw"
    await cq.answer()
    try:
        await cq.message.edit_text(f"💸 Мин: <b>{fmt_money(await get_min_withdraw())}⭐</b>\n\nНовое:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_refb")
async def adm_refb(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_refb"
    await cq.answer()
    try:
        await cq.message.edit_text(f"👥 Реф: <b>{fmt_money(await get_ref_bonus())}⭐</b>\n\nНовое:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_stats")
async def adm_stats(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        total = (await db.fetchrow("SELECT COUNT(*) as c FROM users"))["c"]
        blocked = (await db.fetchrow("SELECT COUNT(*) as c FROM users WHERE blocked=TRUE"))["c"]
        balances = (await db.fetchrow("SELECT COALESCE(SUM(balance),0) as s FROM users"))["s"]
        subs = (await db.fetchrow("SELECT COUNT(*) as c FROM sponsor_tasks WHERE status='subscribed'"))["c"]
        paid = (await db.fetchrow("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='done'"))["s"]
        pend = (await db.fetchrow("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'"))["c"]
        errs = (await db.fetchrow("SELECT COUNT(*) as c FROM error_log WHERE created_at > $1", time.time() - 86400))["c"]
    await cq.answer()
    try:
        await cq.message.edit_text(
            f"📊 <b>Статистика</b>\n\n👥 {total} (🚫 {blocked})\n⭐ {fmt_money(balances)}\n✅ {subs}\n💸 {fmt_money(paid)}\n📤 {pend}\n⚠️ {errs}",
            reply_markup=admin_menu()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "adm_find")
async def adm_find(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_find"
    await cq.answer()
    try:
        await cq.message.edit_text("🔎 <b>Найти юзера</b>\n\nID или @username:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_give")
async def adm_give(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_give"
    await cq.answer()
    try:
        await cq.message.edit_text("💎 <b>Начислить</b>\n\n<code>ID СУММА</code>")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_broadcast")
async def adm_broadcast(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_broadcast"
    await cq.answer()
    try:
        await cq.message.edit_text("📢 Отправь сообщение:")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_promo_create")
async def adm_promo_create(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    _states[cq.from_user.id] = "adm_promo_create"
    await cq.answer()
    try:
        await cq.message.edit_text("Формат: <code>КОД | СУММА | АКТ</code>")
    except Exception:
        pass


@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT w.id, w.user_id, w.amount, w.wallet, w.status, u.username FROM withdrawals w LEFT JOIN users u ON u.user_id = w.user_id WHERE w.status='pending' ORDER BY w.id DESC LIMIT 20"
        )
    if not rows:
        await cq.answer("Нет заявок", show_alert=True)
        return
    text = "💸 <b>Заявки:</b>\n\n"
    for r in rows:
        text += f"#{r['id']} · {user_display(r['user_id'], r['username'])} · <b>{fmt_money(r['amount'])}⭐</b> → @{escape_html(r['wallet'])}\n"
    await cq.answer()
    try:
        await cq.message.edit_text(text[:4000], reply_markup=admin_menu(), disable_web_page_preview=True)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_done_"))
async def wd_done(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    try:
        wid = int(cq.data.replace("wd_done_", ""))
    except Exception:
        await cq.answer("Ошибка")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE withdrawals SET status='done' WHERE id=$1", wid)
        row = await db.fetchrow("SELECT user_id, amount FROM withdrawals WHERE id=$1", wid)
    if row:
        try:
            await bot.send_message(row["user_id"], f"✅ <b>Заявка #{wid} оплачена</b>\n⭐ {fmt_money(row['amount'])}⭐")
        except Exception:
            pass
    await cq.answer("✅")
    try:
        await cq.message.edit_text(f"✅ #{wid}")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("wd_decl_"))
async def wd_decl(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    try:
        wid = int(cq.data.replace("wd_decl_", ""))
    except Exception:
        await cq.answer("Ошибка")
        return
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT user_id, amount, status FROM withdrawals WHERE id=$1", wid)
        if row and row["status"] == "pending":
            await db.execute("UPDATE withdrawals SET status='declined' WHERE id=$1", wid)
            await db.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", row["amount"], row["user_id"])
            try:
                await bot.send_message(row["user_id"], f"❌ <b>Заявка #{wid} отклонена</b>\n⭐ {fmt_money(row['amount'])}⭐ возвращены")
            except Exception:
                pass
    await cq.answer("❌")
    try:
        await cq.message.edit_text(f"❌ #{wid}")
    except Exception:
        pass


# ========== ДИАГНОСТИКА UI ==========
@dp.callback_query(F.data == "adm_diag")
async def adm_diag(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("📊 <b>Диагностика</b>", reply_markup=diag_kb())
    except Exception:
        await cq.message.answer("📊 <b>Диагностика</b>", reply_markup=diag_kb())


@dp.callback_query(F.data == "diag_back")
async def cb_diag_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "diag_services")
async def cb_diag_services(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer("Проверяю…")
    try:
        await cq.message.edit_text("🔎 <b>Проверяю…</b>")
    except Exception:
        pass
    try:
        report = await run_diagnostics()
    except Exception as e:
        report = f"❌ {e}"
    if len(report) > 4000:
        report = report[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄", callback_data="diag_services")
    kb.button(text="⚠️", callback_data="diag_errors")
    kb.button(text="⬅️", callback_data="diag_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(report, reply_markup=kb.as_markup())
    except Exception:
        try:
            await cq.message.answer(report, reply_markup=kb.as_markup())
        except Exception:
            pass


@dp.callback_query(F.data == "diag_errors")
async def cb_diag_errors(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        rows = await get_recent_errors(hours=3)
    except Exception as e:
        await cq.answer(f"Ошибка БД: {e}", show_alert=True)
        return
    if not rows:
        text = "✅ <b>Ошибок нет</b>"
    else:
        text = "⚠️ <b>Ошибки:</b>\n\n"
        for r in rows[:15]:
            ts = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
            text += f"<b>{escape_html(r['service'])}</b> · {escape_html(r['error_type'])}\n<code>{escape_html((r['message'] or '')[:100])}</code>\n<i>{ts}</i>\n\n"
    if len(text) > 4000:
        text = text[:4000] + "…"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄", callback_data="diag_errors")
    kb.button(text="🔍", callback_data="diag_services")
    kb.button(text="⬅️", callback_data="diag_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(text, reply_markup=kb.as_markup())
    except Exception:
        try:
            await cq.message.answer(text, reply_markup=kb.as_markup())
        except Exception:
            pass


# ========== ТОП ==========
@dp.callback_query(F.data == "adm_top")
async def adm_top(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("🏆 <b>Топ</b>", reply_markup=top_kb())
    except Exception:
        await cq.message.answer("🏆 <b>Топ</b>", reply_markup=top_kb())


@dp.callback_query(F.data == "top_back")
async def cb_top_back(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    try:
        await cq.message.edit_text("🛠 <b>Админ-панель</b>", reply_markup=admin_menu())
    except Exception:
        pass


@dp.callback_query(F.data == "top_balance")
async def cb_top_balance(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    rows = await get_top_by_balance(15)
    text = (
        "🏆 <b>Топ по звёздам</b>\n\n"
        + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{fmt_money(r['balance'])}⭐</b>\n" for i, r in enumerate(rows, 1))
        if rows
        else "📭 Пусто"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="👥", callback_data="top_refs")
    kb.button(text="🔄", callback_data="top_balance")
    kb.button(text="⬅️", callback_data="top_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception:
        pass


@dp.callback_query(F.data == "top_refs")
async def cb_top_refs(cq: types.CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    await cq.answer()
    rows = await get_top_by_refs(15)
    text = (
        "🏆 <b>Топ по рефералам</b>\n\n"
        + "".join(f"{i}. {user_display(r['user_id'], r['username'])} — <b>{r['refs_count'] or 0}</b>\n" for i, r in enumerate(rows, 1))
        if rows
        else "📭 Пусто"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐", callback_data="top_balance")
    kb.button(text="🔄", callback_data="top_refs")
    kb.button(text="⬅️", callback_data="top_back")
    kb.adjust(2, 1)
    try:
        await cq.message.edit_text(text[:4000], reply_markup=kb.as_markup())
    except Exception:
        pass


# ========== WEB + MAIN ==========
async def health(_):
    return web.Response(text="ok")


async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")


async def on_shutdown(*args, **kwargs):
    global _http
    if _http and not _http.closed:
        await _http.close()


async def main():
    await init_db()
    await cleanup_expired_tasks()
    await start_web()
    asyncio.create_task(autocheck_worker())
    asyncio.create_task(lazy_worker())
    asyncio.create_task(auto_check_worker())
    dp.shutdown.register(on_shutdown)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
