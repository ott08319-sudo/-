import os
import time
import asyncio
import logging
import json
import random
import aiohttp
import asyncpg
from aiohttp import web
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties

try:
    from flyerapi import Flyer, APIError as FlyerAPIError
    FLYER_AVAILABLE = True
except ImportError:
    FLYER_AVAILABLE = False
    FlyerAPIError = Exception

logging.basicConfig(level=logging.INFO)

# ========== ENV ==========
BOT_TOKEN        = os.getenv("BOT_TOKEN")
ADMIN_ID         = int(os.getenv("ADMIN_ID", "0") or "0")
PORT             = int(os.getenv("PORT", "10000"))
DATABASE_URL     = os.getenv("DATABASE_URL", "")

PIARFLOW_API_KEY = os.getenv("PIARFLOW_API_KEY", "")
TGRASS_API_KEY   = os.getenv("TGRASS_API_KEY", "")
TRAFSLY_API_KEY  = os.getenv("TRAFSLY_API_KEY", "")
AXIONNA_API_KEY  = os.getenv("AXIONNA_API_KEY", "")
FLYER_API_KEY    = os.getenv("FLYER_API_KEY", "")
BOTOHUB_API_KEY  = os.getenv("BOTOHUB_API_KEY", "")

flyer = Flyer(FLYER_API_KEY) if (FLYER_AVAILABLE and FLYER_API_KEY) else None

DEFAULT_MAX_SPONSORS = 14
MAX_LIMIT            = 30
CACHE_TTL            = 300
LAZY_INTERVAL        = 300
MANUAL_COOLDOWN      = 60

DEFAULT_SERVICE_ORDER = "piarflow,tgrass,trafsly,axionna,flyer,botohub"
ALL_SERVICES = ["piarflow", "tgrass", "trafsly", "axionna", "flyer", "botohub"]

INCOMPLETE_STATUSES = ('incomplete', 'abort')

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

_pool = None
_http = None
_manual_last = {}


async def http():
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    return _http


def clean_db_url(url):
    if not url: return url
    url = url.replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("sslmode", None)
    qs.pop("channel_binding", None)
    if parsed.hostname and ("neon.tech" in parsed.hostname or "bothost" in parsed.hostname):
        qs["ssl"] = ["true"]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(clean_db_url(DATABASE_URL), min_size=1, max_size=10)
    return _pool


# ========== FSM ==========
class AdminState(StatesGroup):
    waiting_for_max_sponsors = State()
    waiting_for_sponsor_reward = State()
    waiting_for_broadcast = State()


class WithdrawState(StatesGroup):
    waiting_for_username = State()


# ========== БД ==========
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
            task_type TEXT,
            need_check INTEGER DEFAULT 1,
            created_at DOUBLE PRECISION,
            UNIQUE(user_id, service, assignment_id)
        )""")
        await db.execute("ALTER TABLE sponsor_tasks ADD COLUMN IF NOT EXISTS task_type TEXT")
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
        await db.execute("""
        CREATE TABLE IF NOT EXISTS lazy_watch (
            user_id BIGINT PRIMARY KEY,
            created_at DOUBLE PRECISION,
            last_notified DOUBLE PRECISION DEFAULT 0,
            admin_notified BOOLEAN DEFAULT FALSE
        )""")
        await db.execute("ALTER TABLE lazy_watch ADD COLUMN IF NOT EXISTS admin_notified BOOLEAN DEFAULT FALSE")
        await db.execute("INSERT INTO settings (key, value) VALUES ('max_sponsors', '14') ON CONFLICT (key) DO NOTHING")
        await db.execute("INSERT INTO settings (key, value) VALUES ('sponsor_reward', '0.5') ON CONFLICT (key) DO NOTHING")
        await db.execute("INSERT INTO settings (key, value) VALUES ('service_order', $1) ON CONFLICT (key) DO NOTHING", DEFAULT_SERVICE_ORDER)
    logging.info("✅ БД инициализирована")


async def get_setting(key, default=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT value FROM settings WHERE key=$1", key)
        return row["value"] if row else default


async def set_setting(key, value):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2", key, str(value))


async def get_max_sponsors():
    return int(await get_setting("max_sponsors", DEFAULT_MAX_SPONSORS))


async def get_sponsor_reward():
    return float(await get_setting("sponsor_reward", "0.5"))


async def get_service_order():
    raw = await get_setting("service_order", DEFAULT_SERVICE_ORDER)
    order = [s.strip() for s in str(raw).split(",") if s.strip()]
    for s in ALL_SERVICES:
        if s not in order:
            order.append(s)
    return order


async def log_error(user_id, service, error_type, message):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            recent = await db.fetchrow("""
                SELECT id FROM error_log
                WHERE service=$1 AND error_type=$2 AND created_at > $3 LIMIT 1
            """, service, error_type, time.time() - 300)
            if recent:
                return
            await db.execute(
                "INSERT INTO error_log (user_id, service, error_type, message, created_at) VALUES ($1,$2,$3,$4,$5)",
                user_id, service, error_type, message[:500], time.time()
            )
    except Exception as e:
        logging.error(f"log_error: {e}")


async def register_user(user_id, username, referrer_id=None):
    pool = await get_pool()
    async with pool.acquire() as db:
        exists = await db.fetchrow("SELECT user_id FROM users WHERE user_id = $1", user_id)
        if exists:
            return False
        if referrer_id == user_id:
            referrer_id = None
        if referrer_id:
            r = await db.fetchrow("SELECT user_id FROM users WHERE user_id=$1", referrer_id)
            if not r:
                referrer_id = None
        await db.execute(
            "INSERT INTO users (user_id, username, referrer_id, created_at) VALUES ($1, $2, $3, $4)",
            user_id, username, referrer_id, time.time()
        )
        return True


async def get_user(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return dict(row) if row else None


async def update_balance(user_id, amount):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            UPDATE users 
            SET balance = balance + $1, 
                total_earned = total_earned + CASE WHEN $1 > 0 THEN $1 ELSE 0 END 
            WHERE user_id = $2
        """, amount, user_id)


async def log_sponsor(user_id, service, link, status):
    try:
        pool = await get_pool()
        async with pool.acquire() as db:
            await db.execute(
                "INSERT INTO sponsor_log (user_id, service, link, status, created_at) VALUES ($1, $2, $3, $4, $5)",
                user_id, service, link, status, time.time()
            )
    except Exception as e:
        logging.error(f"log_sponsor: {e}")


async def log_sponsor_status(user_id, status, reason=""):
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


async def clear_pending_tasks(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'", user_id)
        await db.execute("DELETE FROM sponsor_cache WHERE cache_key=$1", f"all_sponsors_{user_id}")


async def pending_count(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status!='subscribed'",
            user_id
        )
        return row["c"]


async def count_total_subs(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        row = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'",
            user_id
        )
        return row["c"]


async def pending_by_service(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT service, COUNT(*) as c FROM sponsor_tasks
            WHERE user_id=$1 AND status!='subscribed'
            GROUP BY service
        """, user_id)
        return {r["service"]: r["c"] for r in rows}


async def pending_links_sorted(user_id):
    order = await get_service_order()
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch("""
            SELECT service, link FROM sponsor_tasks
            WHERE user_id=$1 AND status!='subscribed'
        """, user_id)
    items = [(r["service"], r["link"]) for r in rows]
    def sort_key(item):
        s = item[0]
        return order.index(s) if s in order else 999
    items.sort(key=sort_key)
    return [link for _, link in items]


# ========== LAZY WATCH ==========
async def add_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("""
            INSERT INTO lazy_watch (user_id, created_at, last_notified, admin_notified)
            VALUES ($1,$2,0,FALSE) ON CONFLICT (user_id) DO NOTHING
        """, user_id, time.time())


async def remove_lazy_watch(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("DELETE FROM lazy_watch WHERE user_id=$1", user_id)


# ========== УВЕДОМЛЕНИЯ АДМИНУ ==========
async def notify_admin(text):
    if not ADMIN_ID: return
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception:
        pass


async def notify_admin_new_user(user_id, username, sponsors_count, services_map):
    if not ADMIN_ID: return
    try:
        ts = time.strftime("%d.%m %H:%M", time.localtime())
        if services_map:
            svc_lines = "\n".join(f"  • <b>{s}</b> — {c}" for s, c in services_map.items())
        else:
            svc_lines = "  • нет доступных сервисов"

        text = (
            f"👤 <b>Новый пользователь!</b>\n\n"
            f"🆔 <code>{user_id}</code>\n"
            f"🔗 {username or '—'}\n\n"
            f"📦 Выдано спонсоров: <b>{sponsors_count}</b>\n"
            f"🎯 По сервисам:\n{svc_lines}\n\n"
            f"🕐 {ts}"
        )
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"notify_admin_new_user: {e}")


async def notify_admin_new_subs(user_id, subs_list):
    if not ADMIN_ID or not subs_list:
        return
    try:
        try:
            tg_user = await bot.get_chat(user_id)
            username = f"@{tg_user.username}" if tg_user.username else (tg_user.first_name or "без имени")
        except Exception:
            username = "—"

        by_service = {}
        for service, aid, count in subs_list:
            by_service.setdefault(service, 0)
            by_service[service] += count

        lines = [f"  • <b>{s}</b> — {c}" for s, c in by_service.items()]
        ts = time.strftime("%d.%m %H:%M", time.localtime())

        text = (
            f"🆕 <b>Новые подписки!</b>\n\n"
            f"👤 <b>{username}</b>\n"
            f"🆔 <code>{user_id}</code>\n\n"
            f"📦 <b>{len(subs_list)}</b> заданий выполнено:\n"
            + "\n".join(lines) +
            f"\n\n🕐 {ts}"
        )
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"notify_admin_new_subs: {e}")


# ========== ЛЕНИВЫЕ ==========
LAZY_USER_MSGS = [
    "😡 <b>Эй, ты чего?!</b>\n\n"
    "Ты пришёл заработать звёзды, а сам ничего не сделал?\n"
    "Так дело не пойдёт.\n\n"
    "🔥 Жми на спонсоров ниже, подпишись на <b>ВСЕХ</b> и нажми "
    "«✅ Проверить подписки». Это 2 минуты.\n\n"
    "⚠️ <i>Пока не подпишешься — ты НЕ получишь доступ к боту.</i>",

    "😤 <b>Слушай, так не работает!</b>\n\n"
    "Ты нажал /start и думаешь, что звёзды упадут сами?\n"
    "Нет, дружок. Так не бывает.\n\n"
    "💎 Подпишись на спонсоров, проверь подписки — "
    "и получишь доступ к заработку.\n\n"
    "🚀 <i>Хватит лениться, начни зарабатывать!</i>",

    "😠 <b>Ты серьёзно?</b>\n\n"
    "Другие юзеры уже зарабатывают, а ты даже не подписался ни на одного спонсора.\n\n"
    "⚠️ <b>Хочешь звёзды — работай:</b>\n"
    "1️⃣ Подпишись на ВСЕХ\n"
    "2️⃣ Жми «✅ Проверить подписки»\n\n"
    "💰 <i>Без подписок — нет доступа.</i>",

    "🔥 <b>Проснись!</b>\n\n"
    "Ты зашёл в бота, но ничего не делаешь. Так не пойдёт.\n\n"
    "💎 Просто подпишись на спонсоров, проверь — "
    "и звёзды твои.\n\n"
    "🚀 <i>Ты можешь больше. Докажи!</i>",
]

LAZY_ADMIN_MSGS = [
    "😴 Ленивый юзер: <code>{uid}</code> ({username}) — {subs} подписок, прошло {mins} мин",
    "💤 Бездельник: <code>{uid}</code> ({username}) — {subs} подписок",
    "🥱 Спит: <code>{uid}</code> ({username}) — не работает",
    "🦥 Ленивец: <code>{uid}</code> ({username}) — {subs} подписок, ждёт халявы",
]


async def notify_lazy_user(user_id):
    msg = random.choice(LAZY_USER_MSGS)
    try:
        await bot.send_message(user_id, msg, disable_web_page_preview=True)
        return True
    except Exception:
        return False


async def notify_admin_lazy(user_id, username, subs, mins):
    msg = random.choice(LAZY_ADMIN_MSGS).format(
        uid=user_id, username=username or "—", subs=subs, mins=mins
    )
    try:
        await bot.send_message(ADMIN_ID, msg, disable_web_page_preview=True)
    except Exception:
        pass# ========== PIARFLOW ==========
async def get_piarflow_sponsors(user_id, chat_id, max_sponsors):
    if not PIARFLOW_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors",
            json={"user_id": user_id, "chat_id": chat_id, "max_sponsors": max_sponsors},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("status") == "ok":
                    return data.get("sponsors", [])
    except Exception as e:
        await log_error(user_id, "piarflow", "fetch", str(e))
    return []


async def check_piarflow_sponsors(user_id, links):
    if not PIARFLOW_API_KEY or not links: return []
    s = await http()
    try:
        async with s.post("https://piarflow.com/v1/sponsors/check",
            json={"user_id": user_id, "links": links},
            headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("status") == "ok":
                    return data.get("sponsors", [])
    except Exception as e:
        await log_error(user_id, "piarflow", "check", str(e))
    return []


# ========== TGRASS ==========
async def get_tgrass_offers(user_id, username, lang="ru", is_premium=False):
    if not TGRASS_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://tgrass.space/offers",
            json={"tg_user_id": user_id, "tg_login": username or "", "lang": lang or "ru", "is_premium": is_premium},
            headers={"Auth": TGRASS_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200:
                await log_error(user_id, "tgrass", f"http_{r.status}", f"status={r.status}")
                return []
            data = await r.json()
            if data.get("status") == "not_ok":
                return data.get("offers", [])
    except Exception as e:
        await log_error(user_id, "tgrass", "fetch", str(e))
    return []


async def check_tgrass_subscription(user_id):
    if not TGRASS_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://tgrass.space/offers",
            json={"tg_user_id": user_id},
            headers={"Auth": TGRASS_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status != 200:
                return False
            data = await r.json()
            return data.get("status") == "ok"
    except Exception as e:
        await log_error(user_id, "tgrass", "check", str(e))
    return False


# ========== TRAFSLY ==========
async def get_trafsly_sponsors(user_id, chat_id=None, first_name=None, username=None,
                                language_code="ru", is_premium=False, max_sponsors=14):
    if not TRAFSLY_API_KEY: return []
    payload = {
        "user_id": user_id, "max_sponsors": max_sponsors,
        "language_code": language_code, "is_premium": is_premium
    }
    if chat_id: payload["chat_id"] = chat_id
    if first_name: payload["first_name"] = first_name
    if username: payload["username"] = username

    s = await http()
    try:
        async with s.post("https://api.trafsly.com/api/v1/get-sponsors",
            json=payload, headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("status") == "ok":
                    return []
                if data.get("status") == "warning":
                    sponsors = data.get("sponsors", [])
                    pool = await get_pool()
                    for sp in sponsors:
                        link = sp.get("link")
                        ads_id = sp.get("ads_id")
                        need_check = 1 if ads_id else 0
                        if link:
                            try:
                                async with pool.acquire() as db:
                                    await db.execute("""
                                        INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, need_check, created_at)
                                        VALUES ($1,$2,$3,$4,$5,$6,$7)
                                        ON CONFLICT (user_id, service, assignment_id) DO NOTHING
                                    """, user_id, "trafsly",
                                        str(ads_id) if ads_id else link,
                                        link, "unsubscribed", need_check, time.time())
                            except Exception as e:
                                logging.error(f"Trafsly insert: {e}")
                    return sponsors
    except Exception as e:
        await log_error(user_id, "trafsly", "fetch", str(e))
    return []


async def check_trafsly_sponsors(user_id, ads_ids):
    if not TRAFSLY_API_KEY or not ads_ids: return []
    s = await http()
    pool = await get_pool()
    results = []
    for aid in ads_ids:
        try:
            async with s.post("https://api.trafsly.com/api/v1/confirm-subscription",
                json={"user_id": user_id, "ads_id": int(aid)},
                headers={"Auth": TRAFSLY_API_KEY, "Content-Type": "application/json"}) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("subscribed") is True:
                        async with pool.acquire() as db:
                            await db.execute("""
                                UPDATE sponsor_tasks SET status='subscribed'
                                WHERE user_id=$1 AND service='trafsly' AND assignment_id=$2
                            """, user_id, str(aid))
                        results.append({"ads_id": aid, "status": "subscribed"})
                    elif data.get("subscribed") is False:
                        msg = str(data.get("message", "")).lower()
                        if data.get("status") == "error" or "not verified" in msg or "too many" in msg:
                            async with pool.acquire() as db:
                                await db.execute("""
                                    UPDATE sponsor_tasks SET status='subscribed'
                                    WHERE user_id=$1 AND service='trafsly' AND assignment_id=$2
                                """, user_id, str(aid))
                            results.append({"ads_id": aid, "status": "subscribed"})
                        else:
                            results.append({"ads_id": aid, "status": "unsubscribed"})
                    elif data.get("message") == "User is banned":
                        async with pool.acquire() as db:
                            await db.execute("DELETE FROM sponsor_tasks WHERE user_id=$1 AND service='trafsly'", user_id)
                        return []
        except Exception as e:
            await log_error(user_id, "trafsly", f"check_{aid}", str(e))
    return results


# ========== AXIONNA ==========
async def get_axionna(user_id, max_sponsors=20):
    if not AXIONNA_API_KEY: return []
    s = await http()
    try:
        async with s.post("https://axionna.org/api/sponsors",
            json={"user_id": user_id, "max_sponsors": max_sponsors},
            headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200:
                return []
            d = await r.json()
            if d.get("status") == "ok":
                return d.get("sponsors", [])
    except Exception as e:
        await log_error(user_id, "axionna", "fetch", str(e))
    return []


async def check_axionna(user_id, task_ids):
    if not AXIONNA_API_KEY or not task_ids: return {}
    s = await http()
    try:
        async with s.post("https://axionna.org/api/check",
            json={"user_id": user_id, "task_ids": task_ids},
            headers={"Authorization": f"Bearer {AXIONNA_API_KEY}", "Content-Type": "application/json"}) as r:
            if r.status != 200:
                return {}
            d = await r.json()
            if d.get("status") == "ok":
                return {str(x.get("id")): (x.get("status") == "ok") for x in d.get("results", [])}
    except Exception as e:
        await log_error(user_id, "axionna", "check", str(e))
    return {}


# ========== FLYER ==========
async def get_flyer_tasks(user_id, lang="ru", max_sponsors=14):
    if not flyer: return []
    try:
        tasks = await flyer.get_tasks(user_id=user_id, language_code=lang or "ru", limit=max_sponsors)
        logging.info(f"FLYER get_tasks: {len(tasks) if tasks else 0}")
        if not tasks: return []
        incomplete = [t for t in tasks if t.get('status') in INCOMPLETE_STATUSES]
        out = []
        for t in incomplete:
            links = t.get('links', [])
            if not links: continue
            out.append({
                "id": t.get('signature'),
                "link": links[0],
                "signature": t.get('signature'),
                "task": t.get('task', ''),
            })
        return out
    except FlyerAPIError as e:
        await log_error(user_id, "flyer", "get_tasks_api", str(e))
    except Exception as e:
        await log_error(user_id, "flyer", "get_tasks", str(e))
    return []


async def check_flyer_task(signature):
    if not flyer or not signature: return False
    try:
        status = await flyer.check_task(signature=signature)
        return status == "complete"
    except FlyerAPIError as e:
        await log_error(0, "flyer", "check_api", str(e))
    except Exception as e:
        await log_error(0, "flyer", "check", str(e))
    return False


# ========== BOTOHUB ==========
async def get_botohub_tasks(user_id):
    if not BOTOHUB_API_KEY:
        await log_error(user_id, "botohub", "no_api_key", "ключ не задан")
        return []
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id},
            headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}) as r:
            body = await r.text()
            logging.info(f"Botohub RAW: status={r.status}, body={body[:500]}")
            if r.status != 200:
                await log_error(user_id, "botohub", "http_error", f"HTTP {r.status}: {body[:200]}")
                return []

            data = json.loads(body)

            if data.get("skip") or data.get("completed"):
                return []

            tasks = None
            for field in ("tasks", "sponsors", "result", "results", "data", "items", "links"):
                v = data.get(field)
                if isinstance(v, list):
                    tasks = v
                    break
            if not tasks:
                return []

            links_out = []
            pool = await get_pool()
            for t in tasks:
                link = None
                aid = None
                if isinstance(t, str):
                    link = t; aid = t
                elif isinstance(t, dict):
                    link = t.get("link") or t.get("url") or t.get("target_link") or t.get("target")
                    aid = t.get("id") or t.get("task_id") or t.get("assignment_id") or link
                if link:
                    links_out.append(link)
                    try:
                        async with pool.acquire() as db:
                            await db.execute("""
                                INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at)
                                VALUES ($1,$2,$3,$4,$5,$6)
                                ON CONFLICT (user_id, service, assignment_id) DO NOTHING
                            """, user_id, "botohub", str(aid) if aid else link, link, "unsubscribed", time.time())
                    except Exception as e:
                        logging.error(f"Botohub insert: {e}")
                    await log_sponsor(user_id, "botohub", link, "выдан")
            return links_out
    except Exception as e:
        await log_error(user_id, "botohub", "fetch", str(e))
        return []


async def check_botohub_tasks(user_id):
    if not BOTOHUB_API_KEY: return False
    s = await http()
    try:
        async with s.post("https://botohub.me/get-tasks",
            json={"chat_id": user_id},
            headers={"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}) as r:
            if r.status == 200:
                data = await r.json()
                return bool(data.get("completed"))
    except Exception as e:
        await log_error(user_id, "botohub", "check", str(e))
    return False


# ========== СБОР ВСЕХ СПОНСОРОВ ==========
async def get_all_sponsors(user, force_refresh=False):
    user_id = user.id
    username = getattr(user, "username", "") or ""
    first_name = getattr(user, "first_name", "") or ""
    lang = getattr(user, "language_code", "ru") or "ru"
    is_premium = getattr(user, "is_premium", False) or False
    max_sponsors = await get_max_sponsors()

    pool = await get_pool()

    if not force_refresh:
        async with pool.acquire() as db:
            row = await db.fetchrow(
                "SELECT payload, expires_at FROM sponsor_cache WHERE cache_key = $1",
                f"all_sponsors_{user_id}"
            )
            if row and row["expires_at"] > time.time():
                return json.loads(row["payload"])

    all_sponsors = []

    # Piarflow
    piarflow = await get_piarflow_sponsors(user_id, user_id, max_sponsors)
    for s in piarflow:
        link = s.get("link")
        if not link: continue
        await log_sponsor(user_id, "piarflow", link, "выдан")
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at)
                VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (user_id, service, assignment_id) DO NOTHING
            """, user_id, "piarflow", link, link, "unsubscribed", time.time())
    all_sponsors.extend(piarflow)

    # Flyer
    flyer_tasks = await get_flyer_tasks(user_id, lang, max_sponsors)
    for s in flyer_tasks:
        link = s.get("link")
        aid = s.get("id")
        if not link or not aid: continue
        await log_sponsor(user_id, "flyer", link, "выдан")
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, signature, task_type, status, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (user_id, service, assignment_id) DO NOTHING
            """, user_id, "flyer", str(aid), link, s.get("signature"), s.get("task"), "unsubscribed", time.time())
    all_sponsors.extend(flyer_tasks)

    # Tgrass
    tgrass = await get_tgrass_offers(user_id, username, lang, is_premium)
    for s in tgrass:
        link = s.get("link")
        if not link: continue
        await log_sponsor(user_id, "tgrass", link, "выдан")
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at)
                VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (user_id, service, assignment_id) DO NOTHING
            """, user_id, "tgrass", str(s.get("offer_id")), link, "unsubscribed", time.time())
    all_sponsors.extend(tgrass)

    # Axionna
    axionna = await get_axionna(user_id, max_sponsors)
    for s in axionna:
        link = s.get("link")
        aid = s.get("id")
        if not link or not aid: continue
        await log_sponsor(user_id, "axionna", link, "выдан")
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO sponsor_tasks (user_id, service, assignment_id, link, status, created_at)
                VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (user_id, service, assignment_id) DO NOTHING
            """, user_id, "axionna", str(aid), link, "unsubscribed", time.time())
    all_sponsors.extend(axionna)

    # Botohub (уже сохраняет в БД)
    botohub_links = await get_botohub_tasks(user_id)
    all_sponsors.extend([{"link": link} for link in botohub_links])

    # Trafsly (уже сохраняет в БД)
    trafsly = await get_trafsly_sponsors(user_id, user_id, first_name, username, lang, is_premium, max_sponsors)
    all_sponsors.extend(trafsly)

    all_sponsors = all_sponsors[:max_sponsors]
    logging.info(f"Всего спонсоров: {len(all_sponsors)}")

    if not all_sponsors:
        await log_sponsor_status(user_id, "not_issued", "Нет активных заданий")
    else:
        await log_sponsor_status(user_id, "issued", f"Выдано {len(all_sponsors)} спонсоров")

    if all_sponsors:
        async with pool.acquire() as db:
            await db.execute("""
                INSERT INTO sponsor_cache (cache_key, payload, expires_at) VALUES ($1,$2,$3)
                ON CONFLICT (cache_key) DO UPDATE SET payload=$2, expires_at=$3
            """, f"all_sponsors_{user_id}", json.dumps(all_sponsors), time.time() + CACHE_TTL)

    return all_sponsors


# ========== ПРОВЕРКА ВСЕХ ПОДПИСОК ==========
async def check_all_subscriptions(user_id):
    pool = await get_pool()
    async with pool.acquire() as db:
        tasks = await db.fetch("""
            SELECT service, assignment_id, link, signature
            FROM sponsor_tasks
            WHERE user_id = $1 AND status != 'subscribed'
        """, user_id)

    if not tasks:
        await log_sponsor_status(user_id, "subscribed", "Все задания выполнены")
        return True

    all_done = True
    new_subs = []

    # PIARFLOW — списком
    piarflow_links = [t["link"] for t in tasks if t["service"] == "piarflow" and t["link"]]
    if piarflow_links:
        results = await check_piarflow_sponsors(user_id, piarflow_links)
        done_links = set()
        for r in results:
            if r.get("status") in ("subscribed", "not_counted"):
                done_links.add(r.get("link"))
        for link in piarflow_links:
            if link in done_links:
                async with pool.acquire() as db:
                    await db.execute("""
                        UPDATE sponsor_tasks SET status='subscribed'
                        WHERE user_id=$1 AND service='piarflow' AND assignment_id=$2
                    """, user_id, link)
                new_subs.append(("piarflow", link, 1))
            else:
                all_done = False

    # TRAFSLY — по assignment_id
    trafsly_tasks = [t for t in tasks if t["service"] == "trafsly"]
    for t in trafsly_tasks:
        aid = t["assignment_id"]
        try:
            await check_trafsly_sponsors(user_id, [int(aid)])
        except Exception:
            pass
    async with pool.acquire() as db:
        left = await db.fetchrow("""
            SELECT COUNT(*) as c FROM sponsor_tasks
            WHERE user_id=$1 AND service='trafsly' AND status!='subscribed'
        """, user_id)
        if left["c"] > 0:
            all_done = False
        else:
            new_subs.append(("trafsly", "all", left["c"]))

    # TGRASS — общая
    tgrass_tasks = [t for t in tasks if t["service"] == "tgrass"]
    if tgrass_tasks:
        if await check_tgrass_subscription(user_id):
            async with pool.acquire() as db:
                await db.execute("""
                    UPDATE sponsor_tasks SET status='subscribed'
                    WHERE user_id=$1 AND service='tgrass' AND status!='subscribed'
                """, user_id)
            new_subs.append(("tgrass", "all", len(tgrass_tasks)))
        else:
            all_done = False

    # AXIONNA — по task_ids
    axionna_tasks = [t for t in tasks if t["service"] == "axionna"]
    if axionna_tasks:
        ids = [t["assignment_id"] for t in axionna_tasks]
        res = await check_axionna(user_id, ids)
        for aid, ok in res.items():
            if ok:
                async with pool.acquire() as db:
                    await db.execute("""
                        UPDATE sponsor_tasks SET status='subscribed'
                        WHERE user_id=$1 AND service='axionna' AND assignment_id=$2
                    """, user_id, aid)
                new_subs.append(("axionna", aid, 1))
            else:
                all_done = False

    # FLYER — по signature
    flyer_tasks = [t for t in tasks if t["service"] == "flyer"]
    for t in flyer_tasks:
        sig = t["signature"]
        if not sig:
            continue
        if await check_flyer_task(sig):
            async with pool.acquire() as db:
                await db.execute("""
                    UPDATE sponsor_tasks SET status='subscribed'
                    WHERE user_id=$1 AND service='flyer' AND assignment_id=$2
                """, user_id, t["assignment_id"])
            new_subs.append(("flyer", t["assignment_id"], 1))
        else:
            all_done = False

    # BOTOHUB — общая
    botohub_tasks = [t for t in tasks if t["service"] == "botohub"]
    if botohub_tasks:
        if await check_botohub_tasks(user_id):
            async with pool.acquire() as db:
                await db.execute("""
                    UPDATE sponsor_tasks SET status='subscribed'
                    WHERE user_id=$1 AND service='botohub' AND status!='subscribed'
                """, user_id)
            new_subs.append(("botohub", "all", len(botohub_tasks)))
        else:
            all_done = False

    if not all_done:
        await log_sponsor_status(user_id, "not_subscribed", "Не все подписки выполнены")
    else:
        await log_sponsor_status(user_id, "subscribed", "Все подписки выполнены")

    if new_subs:
        await notify_admin_new_subs(user_id, new_subs)

    async with pool.acquire() as db:
        cnt = await db.fetchrow("""
            SELECT COUNT(*) as c FROM sponsor_tasks
            WHERE user_id = $1 AND status != 'subscribed'
        """, user_id)
        return cnt["c"] == 0


# ========== АКТИВАЦИЯ ==========
async def activate_user(user_id, username="Unknown"):
    is_all_done = await check_all_subscriptions(user_id)
    if not is_all_done:
        return False

    pool = await get_pool()
    async with pool.acquire() as db:
        referrer_row = await db.fetchrow("SELECT referrer_id FROM users WHERE user_id=$1", user_id)
        referrer_id = referrer_row["referrer_id"] if referrer_row else None

        await db.execute("UPDATE users SET is_activated = 1 WHERE user_id = $1", user_id)

        if referrer_id and referrer_id != user_id:
            cnt_row = await db.fetchrow("""
                SELECT COUNT(*) as c FROM sponsor_tasks
                WHERE user_id=$1 AND status='subscribed'
            """, user_id)
            sponsors_count = cnt_row["c"] if cnt_row else 0

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

                bal_row = await db.fetchrow("SELECT balance FROM users WHERE user_id=$1", referrer_id)
                new_bal = bal_row["balance"] if bal_row else 0

                try:
                    await bot.send_message(
                        referrer_id,
                        f"🆗 <b>Реферал подтвердил подписку!</b>\n"
                        f"👤 Реферал: @{username or 'нет'}\n"
                        f"💬 Награда: +{reward:.1f}⭐ ({sponsors_count} × {sponsor_reward})\n"
                        f"📊 Спонсоров у реферала: {sponsors_count}\n"
                        f"💎 Твой баланс: {new_bal:.1f}⭐"
                    )
                except Exception:
                    pass

        return True# ========== ПРОВЕРКА ПЕРЕД ДЕЙСТВИЕМ ==========
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
            "📌 <b>Подпишись на каналы, затем нажми «Проверить подписки»:</b>",
            reply_markup=sponsors_keyboard(sponsors, 1)
        )
        return False
    else:
        await activate_user(user_id, message.from_user.username or "Unknown")
        await message.answer(
            "🎉 <b>Добро пожаловать!</b>",
            reply_markup=main_menu(user_id)
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
        types.InlineKeyboardButton(text="🎚 Приоритет сервисов", callback_data="admin_order")
    )
    builder.row(
        types.InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        types.InlineKeyboardButton(text="🔎 Диагностика", callback_data="admin_diag")
    )
    builder.row(types.InlineKeyboardButton(text="❌ Закрыть", callback_data="admin_close"))
    return builder.as_markup()


def service_order_kb(order):
    kb = InlineKeyboardBuilder()
    for i, s in enumerate(order):
        if i > 0:
            kb.button(text="⬆️", callback_data=f"ord_up_{i}")
        else:
            kb.button(text="⛔", callback_data="ord_noop")
        if i < len(order) - 1:
            kb.button(text="⬇️", callback_data=f"ord_dn_{i}")
        else:
            kb.button(text="⛔", callback_data="ord_noop")
        kb.button(text=f"{i+1}. {s}", callback_data="ord_noop")
    kb.button(text="⬅️ Назад", callback_data="ord_back")
    rows = [3] * len(order) + [1]
    kb.adjust(*rows)
    return kb.as_markup()


def service_order_text(order):
    lines = ["🎚 <b>Приоритет сервисов</b>\n"]
    lines.append("📋 <b>Текущий порядок:</b>")
    for i, s in enumerate(order, 1):
        lines.append(f"{i}. {s}")
    lines.append("\n<i>Сервисы выше — выдаются первыми.</i>")
    lines.append("\nЖми ⬆️ / ⬇️ чтобы менять порядок.")
    return "\n".join(lines)


# ========== ХЕНДЛЕРЫ ==========
@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"

    referrer_id = None
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])

    is_new = await register_user(user_id, username, referrer_id)

    if is_new:
        await add_lazy_watch(user_id)

    # Уведомление админу о новом юзере
    if is_new:
        try:
            sponsors_tmp = await get_all_sponsors(message.from_user)
            services_map = await pending_by_service(user_id)
            total_sp = sum(services_map.values())
            await notify_admin_new_user(
                user_id, f"@{username}" if username != "Unknown" else "", total_sp, services_map
            )
        except Exception as e:
            logging.error(f"notify new user: {e}")

    sponsors = await get_all_sponsors(message.from_user)

    if referrer_id and referrer_id != user_id and sponsors:
        try:
            await bot.send_message(
                referrer_id,
                f"🔔 <b>НОВЫЙ РЕФЕРАЛ!</b>\n"
                f"🛫 Реферал: {username or 'Без юзернейма'}\n"
                f"🗂 Ему выдано спонсоров: {len(sponsors)}\n"
                f"🖥 <i>После подписки вы получите награду!</i>"
            )
        except Exception:
            pass

    if sponsors:
        await message.answer(
            "📌 <b>Подпишись на каналы, затем нажми «Проверить подписки»:</b>",
            reply_markup=sponsors_keyboard(sponsors, 1)
        )
    else:
        await activate_user(user_id, username)
        await message.answer(
            "🎉 <b>Добро пожаловать!</b>",
            reply_markup=main_menu(user_id)
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
    except Exception:
        await callback.answer("❌ Ошибка!")
    await callback.answer()


@dp.callback_query(F.data == "check_subs")
async def check_subs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.answer("🔄 Проверяю...")

    all_done = await check_all_subscriptions(user_id)

    if all_done:
        await activate_user(user_id, callback.from_user.username or "Unknown")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer(
            "🎉 <b>Все подписки подтверждены!</b>",
            reply_markup=main_menu(user_id)
        )
        await callback.answer("✅ Готово!")
    else:
        pool = await get_pool()
        async with pool.acquire() as db:
            unsubscribed = await db.fetch("""
                SELECT link, service FROM sponsor_tasks
                WHERE user_id = $1 AND status != 'subscribed'
            """, user_id)

        if unsubscribed:
            # Антиспам ручных заявок — раз в минуту на юзера
            last = _manual_last.get(user_id, 0)
            if time.time() - last < MANUAL_COOLDOWN:
                await callback.answer(f"❌ Осталось {len(unsubscribed)} подписок! Подожди минуту.", show_alert=True)
                return
            _manual_last[user_id] = time.time()

            sponsors_list = [{"link": t["link"], "service": t["service"]} for t in unsubscribed]
            async with pool.acquire() as db:
                await db.execute(
                    "INSERT INTO manual_checks (user_id, username, sponsors, created_at) VALUES ($1,$2,$3,$4)",
                    user_id, callback.from_user.username or "Unknown", json.dumps(sponsors_list), time.time()
                )

            kb = InlineKeyboardBuilder()
            kb.row(
                types.InlineKeyboardButton(text="✅ Разрешить", callback_data=f"manual_allow_{user_id}"),
                types.InlineKeyboardButton(text="❌ Отказать", callback_data=f"manual_deny_{user_id}")
            )

            await bot.send_message(
                ADMIN_ID,
                f"🔔 <b>Ручная проверка!</b>\n\n"
                f"👤 @{callback.from_user.username or 'нет'} (ID: <code>{user_id}</code>)\n"
                f"📊 Неподтверждённых: {len(unsubscribed)}\n\n"
                f"📌 Список:\n" + "\n".join([f"  • {t['link']} ({t['service']})" for t in unsubscribed]),
                reply_markup=kb.as_markup(),
                disable_web_page_preview=True
            )

            try:
                await callback.answer(f"❌ Осталось {len(unsubscribed)} подписок!", show_alert=True)
            except Exception:
                pass

            user = types.User(id=user_id, is_bot=False, first_name="User", language_code="ru")
            sponsors = await get_all_sponsors(user, force_refresh=True)
            if sponsors:
                try:
                    await callback.message.edit_reply_markup(reply_markup=sponsors_keyboard(sponsors, 1))
                except Exception:
                    pass
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
        await db.execute("UPDATE sponsor_tasks SET status='subscribed' WHERE user_id=$1 AND status!='subscribed'", user_id)
        await db.execute("UPDATE users SET is_activated=1 WHERE user_id=$1", user_id)
        await db.execute("UPDATE manual_checks SET status='approved' WHERE user_id=$1 AND status='pending'", user_id)
    try:
        await bot.send_message(user_id, "🎉 <b>Доступ разрешён!</b>", reply_markup=main_menu(user_id))
    except Exception:
        pass
    await callback.message.edit_text(f"✅ Доступ разрешён для <code>{user_id}</code>")
    await callback.answer("✅")


@dp.callback_query(F.data.startswith("manual_deny_"))
async def manual_deny(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!")
        return
    user_id = int(callback.data.split("_")[2])
    pool = await get_pool()
    async with pool.acquire() as db:
        await db.execute("UPDATE manual_checks SET status='denied' WHERE user_id=$1 AND status='pending'", user_id)
    try:
        await bot.send_message(user_id, "❌ <b>Доступ отклонён.</b>")
    except Exception:
        pass
    await callback.message.edit_text(f"❌ Доступ отклонён для <code>{user_id}</code>")
    await callback.answer("❌")


# ========== REPLY-КНОПКИ ==========
@dp.message(F.text == "👤 Профиль")
async def profile_cmd(message: types.Message):
    if not await check_sponsors_before_action(message):
        return
    user_id = message.from_user.id
    user = await get_user(user_id)
    if user:
        pool = await get_pool()
        async with pool.acquire() as db:
            active_refs = (await db.fetchrow(
                "SELECT COUNT(*) as c FROM users WHERE referrer_id=$1 AND is_activated=1", user_id
            ))["c"]
            total_refs = (await db.fetchrow(
                "SELECT COUNT(*) as c FROM users WHERE referrer_id=$1", user_id
            ))["c"]

        await message.answer(
            f"👤 <b>Твой профиль</b>\n\n"
            f"🆔 ID: <code>{user['user_id']}</code>\n"
            f"💰 Баланс: <b>{user['balance']:.1f} ⭐</b>\n"
            f"👥 Рефералов: <b>{total_refs}</b>\n"
            f"✅ Активных: <b>{active_refs}</b>\n"
            f"📊 Всего заработано: <b>{user['total_earned']:.1f} ⭐</b>"
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
        f"🔗 <b>Твоя реферальная ссылка:</b>\n"
        f"<code>https://t.me/{bot_info.username}?start={user_id}</code>\n\n"
        f"Приглашай друзей и получай <b>{sponsor_reward} ⭐</b> за каждого спонсора!"
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
        await message.answer("⏳ <b>Уже забрал сегодня!</b>")
        return

    pool = await get_pool()
    async with pool.acquire() as db:
        cnt = await db.fetchrow(
            "SELECT COUNT(*) as c FROM sponsor_tasks WHERE user_id=$1 AND status='subscribed'", user_id
        )
        sponsors_count = cnt["c"] if cnt else 0

    if sponsors_count == 0:
        await message.answer("❌ Нет выполненных спонсоров!")
        return

    sponsor_reward = await get_sponsor_reward()
    bonus = sponsors_count * sponsor_reward

    await update_balance(user_id, bonus)
    async with pool.acquire() as db:
        await db.execute("UPDATE users SET last_bonus=$1 WHERE user_id=$2", today, user_id)

    await message.answer(
        f"🎁 <b>Бонус!</b>\n\n"
        f"📊 Спонсоров: <b>{sponsors_count}</b>\n"
        f"💰 Награда: <b>{sponsor_reward} ⭐</b>\n"
        f"💎 Получено: <b>+{bonus:.1f} ⭐</b>"
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
        "💎 <b>Вывод</b>\n\nВведи username (без @):\nПример: <code>ivan_durov</code>\n\nЕсли нет — отправь <code>-</code>"
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
            "INSERT INTO withdraw_requests (user_id, username, amount, created_at) VALUES ($1,$2,$3,$4)",
            user_id, username, amount, time.time()
        )

    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🔔 <b>Заявка на вывод!</b>\n\n"
                f"👤 ID: <code>{user_id}</code>\n"
                f"💰 Сумма: {amount} ⭐\n"
                f"📌 Username: @{username}"
            )
        except Exception:
            pass

    await message.answer(f"✅ Заявка на {amount} ⭐ принята!\nПодарок придёт на @{username} в течение 24ч.")
    await state.clear()


# ========== АДМИН-ПАНЕЛЬ ==========
@dp.message(F.text == "👑 Админ")
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("👑 <b>Админ-панель</b>", reply_markup=admin_keyboard())


@dp.callback_query(F.data == "admin_sponsor_status")
async def admin_sponsor_status(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        rows = await db.fetch(
            "SELECT user_id, status, reason, updated_at FROM sponsor_status ORDER BY updated_at DESC LIMIT 20"
        )
    if not rows:
        await callback.message.edit_text("📋 Нет данных.", reply_markup=admin_keyboard())
        return
    text = "📋 <b>Статус (20):</b>\n\n"
    for r in rows:
        date = datetime.fromtimestamp(r["updated_at"]).strftime("%d.%m %H:%M")
        emoji = "✅" if r["status"] in ["issued", "subscribed"] else "❌"
        text += f"{emoji} [{date}] ID {r['user_id']} → <b>{r['status']}</b>\n"
    await callback.message.edit_text(text, reply_markup=admin_keyboard())


@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    pool = await get_pool()
    async with pool.acquire() as db:
        logs = await db.fetch(
            "SELECT user_id, service, link, created_at FROM sponsor_log ORDER BY created_at DESC LIMIT 20"
        )
    if not logs:
        await callback.message.edit_text("📋 Нет логов.", reply_markup=admin_keyboard())
        return
    text = "📋 <b>Последние выдачи:</b>\n\n"
    for l in logs:
        date = datetime.fromtimestamp(l["created_at"]).strftime("%d.%m %H:%M")
        text += f"• [{date}] {l['service']} → {l['user_id']}\n"
    await callback.message.edit_text(text, reply_markup=admin_keyboard())


@dp.callback_query(F.data == "admin_set_sponsor_reward")
async def admin_set_sponsor_reward_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.waiting_for_sponsor_reward)
    current = await get_sponsor_reward()
    await callback.message.edit_text(f"⚙ Награда за спонсора (текущая: {current} ⭐):\n\nВведи число:")


@dp.message(AdminState.waiting_for_sponsor_reward)
async def admin_set_sponsor_reward_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        value = float(message.text.replace(",", "."))
        if value <= 0:
            await message.answer("❌ Больше 0!")
            return
        await set_setting("sponsor_reward", str(value))
        await message.answer(f"✅ Награда: {value} ⭐")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введи число!")


@dp.callback_query(F.data == "admin_set_max_sponsors")
async def admin_set_max_sponsors_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.waiting_for_max_sponsors)
    current_max = await get_max_sponsors()
    await callback.message.edit_text(f"⚙ Максимум спонсоров (текущий: {current_max}):\n\n1-30:")


@dp.message(AdminState.waiting_for_max_sponsors)
async def admin_set_max_sponsors_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    try:
        value = int(message.text.strip())
        if value < 1 or value > MAX_LIMIT:
            await message.answer(f"❌ 1-{MAX_LIMIT}!")
            return
        await set_setting("max_sponsors", str(value))
        await message.answer(f"✅ Максимум: {value}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Целое число!")


# ========== ПРИОРИТЕТ СЕРВИСОВ ==========
@dp.callback_query(F.data == "admin_order")
async def admin_order(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.answer()
    order = await get_service_order()
    try:
        await callback.message.edit_text(
            service_order_text(order),
            reply_markup=service_order_kb(order)
        )
    except Exception:
        pass


@dp.callback_query(F.data == "ord_noop")
async def cb_ord_noop(callback: types.CallbackQuery):
    await callback.answer()


@dp.callback_query(F.data == "ord_back")
async def cb_ord_back(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.answer()
    try:
        await callback.message.edit_text("👑 <b>Админ-панель</b>", reply_markup=admin_keyboard())
    except Exception:
        pass


@dp.callback_query(F.data.startswith("ord_up_"))
async def cb_ord_up(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    try:
        i = int(callback.data.replace("ord_up_", ""))
    except Exception:
        await callback.answer("Ошибка"); return
    order = await get_service_order()
    if i <= 0 or i >= len(order):
        await callback.answer("Нельзя выше"); return
    order[i-1], order[i] = order[i], order[i-1]
    await set_setting("service_order", ",".join(order))
    await callback.answer("⬆️ Поднято")
    try:
        await callback.message.edit_text(service_order_text(order), reply_markup=service_order_kb(order))
    except Exception:
        pass


@dp.callback_query(F.data.startswith("ord_dn_"))
async def cb_ord_dn(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    try:
        i = int(callback.data.replace("ord_dn_", ""))
    except Exception:
        await callback.answer("Ошибка"); return
    order = await get_service_order()
    if i < 0 or i >= len(order) - 1:
        await callback.answer("Нельзя ниже"); return
    order[i], order[i+1] = order[i+1], order[i]
    await set_setting("service_order", ",".join(order))
    await callback.answer("⬇️ Опущено")
    try:
        await callback.message.edit_text(service_order_text(order), reply_markup=service_order_kb(order))
    except Exception:
        pass


# ========== РАССЫЛКА ==========
@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID: return
    await state.set_state(AdminState.waiting_for_broadcast)
    await callback.message.edit_text(
        "📢 <b>Рассылка</b>\n\nОтправь сообщение для рассылки.\n\n❌ Отмена: <code>/cancel</code>"
    )


@dp.message(AdminState.waiting_for_broadcast)
async def admin_broadcast_process(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return

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
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)

    await message.answer(
        f"✅ <b>Готово!</b>\n\n📤 {ok}\n❌ {fail}",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()


# ========== ДИАГНОСТИКА ==========
async def diagnose_service(name, key, url, payload, headers=None):
    if not key:
        return f"❌ <b>{name}</b> — ключ не задан"
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    t0 = time.time()
    try:
        s = await http()
        async with s.post(url, json=payload, headers=h, timeout=aiohttp.ClientTimeout(total=10)) as r:
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

            return f"🟡 <b>{name}</b> — {r.status} OK ({dt}мс)"
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
        diagnose_service("TGrass", TGRASS_API_KEY,
            "https://tgrass.space/offers",
            {"tg_user_id": test_uid, "tg_login": "", "lang": "ru", "is_premium": False},
            {"Auth": TGRASS_API_KEY}),
        diagnose_service("Trafsly", TRAFSLY_API_KEY,
            "https://api.trafsly.com/api/v1/get-sponsors",
            {"user_id": test_uid, "max_sponsors": mx, "language_code": "ru", "is_premium": False},
            {"Auth": TRAFSLY_API_KEY}),
        diagnose_service("Axionna", AXIONNA_API_KEY,
            "https://axionna.org/api/sponsors",
            {"user_id": test_uid, "max_sponsors": mx},
            {"Authorization": f"Bearer {AXIONNA_API_KEY}"}),
        diagnose_service("Botohub", BOTOHUB_API_KEY,
            "https://botohub.me/get-tasks",
            {"chat_id": test_uid},
            {"Auth": BOTOHUB_API_KEY}),
    )
    header = f"🔎 <b>Диагностика API</b>\n🆔 user_id: <code>{test_uid}</code>\n\n"
    return header + "\n\n".join(results)


@dp.callback_query(F.data == "admin_diag")
async def admin_diag(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.answer("Проверяю…")
    try:
        await callback.message.edit_text("🔎 Диагностика…")
    except Exception:
        pass
    report = await run_diagnostics()
    if len(report) > 4000:
        report = report[:4000] + "…"
    try:
        await callback.message.edit_text(report, reply_markup=admin_keyboard())
    except Exception:
        try:
            await callback.message.answer(report, reply_markup=admin_keyboard())
        except Exception:
            pass


@dp.callback_query(F.data == "admin_close")
async def admin_close(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass


# ========== ЛЕНИВЫЙ ВОРКЕР ==========
async def lazy_worker():
    await asyncio.sleep(60)
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as db:
                rows = await db.fetch("""
                    SELECT user_id, created_at, last_notified, admin_notified
                    FROM lazy_watch
                """)

            for r in rows:
                uid = r["user_id"]
                try:
                    user = await get_user(uid)
                    if not user:
                        await remove_lazy_watch(uid)
                        continue
                    if user.get("is_activated") == 1:
                        await remove_lazy_watch(uid)
                        continue

                    pend = await pending_count(uid)
                    if pend == 0:
                        continue

                    if time.time() - (r["last_notified"] or 0) < LAZY_INTERVAL:
                        continue

                    sent = await notify_lazy_user(uid)
                    if sent:
                        pool2 = await get_pool()
                        async with pool2.acquire() as db:
                            await db.execute(
                                "UPDATE lazy_watch SET last_notified=$1 WHERE user_id=$2",
                                time.time(), uid
                            )

                        if not r["admin_notified"]:
                            mins = int((time.time() - (r["created_at"] or time.time())) / 60)
                            subs = await count_total_subs(uid)
                            try:
                                tg = await bot.get_chat(uid)
                                uname = f"@{tg.username}" if tg.username else (tg.first_name or "")
                            except Exception:
                                uname = ""
                            await notify_admin_lazy(uid, uname, subs, mins)

                            pool3 = await get_pool()
                            async with pool3.acquire() as db:
                                await db.execute(
                                    "UPDATE lazy_watch SET admin_notified=TRUE WHERE user_id=$1", uid
                                )
                except Exception as e:
                    logging.error(f"lazy check {uid}: {e}")

                await asyncio.sleep(0.3)

            await asyncio.sleep(LAZY_INTERVAL)
        except Exception as e:
            logging.error(f"lazy_worker: {e}")
            await asyncio.sleep(120)


# ========== WEB + MAIN ==========
async def handle(_):
    return web.Response(text="Bot is running!")


async def on_shutdown(*args, **kwargs):
    global _http
    if _http and not _http.closed:
        await _http.close()
        logging.info("HTTP session closed")


async def main():
    await init_db()

    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"Web on :{PORT}")

    asyncio.create_task(lazy_worker())
    dp.shutdown.register(on_shutdown)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
