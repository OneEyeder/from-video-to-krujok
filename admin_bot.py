import asyncio
import os
from datetime import datetime

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramNetworkError
from aiogram.types import Message
from dotenv import load_dotenv

import metrics_db


load_dotenv()


ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))


router = Router()


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _is_admin(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id == ADMIN_ID)


@router.message(Command("start"))
async def admin_start(message: Message):
    if not _is_admin(message):
        return
    await message.answer(
        "Админка доступна.\n\n"
        "Команды:\n"
        "/stats\n"
        "/users_today\n"
        "/videos_today\n"
        "/errors_today\n"
        "/banned\n"
        "/user <user_id>\n"
        "/ban <user_id>\n"
        "/unban <user_id>\n"
    )


@router.message(Command("errors_today"))
async def errors_today(message: Message):
    if not _is_admin(message):
        return

    rows = [r for r in metrics_db.videos_today(limit=100) if r["event"] == "video_error"]
    if not rows:
        await message.answer("Сегодня ошибок по видео нет.")
        return

    lines: list[str] = ["Ошибки по видео за сегодня (последние 30):", ""]
    for r in rows[:30]:
        effect = r["effect"] or "-"
        err = (r["error"] or "")
        if err:
            err = err[-400:]
        lines.append(f"{_fmt_ts(int(r['ts']))} | uid={r['user_id']} | effect={effect} | {err}")

    await message.answer("\n".join(lines))


@router.message(Command("banned"))
async def banned_list(message: Message):
    if not _is_admin(message):
        return

    rows = metrics_db.banned_users(limit=50)

    if not rows:
        await message.answer("Забаненных пользователей нет.")
        return

    lines: list[str] = ["Забаненные пользователи (последние 50):", ""]
    for r in rows:
        name = r["username"] or r["full_name"] or "(no name)"
        lines.append(f"{r['user_id']} | {name} | last: {_fmt_ts(int(r['last_seen_ts']))}")
    await message.answer("\n".join(lines))


@router.message(Command("stats"))
async def stats(message: Message):
    if not _is_admin(message):
        return

    s = metrics_db.stats_today()
    await message.answer(
        "Статистика за сегодня:\n\n"
        f"Пользователи всего: {s['total_users']}\n"
        f"Новых сегодня: {s['new_users_today']}\n"
        f"Активных сегодня: {s['active_users_today']}\n\n"
        f"Видео: стартов {s['videos_started_today']}\n"
        f"Видео: успех {s['videos_success_today']}\n"
        f"Видео: ошибок {s['videos_error_today']}\n"
    )


@router.message(Command("users_today"))
async def users_today(message: Message):
    if not _is_admin(message):
        return

    rows = metrics_db.users_today(limit=50)
    if not rows:
        await message.answer("Сегодня новых пользователей нет.")
        return

    lines: list[str] = ["Новые пользователи за сегодня (последние 50):", ""]
    for r in rows:
        name = r["username"] or r["full_name"] or "(no name)"
        lines.append(f"{r['user_id']} | {name} | first: {_fmt_ts(int(r['first_seen_ts']))}")

    await message.answer("\n".join(lines))


@router.message(Command("videos_today"))
async def videos_today(message: Message):
    if not _is_admin(message):
        return

    rows = metrics_db.videos_today(limit=50)
    if not rows:
        await message.answer("Сегодня событий по видео нет.")
        return

    lines: list[str] = ["Видео за сегодня (последние 50 событий):", ""]
    for r in rows:
        effect = r["effect"] or "-"
        dur = f"{float(r['video_duration']):.1f}s" if r["video_duration"] is not None else "-"
        err = (r["error"] or "")
        if err:
            err = err[:120]
        lines.append(
            f"{_fmt_ts(int(r['ts']))} | {r['event']} | uid={r['user_id']} | effect={effect} | dur={dur} | {err}"
        )

    await message.answer("\n".join(lines))


@router.message(Command("user"))
async def user_card(message: Message):
    if not _is_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /user <user_id>")
        return

    try:
        uid = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом")
        return

    card = metrics_db.user_card(uid)
    if card is None:
        await message.answer("Пользователь не найден в базе.")
        return

    await message.answer(
        "Карточка пользователя:\n\n"
        f"user_id: {card['user_id']}\n"
        f"username: {card['username']}\n"
        f"name: {card['full_name']}\n"
        f"first_seen: {_fmt_ts(int(card['first_seen_ts']))}\n"
        f"last_seen: {_fmt_ts(int(card['last_seen_ts']))}\n"
        f"banned: {card['is_banned']}\n\n"
        f"videos_success: {card['videos_success']}\n"
        f"videos_error: {card['videos_error']}\n"
    )


@router.message(Command("ban"))
async def ban_user(message: Message):
    if not _is_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /ban <user_id>")
        return

    try:
        uid = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом")
        return

    metrics_db.set_banned(uid, True)
    await message.answer(f"Пользователь {uid} забанен.")


@router.message(Command("unban"))
async def unban_user(message: Message):
    if not _is_admin(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unban <user_id>")
        return

    try:
        uid = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом")
        return

    metrics_db.set_banned(uid, False)
    await message.answer(f"Пользователь {uid} разбанен.")


@router.message(F.text)
async def ignore_non_commands(message: Message):
    if not _is_admin(message):
        return
    await message.answer("Напиши /stats")


async def _start_health_server() -> web.AppRunner:
    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    return runner


async def main() -> None:
    if not ADMIN_BOT_TOKEN:
        raise RuntimeError("ADMIN_BOT_TOKEN is not set")
    if not ADMIN_ID:
        raise RuntimeError("ADMIN_ID is not set")

    health_runner = await _start_health_server()
    metrics_db.init_db()

    session = AiohttpSession(timeout=60)
    bot = Bot(token=ADMIN_BOT_TOKEN, session=session)
    dp = Dispatcher()
    dp.include_router(router)

    try:
        # Warm up connection + retries for unstable networks on Windows (WinError 121)
        last_exc: Exception | None = None
        for attempt in range(1, 6):
            try:
                await bot.get_me()
                last_exc = None
                break
            except TelegramNetworkError as e:
                last_exc = e
                await asyncio.sleep(2 * attempt)

        if last_exc is not None:
            raise last_exc

        await dp.start_polling(bot)
    finally:
        await health_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
