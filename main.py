import asyncio
import os

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramNetworkError

from config import BOT_TOKEN
from handlers import router
import metrics_db


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

session = AiohttpSession(timeout=60)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()
dp.include_router(router)


async def _start_health_server() -> web.AppRunner:
    port_str = os.getenv("PORT")
    if not port_str:
        raise RuntimeError("Health server disabled: PORT is not set")

    async def health(_: web.Request) -> web.Response:
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(port_str)
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    return runner


async def main():
    health_runner: web.AppRunner | None = None
    try:
        health_runner = await _start_health_server()
    except Exception:
        health_runner = None
    metrics_db.init_db()
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

    try:
        await dp.start_polling(bot)
    finally:
        if health_runner is not None:
            await health_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
