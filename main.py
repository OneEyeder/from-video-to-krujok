import asyncio

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


async def main():
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
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
