import asyncio

from aiogram import F, Router
from aiogram.enums import ContentType
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup

import metrics_db
from video_processing import convert_video_to_circle


router = Router()


_user_locks: dict[int, asyncio.Lock] = {}
_media_group_first_message: dict[str, int] = {}
_global_video_lock = asyncio.Lock()
_user_effect: dict[int, str] = {}


def _track_user(message: Message) -> int:
    user_id = message.from_user.id if message.from_user else 0
    if user_id and message.from_user:
        metrics_db.upsert_user_seen(
            metrics_db.TgUserInfo(
                user_id=user_id,
                username=message.from_user.username,
                full_name=message.from_user.full_name,
            )
        )
    return user_id


BTN_NORMAL = "Обычный кружок"
BTN_EFFECTS = "Эффекты"
BTN_BACK = "Назад"

BTN_SPEED_SLOW = "Неожиданное ускорение и замедление 🚀🐢"
BTN_FLASH = "Вспышка света ⚡️"
BTN_MEME = "Сюрпризный мем 🐸"
BTN_ECHO = "Эхо голоса 👻"
BTN_SHAKE = "Размытие/дрожь камеры 🎥"


def _main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_NORMAL), KeyboardButton(text=BTN_EFFECTS)]],
        resize_keyboard=True,
    )


def _effects_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SPEED_SLOW)],
            [KeyboardButton(text=BTN_FLASH)],
            [KeyboardButton(text=BTN_MEME)],
            [KeyboardButton(text=BTN_ECHO)],
            [KeyboardButton(text=BTN_SHAKE)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


async def _forget_media_group(media_group_id: str) -> None:
    await asyncio.sleep(300)
    _media_group_first_message.pop(media_group_id, None)


@router.message(F.text == "/start")
async def start_handler(message: Message):
    _track_user(message)
    await message.answer(
        "👋 Привет!\n\n"
        "Я превращаю обычные видео в видеосообщения ⭕️.\n\n"
        "📌 Как пользоваться:\n"
        "1️⃣ Отправь видео\n"
        "2️⃣ Подожди пару секунд\n"
        "3️⃣ Получи кружок со звуком 🎥🔊\n\n"
        "⚠️ Видео до 60 секунд.\n\n"
        "Жду видео 👇",
        reply_markup=_main_kb(),
    )


@router.message(F.text == "/effects")
async def effects_handler(message: Message):
    _track_user(message)
    await message.answer("Выбери эффект для следующего кружка:", reply_markup=_effects_kb())


@router.message(F.text == BTN_EFFECTS)
async def effects_button_handler(message: Message):
    _track_user(message)
    await message.answer("Выбери эффект для следующего кружка:", reply_markup=_effects_kb())


@router.message(F.text == BTN_BACK)
async def back_button_handler(message: Message):
    user_id = _track_user(message)
    await message.answer("Ок", reply_markup=_main_kb())


@router.message(F.text == BTN_NORMAL)
async def set_effect_normal(message: Message):
    user_id = _track_user(message)
    _user_effect[user_id] = "normal"
    await message.answer("Ок, сделаю обычный кружок.", reply_markup=_main_kb())


@router.message(F.text == BTN_SPEED_SLOW)
async def set_effect_speed_slow(message: Message):
    user_id = _track_user(message)
    _user_effect[user_id] = "speed_slow"
    await message.answer("Ок, эффект выбран.", reply_markup=_effects_kb())


@router.message(F.text == BTN_FLASH)
async def set_effect_flash(message: Message):
    user_id = _track_user(message)
    _user_effect[user_id] = "flash"
    await message.answer("Ок, эффект выбран.", reply_markup=_effects_kb())


@router.message(F.text == BTN_MEME)
async def set_effect_meme(message: Message):
    user_id = _track_user(message)
    _user_effect[user_id] = "meme"
    await message.answer("Ок, эффект выбран.", reply_markup=_effects_kb())


@router.message(F.text == BTN_ECHO)
async def set_effect_echo(message: Message):
    user_id = _track_user(message)
    _user_effect[user_id] = "echo"
    await message.answer("Ок, эффект выбран.", reply_markup=_effects_kb())


@router.message(F.text == BTN_SHAKE)
async def set_effect_shake(message: Message):
    user_id = _track_user(message)
    _user_effect[user_id] = "shake"
    await message.answer("Ок, эффект выбран.", reply_markup=_effects_kb())


@router.message(F.content_type == ContentType.VIDEO)
async def video_to_circle(message: Message, bot):
    user_id = _track_user(message)

    if user_id and metrics_db.is_banned(user_id):
        metrics_db.log_event(user_id, "banned_block", message_id=message.message_id)
        await message.answer("❌ Доступ ограничен.")
        return

    media_group_id = getattr(message, "media_group_id", None)
    if media_group_id:
        first_message_id = _media_group_first_message.get(media_group_id)
        if first_message_id is None:
            _media_group_first_message[media_group_id] = message.message_id
            asyncio.create_task(_forget_media_group(media_group_id))
        elif first_message_id != message.message_id:
            await message.answer(
                "❌ Бот работает только с одним видео за сообщение. "
                "Отправь видео по одному (не альбомом)."
            )
            return

    lock = _user_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _user_locks[user_id] = lock

    if lock.locked():
        await message.answer("❌ Подождите, пока обработается предыдущее видео.")
        return

    video = message.video

    if video.file_size is not None and video.file_size >= 8 * 1024 * 1024:
        if user_id:
            metrics_db.log_event(
                user_id,
                "video_rejected",
                message_id=message.message_id,
                video_duration=float(video.duration) if video.duration is not None else None,
                video_file_size=int(video.file_size) if video.file_size is not None else None,
                error="file_size_limit",
            )
        await message.answer("❌ Ошибка: видео должно быть меньше 8 МБ. Пришли другое видео.")
        return

    if video.duration is not None and video.duration > 60:
        if user_id:
            metrics_db.log_event(
                user_id,
                "video_rejected",
                message_id=message.message_id,
                video_duration=float(video.duration),
                video_file_size=int(video.file_size) if video.file_size is not None else None,
                error="duration_limit",
            )
        await message.answer("❌ Я не могу обработать видео больше одной минуты. Пришли другое видео.")
        return

    effect = _user_effect.get(user_id, "normal")

    if effect == "meme" and video.duration is not None and video.duration > 55:
        if user_id:
            metrics_db.log_event(
                user_id,
                "video_rejected",
                message_id=message.message_id,
                effect=effect,
                video_duration=float(video.duration),
                video_file_size=int(video.file_size) if video.file_size is not None else None,
                error="duration_limit_for_meme",
            )
        await message.answer("❌ С эффектом мема видео должно быть до 55 секунд. Пришли другое видео.")
        return

    async with lock:
        if _global_video_lock.locked():
            await message.answer("⏳ Сейчас обрабатывается другое видео. Ты в очереди — подожди немного.")

        async with _global_video_lock:
            await convert_video_to_circle(message, bot, effect)
