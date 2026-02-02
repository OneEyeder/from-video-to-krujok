import asyncio
import collections
import os
import re
import random
import time
import uuid
from pathlib import Path

from aiogram.types import Message, FSInputFile
from aiogram.exceptions import TelegramBadRequest

import metrics_db


async def get_duration(path: str) -> float:
    cmd = (
        f"ffprobe -v error -show_entries format=duration "
        f"-of default=noprint_wrappers=1:nokey=1 \"{path}\""
    )
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL
    )
    stdout, _ = await process.communicate()
    return float(stdout.decode().strip())


async def has_audio(path: str) -> bool:
    cmd = (
        f"ffprobe -v error -select_streams a:0 -show_entries stream=codec_type "
        f"-of default=noprint_wrappers=1:nokey=1 \"{path}\""
    )
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await process.communicate()
    return bool(stdout.decode().strip())


def progress_bar(percent: int, size: int = 10) -> str:
    filled = int(size * percent / 100)
    return "▓" * filled + "░" * (size - filled)


async def _safe_edit_status(status_msg, text: str) -> None:
    if status_msg is None:
        return
    try:
        await status_msg.edit_text(text)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        raise


def _build_ffmpeg_cmd(input_file: str, output_file: str, duration: float, effect: str, *, with_audio: bool) -> str:
    effective_duration = min(duration, 60.0)
    out_duration = effective_duration
    end_start = max(effective_duration - 2.0, 0.0)
    meme_start = max(effective_duration - 0.5, 0.0)

    base = "crop='min(iw,ih)':'min(iw,ih)',scale=480:480"

    if effect == "speed_slow":
        if effective_duration < 3.0:
            effect = "normal"
        elif not with_audio:
            effect = "normal"
        else:
            seg_len = 1.5
            max_start = max(effective_duration - seg_len, 0.0)

            t1 = random.uniform(0.0, max_start)
            t2 = random.uniform(0.0, max_start)
            if t2 < t1:
                t1, t2 = t2, t1

            if abs(t2 - t1) < seg_len:
                t2 = min(t1 + seg_len, max_start)

            t1_end = min(t1 + seg_len, effective_duration)
            t2_end = min(t2 + seg_len, effective_duration)

            fc = (
                f"[0:v]{base},split=5[v0][v1][v2][v3][v4];"
                f"[v0]trim=0:{t1},setpts=PTS-STARTPTS[v0t];"
                f"[v1]trim={t1}:{t1_end},setpts=(PTS-STARTPTS)/2[v1t];"
                f"[v2]trim={t1_end}:{t2},setpts=PTS-STARTPTS[v2t];"
                f"[v3]trim={t2}:{t2_end},setpts=(PTS-STARTPTS)*2[v3t];"
                f"[v4]trim={t2_end}:{effective_duration},setpts=PTS-STARTPTS[v4t];"
                f"[v0t][v1t][v2t][v3t][v4t]concat=n=5:v=1:a=0[v];"
                f"[0:a]asplit=5[a0][a1][a2][a3][a4];"
                f"[a0]atrim=0:{t1},asetpts=PTS-STARTPTS[a0t];"
                f"[a1]atrim={t1}:{t1_end},asetpts=PTS-STARTPTS,atempo=2[a1t];"
                f"[a2]atrim={t1_end}:{t2},asetpts=PTS-STARTPTS[a2t];"
                f"[a3]atrim={t2}:{t2_end},asetpts=PTS-STARTPTS,atempo=0.5[a3t];"
                f"[a4]atrim={t2_end}:{effective_duration},asetpts=PTS-STARTPTS[a4t];"
                f"[a0t][a1t][a2t][a3t][a4t]concat=n=5:v=0:a=1[a]"
            )
            return (
                f"ffmpeg -y -i \"{input_file}\" "
                f"-filter_complex \"{fc}\" -map \"[v]\" -map \"[a]\" "
                f"-t {out_duration} -c:v libx264 -preset veryfast -crf 23 -c:a aac -b:a 128k \"{output_file}\""
            )

    if effect == "flash":
        if not with_audio:
            effect = "normal"
        else:
            flash_len = 3.0
            flash_max_start = max(effective_duration - flash_len, 0.0)
            flash_start = random.uniform(0.0, flash_max_start) if flash_max_start > 0 else 0.0
            flash_end = min(flash_start + flash_len, effective_duration)

            flash_file = Path(__file__).resolve().parent / "flesh-bang.mp4"
            if not flash_file.exists():
                return _build_ffmpeg_cmd(input_file, output_file, duration, "normal", with_audio=with_audio)

            flash_file_str = str(flash_file)

            # Overlay only video from the flash clip; keep original audio.
            fc = (
                f"[0:v]{base},trim=0:{effective_duration},setpts=PTS-STARTPTS[v0];"
                f"[1:v]{base},trim=0:{flash_len},setpts=PTS-STARTPTS[fv];"
                f"[v0][fv]overlay=0:0:enable='between(t,{flash_start},{flash_end})'[v]"
            )

            return (
                f"ffmpeg -y -i \"{input_file}\" -stream_loop -1 -i \"{flash_file_str}\" "
                f"-filter_complex \"{fc}\" -map \"[v]\" -map 0:a? "
                f"-t {out_duration} -c:v libx264 -preset veryfast -crf 23 -c:a aac -b:a 128k \"{output_file}\""
            )

    if effect == "speed":
        if not with_audio:
            effect = "normal"
        else:
            fc = (
                f"[0:v]{base},split=2[v0][v1];"
                f"[v0]trim=0:{end_start},setpts=PTS-STARTPTS[v0t];"
                f"[v1]trim={end_start}:{effective_duration},setpts=(PTS-STARTPTS)/2[v1t];"
                f"[v0t][v1t]concat=n=2:v=1:a=0[v];"
                f"[0:a]asplit=2[a0][a1];"
                f"[a0]atrim=0:{end_start},asetpts=PTS-STARTPTS[a0t];"
                f"[a1]atrim={end_start}:{effective_duration},asetpts=PTS-STARTPTS,atempo=2[a1t];"
                f"[a0t][a1t]concat=n=2:v=0:a=1[a]"
            )
            return (
                f"ffmpeg -y -i \"{input_file}\" "
                f"-filter_complex \"{fc}\" -map \"[v]\" -map \"[a]\" "
                f"-t {out_duration} -c:v libx264 -preset veryfast -crf 23 -c:a aac -b:a 128k \"{output_file}\""
            )

    if effect == "meme":
        raise RuntimeError("meme effect must be handled by _build_meme_insert_cmd")

    vf = base
    af = ""

    if effect == "echo":
        af = "-af \"aecho=0.8:0.9:1000|1800:0.35|0.25\" "
    elif effect == "shake":
        vf = (
            f"{vf},rotate=0.04*sin(60*t):c=black@0:enable='gte(t,{end_start})',"
            f"gblur=sigma=8:steps=2:enable='gte(t,{end_start})'"
        )

    audio_part = ""
    if with_audio:
        audio_part = f"{af}-c:a aac -b:a 128k "
    else:
        audio_part = "-an "

    return (
        f"ffmpeg -y -i \"{input_file}\" "
        f"-vf \"{vf}\" "
        f"-t {out_duration} "
        f"-c:v libx264 -preset veryfast -crf 23 "
        f"{audio_part}"
        f"\"{output_file}\""
    )


def _get_memes_dir() -> Path | None:
    project_memes_dir = Path(__file__).resolve().parent / "memes"
    if project_memes_dir.exists():
        return project_memes_dir

    fallback_memes_dir = Path.home() / "Desktop" / "memes"
    if fallback_memes_dir.exists():
        return fallback_memes_dir

    return None


def _build_meme_insert_cmd(input_file: str, output_file: str, duration: float, *, with_audio: bool) -> str:
    effective_duration = min(duration, 60.0)
    meme_len = 5.0
    out_duration = min(60.0, effective_duration + meme_len)
    if not with_audio:
        return _build_ffmpeg_cmd(input_file, output_file, duration, "normal", with_audio=False)

    memes_dir = _get_memes_dir()
    meme_files = sorted(memes_dir.glob("*.mp4")) if memes_dir else []
    if not meme_files:
        return _build_ffmpeg_cmd(input_file, output_file, duration, "normal", with_audio=with_audio)

    meme_file = str(random.choice(meme_files))

    insert_at = random.uniform(0.0, effective_duration) if effective_duration > 0 else 0.0

    base = "crop='min(iw,ih)':'min(iw,ih)',scale=480:480"

    # Insert meme segment (5s) into the video/audio timeline => output duration = original + 5s.
    # Note: expects audio tracks to exist (0:a and 1:a). If a meme has no audio, ffmpeg may fail.
    fc = (
        f"[0:v]{base},trim=0:{effective_duration},setpts=PTS-STARTPTS[v0];"
        f"[1:v]{base},trim=0:{meme_len},setpts=PTS-STARTPTS[mv];"
        f"[v0]split=2[vpre][vpost];"
        f"[vpre]trim=0:{insert_at},setpts=PTS-STARTPTS[vpre_t];"
        f"[vpost]trim={insert_at}:{effective_duration},setpts=PTS-STARTPTS[vpost_t];"
        f"[vpre_t][mv][vpost_t]concat=n=3:v=1:a=0[v];"
        f"[0:a]atrim=0:{effective_duration},asetpts=PTS-STARTPTS[a0];"
        f"[1:a]atrim=0:{meme_len},asetpts=PTS-STARTPTS[ma];"
        f"[a0]asplit=2[apre][apost];"
        f"[apre]atrim=0:{insert_at},asetpts=PTS-STARTPTS[apre_t];"
        f"[apost]atrim={insert_at}:{effective_duration},asetpts=PTS-STARTPTS[apost_t];"
        f"[apre_t][ma][apost_t]concat=n=3:v=0:a=1[a]"
    )

    return (
        f"ffmpeg -y -i \"{input_file}\" -stream_loop -1 -i \"{meme_file}\" "
        f"-filter_complex \"{fc}\" -map \"[v]\" -map \"[a]\" "
        f"-t {out_duration} -c:v libx264 -preset veryfast -crf 23 -c:a aac -b:a 128k \"{output_file}\""
    )


async def convert_video_to_circle(message: Message, bot, effect: str = "normal") -> None:
    video = message.video

    user_id = message.from_user.id if message.from_user else 0
    if user_id:
        metrics_db.log_event(
            user_id,
            "video_start",
            message_id=message.message_id,
            effect=effect,
            video_duration=float(video.duration) if video.duration is not None else None,
            video_file_size=int(video.file_size) if video.file_size is not None else None,
        )

    input_file = f"input_{uuid.uuid4()}.mp4"
    output_file = f"circle_{uuid.uuid4()}.mp4"

    status_msg = None
    try:
        # 1) скачиваем видео
        await bot.download(video.file_id, destination=input_file)

        # 2) узнаём длительность
        duration = await get_duration(input_file)

        with_audio = await has_audio(input_file)

        # 3) статус-сообщение
        status_msg = await message.answer(
            "⏳ Обрабатываю видео…\n"
            "░░░░░░░░░░ 0%"
        )

        # 4) ffmpeg команда
        if effect == "meme":
            cmd = _build_meme_insert_cmd(input_file, output_file, duration, with_audio=with_audio)
        else:
            cmd = _build_ffmpeg_cmd(input_file, output_file, duration, effect, with_audio=with_audio)

        # 5) запускаем ffmpeg и читаем прогресс
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )

        time_regex = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")
        last_update = 0

        start_time = time.time()

        stderr_tail: collections.deque[str] = collections.deque(maxlen=200)

        while True:
            if time.time() - start_time > 300:
                process.kill()
                await process.wait()
                await _safe_edit_status(status_msg, "❌ Обработка заняла больше 5 минут. Пришли другое видео.")
                if user_id:
                    metrics_db.log_event(
                        user_id,
                        "video_error",
                        message_id=message.message_id,
                        effect=effect,
                        video_duration=float(video.duration) if video.duration is not None else None,
                        video_file_size=int(video.file_size) if video.file_size is not None else None,
                        error="timeout_5m",
                    )
                return

            try:
                line = await asyncio.wait_for(process.stderr.readline(), timeout=1)
            except asyncio.TimeoutError:
                continue

            if not line:
                break

            decoded = line.decode(errors="replace")
            stderr_tail.append(decoded.strip())

            match = time_regex.search(decoded)
            if match:
                h, m, s = match.groups()
                current = int(h) * 3600 + int(m) * 60 + float(s)
                percent = min(int(current / duration * 100), 100)

                now = time.time()
                if now - last_update >= 1:  # обновление не чаще 1 раза в секунду
                    bar = progress_bar(percent)
                    await _safe_edit_status(status_msg, f"⏳ Обрабатываю видео…\n{bar} {percent}%")
                    last_update = now

        await process.wait()

        # 6) если ошибка — пишем пользователю
        if process.returncode != 0:
            await _safe_edit_status(status_msg, "❌ Ошибка обработки видео")
            if user_id:
                tail = "\n".join([t for t in stderr_tail if t])
                print("ffmpeg failed", process.returncode)
                print("ffmpeg cmd:", cmd)
                if tail:
                    print("ffmpeg stderr tail:\n" + tail)
                metrics_db.log_event(
                    user_id,
                    "video_error",
                    message_id=message.message_id,
                    effect=effect,
                    video_duration=float(video.duration) if video.duration is not None else None,
                    video_file_size=int(video.file_size) if video.file_size is not None else None,
                    error=("ffmpeg_nonzero_returncode\n" + tail)[-2000:],
                )
            return

        # 7) отправляем кружок
        await message.answer_video_note(FSInputFile(output_file))

        if user_id:
            metrics_db.log_event(
                user_id,
                "video_success",
                message_id=message.message_id,
                effect=effect,
                video_duration=float(video.duration) if video.duration is not None else None,
                video_file_size=int(video.file_size) if video.file_size is not None else None,
            )

        # 8) обновляем статус
        await _safe_edit_status(status_msg, "✅ Готово! Вот твой кружок ⭕️")

    except Exception as e:
        if user_id:
            metrics_db.log_event(
                user_id,
                "video_error",
                message_id=message.message_id,
                effect=effect,
                video_duration=float(video.duration) if video.duration is not None else None,
                video_file_size=int(video.file_size) if video.file_size is not None else None,
                error=str(e)[:500],
            )
        if status_msg is not None:
            await _safe_edit_status(status_msg, "❌ Ошибка обработки видео")
        return
    finally:
        # 9) удаляем временные файлы
        if os.path.exists(input_file):
            os.remove(input_file)
        if os.path.exists(output_file):
            os.remove(output_file)
