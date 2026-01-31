import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras


_DB_LOCK = threading.Lock()


@dataclass(frozen=True)
class TgUserInfo:
    user_id: int
    username: str | None
    full_name: str | None


def _default_db_path() -> str:
    return str(Path(__file__).resolve().parent / "metrics.sqlite3")


def _pg_dsn() -> str | None:
    dsn = os.getenv("DATABASE_URL", "").strip()
    return dsn or None


def _is_postgres() -> bool:
    return _pg_dsn() is not None


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _pg_connect():
    dsn = _pg_dsn()
    if dsn is None:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(dsn)


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return {k: row[k] for k in row.keys()}


def init_db(db_path: str | None = None) -> None:
    db = db_path or _default_db_path()
    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        full_name TEXT,
                        first_seen_ts BIGINT NOT NULL,
                        last_seen_ts BIGINT NOT NULL,
                        is_banned BOOLEAN NOT NULL DEFAULT FALSE
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id BIGSERIAL PRIMARY KEY,
                        ts BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        event TEXT NOT NULL,
                        message_id BIGINT,
                        effect TEXT,
                        video_duration DOUBLE PRECISION,
                        video_file_size BIGINT,
                        error TEXT
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts)")
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        username TEXT,
                        full_name TEXT,
                        first_seen_ts INTEGER NOT NULL,
                        last_seen_ts INTEGER NOT NULL,
                        is_banned INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts INTEGER NOT NULL,
                        user_id INTEGER NOT NULL,
                        event TEXT NOT NULL,
                        message_id INTEGER,
                        effect TEXT,
                        video_duration REAL,
                        video_file_size INTEGER,
                        error TEXT
                    )
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events(user_id, ts)")
                conn.commit()
            finally:
                conn.close()


def upsert_user_seen(user: TgUserInfo, ts: int | None = None, db_path: str | None = None) -> None:
    db = db_path or _default_db_path()
    now_ts = int(ts or time.time())

    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO users(user_id, username, full_name, first_seen_ts, last_seen_ts, is_banned)
                    VALUES(%s, %s, %s, %s, %s, FALSE)
                    ON CONFLICT (user_id) DO UPDATE
                    SET username = EXCLUDED.username,
                        full_name = EXCLUDED.full_name,
                        last_seen_ts = EXCLUDED.last_seen_ts
                    """,
                    (user.user_id, user.username, user.full_name, now_ts, now_ts),
                )
                cur.execute(
                    "INSERT INTO events(ts, user_id, event) VALUES(%s, %s, 'user_seen')",
                    (now_ts, user.user_id),
                )
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                row = conn.execute("SELECT user_id FROM users WHERE user_id = ?", (user.user_id,)).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO users(user_id, username, full_name, first_seen_ts, last_seen_ts, is_banned)
                        VALUES(?, ?, ?, ?, ?, 0)
                        """,
                        (user.user_id, user.username, user.full_name, now_ts, now_ts),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE users
                        SET username = ?, full_name = ?, last_seen_ts = ?
                        WHERE user_id = ?
                        """,
                        (user.username, user.full_name, now_ts, user.user_id),
                    )
                conn.execute(
                    """
                    INSERT INTO events(ts, user_id, event)
                    VALUES(?, ?, 'user_seen')
                    """,
                    (now_ts, user.user_id),
                )
                conn.commit()
            finally:
                conn.close()


def log_event(
    user_id: int,
    event: str,
    *,
    message_id: int | None = None,
    effect: str | None = None,
    video_duration: float | None = None,
    video_file_size: int | None = None,
    error: str | None = None,
    ts: int | None = None,
    db_path: str | None = None,
) -> None:
    db = db_path or _default_db_path()
    now_ts = int(ts or time.time())

    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO events(ts, user_id, event, message_id, effect, video_duration, video_file_size, error)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (now_ts, user_id, event, message_id, effect, video_duration, video_file_size, error),
                )
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                conn.execute(
                    """
                    INSERT INTO events(ts, user_id, event, message_id, effect, video_duration, video_file_size, error)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (now_ts, user_id, event, message_id, effect, video_duration, video_file_size, error),
                )
                conn.commit()
            finally:
                conn.close()


def is_banned(user_id: int, db_path: str | None = None) -> bool:
    db = db_path or _default_db_path()
    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT is_banned FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                if row is None:
                    return False
                return bool(row["is_banned"])
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                row = conn.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,)).fetchone()
                if row is None:
                    return False
                return bool(row["is_banned"])
            finally:
                conn.close()


def set_banned(user_id: int, banned: bool, db_path: str | None = None) -> None:
    db = db_path or _default_db_path()
    now_ts = int(time.time())
    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO users(user_id, username, full_name, first_seen_ts, last_seen_ts, is_banned)
                    VALUES(%s, NULL, NULL, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET is_banned = EXCLUDED.is_banned,
                        last_seen_ts = GREATEST(users.last_seen_ts, EXCLUDED.last_seen_ts)
                    """,
                    (user_id, now_ts, now_ts, banned),
                )
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                row = conn.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO users(user_id, username, full_name, first_seen_ts, last_seen_ts, is_banned)
                        VALUES(?, NULL, NULL, ?, ?, ?)
                        """,
                        (user_id, now_ts, now_ts, 1 if banned else 0),
                    )
                else:
                    conn.execute(
                        "UPDATE users SET is_banned = ? WHERE user_id = ?",
                        (1 if banned else 0, user_id),
                    )
                conn.commit()
            finally:
                conn.close()


def _day_bounds_ts_local(day: datetime) -> tuple[int, int]:
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    return int(start.timestamp()), int(end.timestamp())


def stats_today(db_path: str | None = None) -> dict[str, int]:
    db = db_path or _default_db_path()
    today = datetime.now()
    start_ts, end_ts = _day_bounds_ts_local(today)

    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT COUNT(*) AS c FROM users")
                total_users = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM users WHERE first_seen_ts BETWEEN %s AND %s", (start_ts, end_ts))
                new_users = cur.fetchone()["c"]
                cur.execute(
                    "SELECT COUNT(DISTINCT user_id) AS c FROM events WHERE event = 'user_seen' AND ts BETWEEN %s AND %s",
                    (start_ts, end_ts),
                )
                active_users = cur.fetchone()["c"]
                cur.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE event = 'video_start' AND ts BETWEEN %s AND %s",
                    (start_ts, end_ts),
                )
                videos_started = cur.fetchone()["c"]
                cur.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE event = 'video_success' AND ts BETWEEN %s AND %s",
                    (start_ts, end_ts),
                )
                videos_success = cur.fetchone()["c"]
                cur.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE event = 'video_error' AND ts BETWEEN %s AND %s",
                    (start_ts, end_ts),
                )
                videos_error = cur.fetchone()["c"]

                return {
                    "total_users": int(total_users),
                    "new_users_today": int(new_users),
                    "active_users_today": int(active_users),
                    "videos_started_today": int(videos_started),
                    "videos_success_today": int(videos_success),
                    "videos_error_today": int(videos_error),
                }
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                total_users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
                new_users = conn.execute(
                    "SELECT COUNT(*) AS c FROM users WHERE first_seen_ts BETWEEN ? AND ?",
                    (start_ts, end_ts),
                ).fetchone()["c"]
                active_users = conn.execute(
                    "SELECT COUNT(DISTINCT user_id) AS c FROM events WHERE event = 'user_seen' AND ts BETWEEN ? AND ?",
                    (start_ts, end_ts),
                ).fetchone()["c"]
                videos_started = conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE event = 'video_start' AND ts BETWEEN ? AND ?",
                    (start_ts, end_ts),
                ).fetchone()["c"]
                videos_success = conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE event = 'video_success' AND ts BETWEEN ? AND ?",
                    (start_ts, end_ts),
                ).fetchone()["c"]
                videos_error = conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE event = 'video_error' AND ts BETWEEN ? AND ?",
                    (start_ts, end_ts),
                ).fetchone()["c"]

                return {
                    "total_users": int(total_users),
                    "new_users_today": int(new_users),
                    "active_users_today": int(active_users),
                    "videos_started_today": int(videos_started),
                    "videos_success_today": int(videos_success),
                    "videos_error_today": int(videos_error),
                }
            finally:
                conn.close()


def users_today(limit: int = 50, db_path: str | None = None) -> list[dict]:
    db = db_path or _default_db_path()
    today = datetime.now()
    start_ts, end_ts = _day_bounds_ts_local(today)

    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT user_id, username, full_name, first_seen_ts, last_seen_ts
                    FROM users
                    WHERE first_seen_ts BETWEEN %s AND %s
                    ORDER BY first_seen_ts DESC
                    LIMIT %s
                    """,
                    (start_ts, end_ts, limit),
                )
                return [dict(r) for r in cur.fetchall()]
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                rows = conn.execute(
                    """
                    SELECT user_id, username, full_name, first_seen_ts, last_seen_ts
                    FROM users
                    WHERE first_seen_ts BETWEEN ? AND ?
                    ORDER BY first_seen_ts DESC
                    LIMIT ?
                    """,
                    (start_ts, end_ts, limit),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()


def videos_today(limit: int = 50, db_path: str | None = None) -> list[dict]:
    db = db_path or _default_db_path()
    today = datetime.now()
    start_ts, end_ts = _day_bounds_ts_local(today)

    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT ts, user_id, event, effect, video_duration, video_file_size, error, message_id
                    FROM events
                    WHERE event IN ('video_start', 'video_success', 'video_error')
                      AND ts BETWEEN %s AND %s
                    ORDER BY ts DESC
                    LIMIT %s
                    """,
                    (start_ts, end_ts, limit),
                )
                return [dict(r) for r in cur.fetchall()]
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                rows = conn.execute(
                    """
                    SELECT ts, user_id, event, effect, video_duration, video_file_size, error, message_id
                    FROM events
                    WHERE event IN ('video_start', 'video_success', 'video_error')
                      AND ts BETWEEN ? AND ?
                    ORDER BY ts DESC
                    LIMIT ?
                    """,
                    (start_ts, end_ts, limit),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()


def user_card(user_id: int, db_path: str | None = None) -> dict[str, object] | None:
    db = db_path or _default_db_path()

    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT user_id, username, full_name, first_seen_ts, last_seen_ts, is_banned
                    FROM users
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                u = cur.fetchone()
                if u is None:
                    return None

                cur.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE user_id = %s AND event = 'video_success'",
                    (user_id,),
                )
                video_success = cur.fetchone()["c"]
                cur.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE user_id = %s AND event = 'video_error'",
                    (user_id,),
                )
                video_error = cur.fetchone()["c"]

                return {
                    "user_id": int(u["user_id"]),
                    "username": u["username"],
                    "full_name": u["full_name"],
                    "first_seen_ts": int(u["first_seen_ts"]),
                    "last_seen_ts": int(u["last_seen_ts"]),
                    "is_banned": bool(u["is_banned"]),
                    "videos_success": int(video_success),
                    "videos_error": int(video_error),
                }
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                u = conn.execute(
                    """
                    SELECT user_id, username, full_name, first_seen_ts, last_seen_ts, is_banned
                    FROM users
                    WHERE user_id = ?
                    """,
                    (user_id,),
                ).fetchone()
                if u is None:
                    return None

                video_success = conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE user_id = ? AND event = 'video_success'",
                    (user_id,),
                ).fetchone()["c"]
                video_error = conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE user_id = ? AND event = 'video_error'",
                    (user_id,),
                ).fetchone()["c"]

                return {
                    "user_id": int(u["user_id"]),
                    "username": u["username"],
                    "full_name": u["full_name"],
                    "first_seen_ts": int(u["first_seen_ts"]),
                    "last_seen_ts": int(u["last_seen_ts"]),
                    "is_banned": bool(u["is_banned"]),
                    "videos_success": int(video_success),
                    "videos_error": int(video_error),
                }
            finally:
                conn.close()


def banned_users(limit: int = 50, db_path: str | None = None) -> list[dict]:
    db = db_path or _default_db_path()
    with _DB_LOCK:
        if _is_postgres():
            conn = _pg_connect()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT user_id, username, full_name, last_seen_ts
                    FROM users
                    WHERE is_banned = TRUE
                    ORDER BY last_seen_ts DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return [dict(r) for r in cur.fetchall()]
            finally:
                conn.close()
        else:
            conn = _connect(db)
            try:
                rows = conn.execute(
                    """
                    SELECT user_id, username, full_name, last_seen_ts
                    FROM users
                    WHERE is_banned = 1
                    ORDER BY last_seen_ts DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()
