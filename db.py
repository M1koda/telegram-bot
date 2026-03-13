import sqlite3
import threading
from datetime import datetime
import pytz

DB_NAME = "support.db"
TZ = pytz.timezone("Europe/Kyiv")

_db_lock = threading.Lock()

_conn = sqlite3.connect(DB_NAME, check_same_thread=False)
_conn.row_factory = sqlite3.Row

# стабильность SQLite
_conn.execute("PRAGMA journal_mode=WAL;")
_conn.execute("PRAGMA synchronous=NORMAL;")
_conn.execute("PRAGMA foreign_keys=ON;")
_conn.execute("PRAGMA busy_timeout=5000;")  # 5s


def now_dt():
    return datetime.now(TZ)


def now_str():
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def _exec(sql, params=(), fetchone=False, fetchall=False):
    with _db_lock:
        cur = _conn.execute(sql, params)
        _conn.commit()
        if fetchone:
            return cur.fetchone()
        if fetchall:
            return cur.fetchall()
        return None


def init_db():
    # users
    _exec("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        phone TEXT,
        created_at TEXT
    )
    """)

    # roles: operator/admin. SUPERADMIN хранится в config, но роль тоже можно иметь.
    _exec("""
    CREATE TABLE IF NOT EXISTS roles (
        user_id INTEGER PRIMARY KEY,
        is_operator INTEGER DEFAULT 0,
        is_admin INTEGER DEFAULT 0,
        updated_at TEXT
    )
    """)

    # chats
    _exec("""
    CREATE TABLE IF NOT EXISTS chats (
        chat_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL,
        status TEXT NOT NULL,                 -- waiting/active/closed
        operator_id INTEGER,
        previous_operator_id INTEGER,
        created_at TEXT,
        closed_at TEXT,
        first_text TEXT,                      -- короткое описание обращения/заявки
        rating INTEGER                        -- 1..5
    )
    """)
    # schema migration: add first_text column to existing chats table if missing
    cols = _exec("PRAGMA table_info(chats)", fetchall=True)
    if cols and "first_text" not in [c["name"] for c in cols]:
        _exec("ALTER TABLE chats ADD COLUMN first_text TEXT")
    if cols and "rating" not in [c["name"] for c in cols]:
        _exec("ALTER TABLE chats ADD COLUMN rating INTEGER")

    # messages
    _exec("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        sender_role TEXT NOT NULL,            -- client/operator/system
        sender_id INTEGER,
        text TEXT,
        created_at TEXT
    )
    """)

    # operator_state: current_chat_id (какой чат открыт в UI прямо сейчас)
    _exec("""
    CREATE TABLE IF NOT EXISTS operator_state (
        operator_id INTEGER PRIMARY KEY,
        current_chat_id INTEGER
    )
    """)

    # stats
    _exec("""
    CREATE TABLE IF NOT EXISTS operator_stats (
        operator_id INTEGER PRIMARY KEY,
        taken_count INTEGER DEFAULT 0,
        closed_count INTEGER DEFAULT 0,
        rating_sum REAL DEFAULT 0,
        rating_count INTEGER DEFAULT 0
    )
    """)
    cols = _exec("PRAGMA table_info(operator_stats)", fetchall=True)
    if cols:
        names = [c["name"] for c in cols]
        if "rating_sum" not in names:
            _exec("ALTER TABLE operator_stats ADD COLUMN rating_sum REAL DEFAULT 0")
        if "rating_count" not in names:
            _exec("ALTER TABLE operator_stats ADD COLUMN rating_count INTEGER DEFAULT 0")


# ---------- users ----------
def user_exists(uid: int) -> bool:
    r = _exec("SELECT 1 FROM users WHERE user_id=?", (uid,), fetchone=True)
    return r is not None


def upsert_user(uid: int, username: str, phone: str):
    _exec(
        "INSERT OR REPLACE INTO users (user_id, username, phone, created_at) VALUES (?,?,?, COALESCE((SELECT created_at FROM users WHERE user_id=?), ?))",
        (uid, username or "—", phone or "—", uid, now_str())
    )


def get_user(uid: int):
    r = _exec("SELECT username, phone FROM users WHERE user_id=?", (uid,), fetchone=True)
    if not r:
        return ("—", "—")
    return (r["username"], r["phone"])


# ---------- roles ----------
def ensure_role_row(uid: int):
    _exec(
        "INSERT OR IGNORE INTO roles (user_id, is_operator, is_admin, updated_at) VALUES (?,?,?,?)",
        (uid, 0, 0, now_str())
    )


def set_operator(uid: int, value: bool):
    ensure_role_row(uid)
    _exec("UPDATE roles SET is_operator=?, updated_at=? WHERE user_id=?", (1 if value else 0, now_str(), uid))


def set_admin(uid: int, value: bool):
    ensure_role_row(uid)
    _exec("UPDATE roles SET is_admin=?, updated_at=? WHERE user_id=?", (1 if value else 0, now_str(), uid))


def is_operator(uid: int) -> bool:
    ensure_role_row(uid)
    r = _exec("SELECT is_operator FROM roles WHERE user_id=?", (uid,), fetchone=True)
    return bool(r["is_operator"])


def is_admin(uid: int) -> bool:
    ensure_role_row(uid)
    r = _exec("SELECT is_admin FROM roles WHERE user_id=?", (uid,), fetchone=True)
    return bool(r["is_admin"])


def list_operators():
    rows = _exec("SELECT user_id FROM roles WHERE is_operator=1 ORDER BY user_id", fetchall=True)
    return [int(r["user_id"]) for r in rows]


def list_admins_and_operators():
    rows = _exec("SELECT user_id FROM roles WHERE is_operator=1 OR is_admin=1", fetchall=True)
    return [int(r["user_id"]) for r in rows]


# ---------- operator state ----------
def set_current_chat(operator_id: int, chat_id: int | None):
    _exec("INSERT OR REPLACE INTO operator_state (operator_id, current_chat_id) VALUES (?,?)", (operator_id, chat_id))


def get_current_chat(operator_id: int):
    r = _exec("SELECT current_chat_id FROM operator_state WHERE operator_id=?", (operator_id,), fetchone=True)
    return int(r["current_chat_id"]) if r and r["current_chat_id"] else None


# ---------- chats ----------
def create_chat(client_id: int, first_text: str):
    _exec(
        "INSERT INTO chats (client_id, status, created_at, first_text) VALUES (?,?,?,?)",
        (client_id, "waiting", now_str(), first_text[:500] if first_text else "")
    )
    r = _exec("SELECT last_insert_rowid() AS id", fetchone=True)
    return int(r["id"])


def take_chat(chat_id: int, operator_id: int) -> bool:
    # берём только waiting
    with _db_lock:
        cur = _conn.execute(
            "UPDATE chats SET status='active', operator_id=? WHERE chat_id=? AND status='waiting'",
            (operator_id, chat_id)
        )
        _conn.commit()
        ok = cur.rowcount == 1
    if ok:
        ensure_stat_row(operator_id)
        _exec("UPDATE operator_stats SET taken_count=taken_count+1 WHERE operator_id=?", (operator_id,))
    return ok


def transfer_chat(chat_id: int, operator_id: int) -> bool:
    with _db_lock:
        cur = _conn.execute("""
            UPDATE chats
            SET status='waiting',
                previous_operator_id=operator_id,
                operator_id=NULL
            WHERE chat_id=? AND status='active' AND operator_id=?
        """, (chat_id, operator_id))
        _conn.commit()
        return cur.rowcount == 1


def close_chat(chat_id: int) -> bool:
    with _db_lock:
        cur = _conn.execute("""
            UPDATE chats
            SET status='closed', closed_at=?
            WHERE chat_id=? AND status='active'
        """, (now_str(), chat_id))
        _conn.commit()
        return cur.rowcount == 1


def get_chat(chat_id: int):
    return _exec("SELECT * FROM chats WHERE chat_id=?", (chat_id,), fetchone=True)


def get_chat_rating(chat_id: int):
    r = _exec("SELECT rating FROM chats WHERE chat_id=?", (chat_id,), fetchone=True)
    if not r:
        return None
    return int(r["rating"]) if r["rating"] is not None else None


def set_chat_rating(chat_id: int, rating: int):
    _exec("UPDATE chats SET rating=? WHERE chat_id=?", (rating, chat_id))


def get_waiting_chats(limit=10, offset=0):
    rows = _exec(
        "SELECT chat_id FROM chats WHERE status='waiting' ORDER BY chat_id DESC LIMIT ? OFFSET ?",
        (limit, offset), fetchall=True
    )
    return [int(r["chat_id"]) for r in rows]


def get_active_chats_all():
    rows = _exec("SELECT chat_id FROM chats WHERE status='active' ORDER BY chat_id DESC", fetchall=True)
    return [int(r["chat_id"]) for r in rows]


def get_my_active_chats(operator_id: int):
    rows = _exec(
        "SELECT chat_id FROM chats WHERE status='active' AND operator_id=? ORDER BY chat_id DESC",
        (operator_id,), fetchall=True
    )
    return [int(r["chat_id"]) for r in rows]


def get_closed_chats(limit=10, offset=0):
    rows = _exec(
        "SELECT chat_id FROM chats WHERE status='closed' ORDER BY chat_id DESC LIMIT ? OFFSET ?",
        (limit, offset), fetchall=True
    )
    return [int(r["chat_id"]) for r in rows]


def get_closed_chats_filtered(mode="all", value=None, operator_q=None, client_q=None, limit=10, offset=0):
    where = ["c.status='closed'"]
    params: list = []

    if mode == "day" and value:
        where.append("substr(c.created_at,1,10)=?")
        params.append(value)
    elif mode == "month" and value:
        where.append("substr(c.created_at,1,7)=?")
        params.append(value)

    if operator_q:
        like = f"%{operator_q}%"
        where.append(
            "(ou.username LIKE ? OR ou.phone LIKE ? OR CAST(ou.user_id AS TEXT) LIKE ?)"
        )
        params.extend([like, like, like])

    if client_q:
        like = f"%{client_q}%"
        where.append(
            "(cu.username LIKE ? OR cu.phone LIKE ? OR CAST(cu.user_id AS TEXT) LIKE ?)"
        )
        params.extend([like, like, like])

    sql = f"""
        SELECT c.chat_id
        FROM chats c
        LEFT JOIN users cu ON cu.user_id = c.client_id
        LEFT JOIN users ou ON ou.user_id = c.operator_id
        WHERE {' AND '.join(where)}
        ORDER BY c.chat_id DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    rows = _exec(sql, params, fetchall=True)
    return [int(r["chat_id"]) for r in rows]


def get_stats_by_period(start_date: str, end_date: str):
    rows = _exec("""
        SELECT c.operator_id AS operator_id,
               COUNT(*) AS closed_count,
               AVG(c.rating) AS rating_avg
        FROM chats c
        WHERE c.status='closed'
          AND c.operator_id IS NOT NULL
          AND substr(c.closed_at,1,10) BETWEEN ? AND ?
        GROUP BY c.operator_id
        ORDER BY closed_count DESC
    """, (start_date, end_date), fetchall=True)
    return rows


def get_closed_chats_by_day(day_iso: str, limit=10, offset=0):
    # day_iso: 'YYYY-MM-DD'
    rows = _exec(
        "SELECT chat_id FROM chats WHERE status='closed' AND substr(created_at,1,10)=? ORDER BY chat_id DESC LIMIT ? OFFSET ?",
        (day_iso, limit, offset), fetchall=True
    )
    return [int(r["chat_id"]) for r in rows]


def get_closed_chats_by_month(month_iso: str, limit=10, offset=0):
    # month_iso: 'YYYY-MM'
    rows = _exec(
        "SELECT chat_id FROM chats WHERE status='closed' AND substr(created_at,1,7)=? ORDER BY chat_id DESC LIMIT ? OFFSET ?",
        (month_iso, limit, offset), fetchall=True
    )
    return [int(r["chat_id"]) for r in rows]


def count_closed():
    r = _exec("SELECT COUNT(*) AS c FROM chats WHERE status='closed'", fetchone=True)
    return int(r["c"])


def count_waiting():
    r = _exec("SELECT COUNT(*) AS c FROM chats WHERE status='waiting'", fetchone=True)
    return int(r["c"])


def count_active():
    r = _exec("SELECT COUNT(*) AS c FROM chats WHERE status='active'", fetchone=True)
    return int(r["c"])


def get_chat_label(chat_id: int, include_closed=False):
    r = _exec("""
        SELECT c.created_at AS c_created, c.closed_at AS c_closed, c.previous_operator_id AS prev,
               c.rating AS rating,
               u.username AS username, u.phone AS phone
        FROM chats c
        JOIN users u ON u.user_id = c.client_id
        WHERE c.chat_id=?
    """, (chat_id,), fetchone=True)

    if not r:
        return "Невідомий чат"

    created = r["c_created"]
    closed = r["c_closed"]
    mark = "🔁 " if r["prev"] else ""
    base = f"{mark}{created[:16]} | {r['username']} | {r['phone']}"
    if include_closed and closed:
        base = f"{mark}{created[:16]} – {closed[:16]} | {r['username']} | {r['phone']}"
    if include_closed and r["rating"] is not None:
        base = f"{base} | ⭐️{int(r['rating'])}"
    return base


# ---------- messages ----------
def add_message(chat_id: int, role: str, sender_id: int | None, text: str):
    _exec(
        "INSERT INTO messages (chat_id, sender_role, sender_id, text, created_at) VALUES (?,?,?,?,?)",
        (chat_id, role, sender_id, text, now_str())
    )


def get_chat_history(chat_id: int):
    rows = _exec("""
        SELECT m.created_at AS t, m.sender_role AS r, m.sender_id AS sid, m.text AS txt,
               u.username AS username, u.phone AS phone
        FROM messages m
        LEFT JOIN users u ON u.user_id = m.sender_id
        WHERE m.chat_id=?
        ORDER BY m.id
    """, (chat_id,), fetchall=True)

    if not rows:
        return "Історія порожня."

    out = []
    for row in rows:
        t = row["t"][:16]
        role = row["r"]
        if role == "client":
            who = "Абонент"
        elif role == "operator":
            who = "Оператор"
        else:
            who = "Система"
        u = row["username"] if row["username"] else "—"
        p = row["phone"] if row["phone"] else "—"
        out.append(f"{t} | {who} {u} | {p}: {row['txt']}")
    return "\n".join(out)


def get_client_chats(client_id: int, limit=10, offset=0):
    rows = _exec("""
        SELECT chat_id FROM chats
        WHERE client_id=? AND status='closed'
        ORDER BY chat_id DESC
        LIMIT ? OFFSET ?
    """, (client_id, limit, offset), fetchall=True)
    return [int(r["chat_id"]) for r in rows]


# ---------- stats ----------
def ensure_stat_row(uid: int):
    _exec(
        "INSERT OR IGNORE INTO operator_stats (operator_id, taken_count, closed_count, rating_sum, rating_count) VALUES (?,?,?,?,?)",
        (uid, 0, 0, 0, 0)
    )


def inc_closed(uid: int):
    ensure_stat_row(uid)
    _exec("UPDATE operator_stats SET closed_count=closed_count+1 WHERE operator_id=?", (uid,))


def get_stats():
    rows = _exec("""
        SELECT *,
               CASE WHEN rating_count > 0 THEN rating_sum * 1.0 / rating_count ELSE NULL END AS rating_avg
        FROM operator_stats
        ORDER BY closed_count DESC, taken_count DESC
    """, fetchall=True)
    return rows


def add_rating(operator_id: int, rating: int):
    ensure_stat_row(operator_id)
    _exec(
        "UPDATE operator_stats SET rating_sum=rating_sum+?, rating_count=rating_count+1 WHERE operator_id=?",
        (rating, operator_id)
    )
