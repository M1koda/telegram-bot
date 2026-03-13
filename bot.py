import time
import re
from config import SUPERADMIN_ID, CALL_REQUESTS_CHANNEL_ID
from telegram_api import (
    get_updates, send_message, edit_message, answer_callback,
    kb_client_main, kb_operator_main, kb_admin_main, kb_admin_start,
    kb_operator_in_chat, kb_client_in_chat, kb_cancel,
    kb_inline, ibtn, kb_reg_request, kb_yes_no_free, kb_contact_format,
    kb_geo_request, kb_remove, kb_rating
)
from db import (
    init_db,
    user_exists, upsert_user, get_user,
    is_operator, is_admin, set_operator, set_admin,
    list_operators, list_admins_and_operators,
    set_current_chat, get_current_chat,
    create_chat, take_chat, transfer_chat, close_chat, get_chat,
    add_message, get_chat_history, get_chat_label,
    get_waiting_chats, get_my_active_chats, get_active_chats_all,
    get_closed_chats, get_closed_chats_by_day, get_closed_chats_by_month,
    count_closed, count_waiting, count_active,
    get_client_chats,
    inc_closed, get_stats, add_rating, get_closed_chats_filtered,
    get_chat_rating, set_chat_rating, get_stats_by_period
)

# -------- In-memory FSM/state --------

# регистрация клиента
REG_STATE = {}  # uid -> "need_contact"

# клиент создаёт обращение в поддержку (сначала текст, потом create_chat)
SUPPORT_DRAFT = {}  # uid -> True/False

# анкета подключения
CONNECT = {}  # uid -> {"step":..., "data":...}

# создание чата оператором
OP_CREATE = {}  # operator_id -> {"step":..., "target":..., "text":...}

# режим админа: выбирает админ/оператор
ADMIN_MODE = {}  # uid -> "choose"/"admin"/"operator"

# пагинация архива
ARCHIVE_PAGE = {}  # uid -> {"mode":"all/day/month", "value":..., "page":0}

# просмотр архива абонента в чате
CLIENT_ARCH_PAGE = {}  # uid -> {"client_id":..., "page":0}

# ожидание ввода username/id при добавлении/удалении оператора
ADMIN_INPUT = {}  # uid -> {"action":"add_op"/"del_op"}

# ожидание оценки оператора после закрытия чата
RATING_PENDING = {}  # client_id -> {"operator_id":..., "chat_id":...}

# ожидание ввода периода для статистики
STATS_FILTER = {}  # admin_id -> True


# -------- Helpers --------

def fmt_user(uid: int) -> str:
    u, p = get_user(uid)
    return f"{u} | {p}"


def is_superadmin(uid: int) -> bool:
    return uid == SUPERADMIN_ID


def ensure_superadmin(uid: int):
    if is_superadmin(uid):
        set_admin(uid, True)
        set_operator(uid, True)


def send_main_menu(uid: int):
    # если оператор/админ — показываем панель, иначе меню клиента
    if is_admin(uid):
        ADMIN_MODE.setdefault(uid, "choose")
        send_message(uid, "Оберіть режим:", kb_admin_start())
        return
    if is_operator(uid):
        send_message(uid, "Панель оператора:", kb_operator_main(is_admin(uid)))
        return
    send_message(uid, "Головне меню:", kb_client_main())


def notify_new_message(chat_id: int, client_id: int, text: str):
    """
    FIX п.1: оператору, который прямо сейчас в этом чате (current_chat_id == chat_id),
    НЕ шлём отдельное уведомление "📣 Нове повідомлення". Ему идёт только обычное сообщение.
    """
    userline = fmt_user(client_id)

    targets = list_admins_and_operators()
    for op_id in targets:
        cur = get_current_chat(op_id)
        if cur == chat_id:
            # оператор уже в чате — шлём только “строка: текст”
            send_message(op_id, f"{userline}: {text}", kb_operator_in_chat())
        else:
            # не в чате — шлём уведомление+текст
            send_message(op_id, f"📣 Нове повідомлення\n{userline}\n{text}", kb_operator_main(is_admin(op_id)))


def notify_new_chat(chat_id: int):
    ch = get_chat(chat_id)
    if not ch:
        return
    client_id = int(ch["client_id"])
    label = get_chat_label(chat_id)
    targets = list_admins_and_operators()
    for op_id in targets:
        send_message(op_id, f"🆕 Нове звернення\n{label}", kb_operator_main(is_admin(op_id)))


def notify_transfer(chat_id: int, from_operator: int):
    ch = get_chat(chat_id)
    if not ch:
        return
    op_u, _ = get_user(from_operator)
    label = get_chat_label(chat_id)
    targets = list_admins_and_operators()
    for op_id in targets:
        if op_id == from_operator:
            continue
        send_message(op_id, f"🔁 Оператор {op_u} передав чат.\n{label}\nВізьміть в обробку.", kb_operator_main(is_admin(op_id)))


def parse_day_or_month(s: str):
    s = s.strip()
    m_day = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s)
    if m_day:
        dd, mm, yyyy = m_day.group(1), m_day.group(2), m_day.group(3)
        return ("day", f"{yyyy}-{mm}-{dd}")
    m_month = re.fullmatch(r"(\d{2})\.(\d{4})", s)
    if m_month:
        mm, yyyy = m_month.group(1), m_month.group(2)
        return ("month", f"{yyyy}-{mm}")
    return (None, None)


def parse_stats_period(s: str):
    s = s.strip()
    if s.lower() in ("all", "все"):
        return ("all", None, None)

    m_range = re.fullmatch(r"(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{4})", s)
    if m_range:
        mm1, yy1, mm2, yy2 = int(m_range.group(1)), int(m_range.group(2)), int(m_range.group(3)), int(m_range.group(4))
        start = f"{yy1:04d}-{mm1:02d}-01"
        end = f"{yy2:04d}-{mm2:02d}-01"
        end = _last_day_of_month(end)
        if start > end:
            start, end = end, start
        return ("range", start, end)

    d_range = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{2})\.(\d{4})", s)
    if d_range:
        d1, m1, y1 = int(d_range.group(1)), int(d_range.group(2)), int(d_range.group(3))
        d2, m2, y2 = int(d_range.group(4)), int(d_range.group(5)), int(d_range.group(6))
        start = f"{y1:04d}-{m1:02d}-{d1:02d}"
        end = f"{y2:04d}-{m2:02d}-{d2:02d}"
        if start > end:
            start, end = end, start
        return ("range", start, end)

    m_one = re.fullmatch(r"(\d{2})\.(\d{4})", s)
    if m_one:
        mm, yy = int(m_one.group(1)), int(m_one.group(2))
        start = f"{yy:04d}-{mm:02d}-01"
        end = _last_day_of_month(start)
        return ("range", start, end)

    d_one = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s)
    if d_one:
        dd, mm, yy = int(d_one.group(1)), int(d_one.group(2)), int(d_one.group(3))
        date = f"{yy:04d}-{mm:02d}-{dd:02d}"
        return ("range", date, date)

    y_one = re.fullmatch(r"(\d{4})", s)
    if y_one:
        yy = int(y_one.group(1))
        return ("range", f"{yy:04d}-01-01", f"{yy:04d}-12-31")

    return (None, None, None)


def _last_day_of_month(start_iso: str) -> str:
    from datetime import datetime, timedelta
    dt = datetime.strptime(start_iso, "%Y-%m-%d")
    if dt.month == 12:
        next_month = datetime(dt.year + 1, 1, 1)
    else:
        next_month = datetime(dt.year, dt.month + 1, 1)
    last = next_month - timedelta(days=1)
    return last.strftime("%Y-%m-%d")


def kb_inline_chats(chat_ids, action_prefix, include_closed=False, page_btns=None):
    rows = []
    for cid in chat_ids:
        rows.append([ibtn(get_chat_label(cid, include_closed=include_closed), f"{action_prefix}:{cid}")])
    if page_btns:
        rows.append(page_btns)
    rows.append([ibtn("⬅️ Назад", f"{action_prefix}:back")])
    return kb_inline(rows)


# -------- Connection request (п.3 анкета) --------

def start_connect(uid: int):
    CONNECT[uid] = {"step": 1, "data": {}}
    # показываем кнопку отмены снизу, а выбор — инлайном
    send_message(uid, "Для скасування використайте кнопку нижче.", kb_cancel())
    send_message(uid,
        "Якого типу приміщення бажаєте підключити?",
        kb_inline([
            [ibtn("🏠 Приватний будинок", "conn:type:house")],
            [ibtn("🏢 Квартира", "conn:type:flat")],
            [ibtn("🏭 Нежитлове приміщення", "conn:type:biz")]
        ])
    )


def connect_ask_tariff(uid: int, place_type: str):
    if place_type == "flat":
        send_message(uid, "Який тарифний план Ви бажаєте? (оберіть з кнопок)",
            kb_inline([
                [ibtn("Unlim-299 - 100 Мбіт/с.", "conn:tariff:unlim299")],
                [ibtn("Unlim-399 - 1000 Мбіт/с.", "conn:tariff:unlim399")]
            ])
        )
    elif place_type == "house":
        send_message(uid, "Який тарифний план Ви бажаєте? (оберіть з кнопок)",
            kb_inline([
                [ibtn("Private-299 - 100 Мбіт/с.", "conn:tariff:priv299")],
                [ibtn("Private-399 - 1000 Мбіт/с.", "conn:tariff:priv399")]
            ])
        )
    else:
        # biz: пропускаем тариф
        CONNECT[uid]["step"] = 5
        send_message(uid, "Чи є у Вас роутер? (можете обрати або написати свій варіант)", kb_yes_no_free())


def build_connect_text(uid: int, data: dict) -> str:
    username, phone = get_user(uid)
    lines = ["#ЗаявкаНаПідключення", username, phone, ""]
    idx = 1

    def add(q, a):
        nonlocal idx
        lines.append(f"{idx}. {q}\n   ➜ {a}")
        idx += 1

    t = data.get("place")
    place_label = {"flat": "Квартира", "house": "Приватний будинок", "biz": "Нежитлове приміщення"}.get(t, "—")
    add("Тип приміщення", place_label)
    add("Адреса підключення", data.get("address", "—"))

    if t in ("house", "biz"):
        add("Геолокація", data.get("geo", "—"))

    if t == "biz":
        add("Тип підключення", data.get("person_type", "—"))
        if data.get("person_type") == "Юридична":
            add("Email", data.get("email", "—"))

    if t == "flat":
        add("Тариф", data.get("tariff", "—"))
    if t == "house":
        add("Тариф", data.get("tariff", "—"))

    add("Роутер", data.get("router", "—"))
    add("Телебачення", data.get("tv", "—"))
    add("Формат звʼязку", data.get("contact_format", "—"))

    return "\n".join(lines)


def finish_connect(uid: int, data: dict):
    # формат связи
    fmt = data.get("contact_format")
    text = build_connect_text(uid, data)

    if fmt == "💬 Продовжити тут":
        # создать чат в поддержку и отправить текст как первое сообщение
        cid = create_chat(uid, first_text="Заявка на підключення")
        add_message(cid, "client", uid, text)
        notify_new_chat(cid)
        send_message(uid, "✅ Дякуємо! Заявку створено. Оператор скоро відповість.", kb_client_in_chat())
    else:
        # звонок — в канал
        send_message(CALL_REQUESTS_CHANNEL_ID, text, reply_markup=None)
        send_message(uid, "✅ Дякуємо! Ми звʼяжемось з Вами телефоном.", kb_client_main())


# -------- Support flow --------

def start_support(uid: int):
    existing = find_latest_open_chat_for_client(uid)
    if existing:
        send_message(uid, "У вас вже є активний чат. Напишіть повідомлення тут.", kb_client_in_chat())
        return
    SUPPORT_DRAFT[uid] = True
    send_message(uid, "Опишіть, будь ласка, суть звернення одним повідомленням:", kb_cancel())


def create_support_chat(uid: int, text: str):
    if not text or not text.strip():
        SUPPORT_DRAFT[uid] = True
        send_message(uid, "Будь ласка, надішліть текстове повідомлення.", kb_cancel())
        return
    cid = create_chat(uid, first_text=text)
    add_message(cid, "client", uid, text)
    notify_new_chat(cid)
    send_message(uid, "✅ Звернення створено. Оператор скоро відповість.", kb_client_in_chat())


# -------- Operator/admin screens --------

def show_waiting(uid: int, page: int = 0):
    limit = 10
    offset = page * limit
    chats = get_waiting_chats(limit=limit, offset=offset)
    btns = []
    if page > 0:
        btns.append(ibtn("⬅️ Попередні", f"wait:page:{page-1}"))
    if len(chats) == limit:
        btns.append(ibtn("➡️ Наступні", f"wait:page:{page+1}"))
    page_btns = [btns] if btns else None

    send_message(uid, "⏳ Очікуючі чати:", reply_markup=kb_inline_chats(chats, "wait_take", page_btns=page_btns))


def show_active(uid: int):
    if is_admin(uid):
        chats = get_active_chats_all()
    else:
        chats = get_my_active_chats(uid)
    send_message(uid, "🟢 Активні чати:", reply_markup=kb_inline_chats(chats, "open", page_btns=None))


def show_my_chats(uid: int):
    chats = get_my_active_chats(uid)
    send_message(uid, "📌 Мої чати:", reply_markup=kb_inline_chats(chats, "open", page_btns=None))


def show_archive(uid: int):
    ARCHIVE_PAGE[uid] = {"mode": "all", "value": None, "page": 0, "op_q": None, "client_q": None}
    show_archive_page(uid)


def build_archive_markup(uid: int, chats, page: int, has_next: bool):
    rows = []
    for cid in chats:
        rows.append([ibtn(get_chat_label(cid, include_closed=True), f"arch_open:{cid}")])

    btns = []
    if page > 0:
        btns.append(ibtn("⬅️", f"arch:page:{page-1}"))
    btns.append(ibtn(f"{page+1}", "arch:noop"))
    if has_next:
        btns.append(ibtn("➡️", f"arch:page:{page+1}"))
    rows.append(btns)

    rows.append([ibtn("📅 Фільтр по даті", "arch:filter:date")])
    rows.append([ibtn("🔎 Оператор", "arch:filter:op"), ibtn("🔎 Абонент", "arch:filter:client")])
    rows.append([ibtn("✖️ Скинути фільтри", "arch:filter:clear")])
    rows.append([ibtn("↩️ Назад", "arch:back")])
    return {"inline_keyboard": rows}


def show_archive_page(uid: int):
    st = ARCHIVE_PAGE.get(uid, {"mode": "all", "value": None, "page": 0, "op_q": None, "client_q": None})
    mode = st["mode"]
    val = st["value"]
    page = st["page"]
    op_q = st.get("op_q")
    client_q = st.get("client_q")

    limit = 10
    offset = page * limit

    chats = get_closed_chats_filtered(
        mode=mode,
        value=val,
        operator_q=op_q,
        client_q=client_q,
        limit=limit,
        offset=offset
    )

    has_next = len(chats) == limit
    filters_line = []
    if mode in ("day", "month") and val:
        filters_line.append(f"дата: {val}")
    if op_q:
        filters_line.append(f"оператор: {op_q}")
    if client_q:
        filters_line.append(f"абонент: {client_q}")
    subtitle = f"\nФільтри: {', '.join(filters_line)}" if filters_line else ""

    text = f"📚 Архів чатів:{subtitle}"
    markup = build_archive_markup(uid, chats, page, has_next)
    msg_id = st.get("msg_id")
    if msg_id:
        edit_message(uid, msg_id, text, reply_markup=markup)
    else:
        res = send_message(uid, text, reply_markup=markup)
        try:
            st["msg_id"] = int(res["result"]["message_id"])
        except Exception:
            st["msg_id"] = None


def show_admin_panel(uid: int):
    send_message(uid, "🛡 Адмін-панель:", kb_admin_main())


def show_operator_panel(uid: int):
    send_message(uid, "Панель оператора:", kb_operator_main(is_admin(uid)))


# -------- Admin add/remove operators --------

def resolve_user_id_from_text(s: str):
    s = s.strip()
    if s.isdigit():
        return int(s)
    # если @username — в базе users мы храним username с @
    if s.startswith("@"):
        # тупо ищем по users.username
        # (если человека нет в users, добавлять по @ без ID не получится)
        from db import _exec  # локально, чтобы не экспортировать наружу
        r = _exec("SELECT user_id FROM users WHERE username=?", (s,), fetchone=True)
        return int(r["user_id"]) if r else None
    return None


# -------- Client chat interactions --------

def client_send_to_chat(uid: int, chat_id: int, text: str):
    add_message(chat_id, "client", uid, text)
    notify_new_message(chat_id, uid, text)


# -------- Operator send to chat --------

def operator_send_to_chat(operator_id: int, chat_id: int, text: str):
    add_message(chat_id, "operator", operator_id, text)

    ch = get_chat(chat_id)
    if not ch:
        return
    client_id = int(ch["client_id"])
    op_u, _ = get_user(operator_id)

    # клиенту: другой смайлик для сообщений оператора
    send_message(client_id, f"👨‍💻 Оператор: {text}", kb_client_in_chat())


# -------- Callbacks --------

def handle_callback(cb):
    uid = cb["from"]["id"]
    data = cb["data"]
    answer_callback(cb["id"])

    # ---- rating ----
    if data.startswith("rate:"):
        if uid in RATING_PENDING:
            try:
                rating = int(data.split(":")[1])
            except (ValueError, IndexError):
                rating = 0
            if 1 <= rating <= 5:
                payload = RATING_PENDING.pop(uid)
                op_id = payload.get("operator_id")
                chat_id = payload.get("chat_id")
                if chat_id:
                    existing = get_chat_rating(chat_id)
                    if existing is not None:
                        send_message(uid, "Оцінку вже отримано. Дякуємо!", kb_client_main())
                        return
                    set_chat_rating(chat_id, rating)
                if op_id:
                    add_rating(op_id, rating)
                send_message(uid, "Дякуємо за оцінку!", kb_client_main())
            else:
                send_message(uid, "Оцініть роботу оператора:", kb_rating())
        else:
            send_message(uid, "Оцініть роботу оператора:", kb_rating())
        return

    # ---- connection flow callbacks ----
    if data.startswith("conn:"):
        if data == "conn:cancel":
            CONNECT.pop(uid, None)
            send_main_menu(uid)
            return

        st = CONNECT.get(uid)
        if not st:
            start_connect(uid)
            return

        if data.startswith("conn:type:"):
            t = data.split(":")[-1]  # flat/house/biz
            st["data"]["place"] = t
            st["step"] = 2
            if t == "flat":
                prompt = "Підкажіть, будь ласка, адресу підключення (будинок, під'їзд, поверх, номер квартири)."
            else:
                prompt = "Підкажіть, будь ласка, адресу підключення."
            send_message(uid, prompt, kb_cancel())
            return

        if data.startswith("conn:person:"):
            val = data.split(":")[-1]
            if val == "phys":
                st["data"]["person_type"] = "Фізична"
                st["step"] = 5
                send_message(uid, "Чи є у Вас роутер? (можете обрати або написати свій варіант)", kb_yes_no_free())
                return
            if val == "jur":
                st["data"]["person_type"] = "Юридична"
                st["step"] = 32
                send_message(uid, "Надішліть, будь ласка, адресу Вашої електронної пошти.", kb_cancel())
                return

        if data.startswith("conn:tariff:"):
            code = data.split(":")[-1]
            mapping = {
                "unlim299": "Unlim-299 - 100 Мбіт/с.",
                "unlim399": "Unlim-399 - 1000 Мбіт/с.",
                "priv299": "Private-299 - 100 Мбіт/с.",
                "priv399": "Private-399 - 1000 Мбіт/с.",
            }
            st["data"]["tariff"] = mapping.get(code, "—")
            st["step"] = 5
            send_message(uid, "Чи є у Вас роутер? (можете обрати або написати свій варіант)", kb_yes_no_free())
            return

        return

    # ---- waiting list take ----
    if data.startswith("wait_take:"):
        val = data.split(":")[-1]
        if val == "back":
            show_operator_panel(uid)
            return
        if val.startswith("page"):
            return

    if data.startswith("wait:page:"):
        page = int(data.split(":")[-1])
        show_waiting(uid, page=page)
        return

    if data.startswith("wait_take:"):
        cid = int(data.split(":")[-1])
        ok = take_chat(cid, uid)
        if ok:
            set_current_chat(uid, cid)
            # показать историю сразу
            hist = get_chat_history(cid)
            send_message(uid, f"✅ Ви взяли чат.\n\n{hist}", kb_operator_in_chat())
        else:
            send_message(uid, "Цей чат вже хтось взяв або він недоступний.", kb_operator_main(is_admin(uid)))
        return

    # ---- open chat ----
    if data.startswith("open:"):
        val = data.split(":")[-1]
        if val == "back":
            show_operator_panel(uid)
            return
        cid = int(val)
        set_current_chat(uid, cid)
        hist = get_chat_history(cid)
        send_message(uid, f"📨 Відкрито чат.\n\n{hist}", kb_operator_in_chat())
        return

    # ---- archive ----
    if data.startswith("arch:"):
        parts = data.split(":")
        cmd = parts[1] if len(parts) > 1 else ""
        if cmd == "back":
            show_operator_panel(uid)
            return
        if cmd == "page":
            page = int(data.split(":")[-1])
            st = ARCHIVE_PAGE.get(uid)
            if st:
                st["page"] = page
            show_archive_page(uid)
            return
        if cmd == "filter":
            ftype = parts[2] if len(parts) > 2 else "date"
            ARCHIVE_PAGE.setdefault(uid, {"mode": "all", "value": None, "page": 0, "op_q": None, "client_q": None})
            if ftype == "date":
                send_message(uid, "Введіть дату (дд.мм.рррр) або місяць (мм.рррр).", kb_cancel())
                ARCHIVE_PAGE[uid]["await_filter"] = "date"
            elif ftype == "op":
                send_message(uid, "Введіть @username або номер телефону оператора.", kb_cancel())
                ARCHIVE_PAGE[uid]["await_filter"] = "op"
            elif ftype == "client":
                send_message(uid, "Введіть @username або номер телефону абонента.", kb_cancel())
                ARCHIVE_PAGE[uid]["await_filter"] = "client"
            elif ftype == "clear":
                st = ARCHIVE_PAGE.get(uid, {})
                st["mode"] = "all"
                st["value"] = None
                st["page"] = 0
                st["op_q"] = None
                st["client_q"] = None
                show_archive_page(uid)
            return
        if cmd == "noop":
            return

    if data.startswith("arch_open:"):
        cid = int(data.split(":")[-1])
        hist = get_chat_history(cid)
        rating = get_chat_rating(cid)
        if rating is not None:
            hist = f"{hist}\n\n⭐️ Оцінка: {rating}/5"
        send_message(uid, hist, kb_operator_main(is_admin(uid)))
        return


# -------- Messages handler --------

def handle_message(m):
    uid = m["chat"]["id"]
    text = m.get("text", "")
    frm = m.get("from", {})
    username = frm.get("username")
    username = f"@{username}" if username else "—"

    ensure_superadmin(uid)

    # ---------- rating flow ----------
    if uid in RATING_PENDING:
        send_message(uid, "Оцініть роботу оператора:", kb_rating())
        return

    # ---------- registration ----------
    if text == "/start":
        if not user_exists(uid) and not (is_operator(uid) or is_admin(uid)):
            REG_STATE[uid] = "need_contact"
            send_message(uid, "Щоб продовжити, поділіться, будь ласка, номером телефону:", kb_reg_request())
            return
        send_main_menu(uid)
        return

    if uid in REG_STATE and REG_STATE[uid] == "need_contact":
        contact = m.get("contact")
        if not contact:
            send_message(uid, "Натисніть кнопку «Поділитись номером телефону».", kb_reg_request())
            return
        phone = contact.get("phone_number", "—")
        upsert_user(uid, username, phone)
        REG_STATE.pop(uid, None)
        send_message(uid, "✅ Реєстрацію завершено!", kb_client_main())
        return

    # операторы/админы тоже должны быть в users (для отображения @)
    if not user_exists(uid):
        # без телефона — ставим —
        upsert_user(uid, username, "—")

    # ---------- universal cancel ----------
    if text == "❌ Скасувати та в меню":
        SUPPORT_DRAFT.pop(uid, None)
        CONNECT.pop(uid, None)
        OP_CREATE.pop(uid, None)
        STATS_FILTER.pop(uid, None)
        # архив фильтр ожидание
        if uid in ARCHIVE_PAGE:
            ARCHIVE_PAGE[uid].pop("await_filter", None)
        send_main_menu(uid)
        return

    # ---------- admin mode ----------
    if is_admin(uid) and text == "👑 Адмін-панель":
        ADMIN_MODE[uid] = "admin"
        show_admin_panel(uid)
        return
    if is_admin(uid) and text == "🎧 Режим оператора":
        ADMIN_MODE[uid] = "operator"
        show_operator_panel(uid)
        return
    if is_admin(uid) and text == "🏠 Головне меню":
        ADMIN_MODE[uid] = "choose"
        send_message(uid, "Оберіть режим:", kb_admin_start())
        return

    # ---------- admin actions ----------
    if is_admin(uid) and text == "👥 Оператори":
        ops = list_operators()
        if not ops:
            send_message(uid, "Операторів ще немає.", kb_admin_main())
            return
        lines = ["👥 Оператори:"]
        for op_id in ops:
            u, p = get_user(op_id)
            lines.append(f"- {op_id} | {u} | {p}")
        send_message(uid, "\n".join(lines), kb_admin_main())
        return

    if is_admin(uid) and text == "➕ Додати оператора":
        ADMIN_INPUT[uid] = {"action": "add_op"}
        send_message(uid, "Введіть TG ID оператора (цифрами) або @username (якщо він вже є в базі).", kb_cancel())
        return

    if is_admin(uid) and text == "➖ Видалити оператора":
        ADMIN_INPUT[uid] = {"action": "del_op"}
        send_message(uid, "Введіть TG ID оператора (цифрами) або @username (якщо він вже є в базі).", kb_cancel())
        return

    if is_admin(uid) and text == "📊 Статистика":
        rows = get_stats()
        if not rows:
            send_message(uid, "Статистика порожня.", kb_admin_main())
            return
        lines = ["📊 Статистика операторів (всі):"]
        for r in rows:
            op_id = int(r["operator_id"])
            u, _ = get_user(op_id)
            avg = r["rating_avg"]
            avg_str = f"{avg:.2f}" if avg is not None else "—"
            lines.append(
                f"- {u} ({op_id}): взято {r['taken_count']}, закрито {r['closed_count']}, рейтинг {avg_str}"
            )
        send_message(uid, "\n".join(lines), kb_admin_main())
        send_message(
            uid,
            "Введіть період для фільтру статистики:\n"
            "• 04.2026 (місяць)\n"
            "• 2026 (рік)\n"
            "• 01.04.2026-07.04.2026 (діапазон дат)\n"
            "• 04.2026-08.2026 (діапазон місяців)\n"
            "Або напишіть 'все' щоб скинути фільтр.",
            kb_cancel()
        )
        STATS_FILTER[uid] = True
        return

    if uid in STATS_FILTER and is_admin(uid):
        kind, start, end = parse_stats_period(text)
        if kind is None:
            send_message(uid, "Невірний формат. Спробуйте ще раз або напишіть 'все'.", kb_cancel())
            return
        STATS_FILTER.pop(uid, None)
        if kind == "all":
            rows = get_stats()
            if not rows:
                send_message(uid, "Статистика порожня.", kb_admin_main())
                return
            lines = ["📊 Статистика операторів (всі):"]
            for r in rows:
                op_id = int(r["operator_id"])
                u, _ = get_user(op_id)
                avg = r["rating_avg"]
                avg_str = f"{avg:.2f}" if avg is not None else "—"
                lines.append(
                    f"- {u} ({op_id}): взято {r['taken_count']}, закрито {r['closed_count']}, рейтинг {avg_str}"
                )
            send_message(uid, "\n".join(lines), kb_admin_main())
            return

        rows = get_stats_by_period(start, end)
        if not rows:
            send_message(uid, "Статистика за період порожня.", kb_admin_main())
            return
        lines = [f"📊 Статистика операторів ({start} – {end}):"]
        for r in rows:
            op_id = int(r["operator_id"])
            u, _ = get_user(op_id)
            avg = r["rating_avg"]
            avg_str = f"{avg:.2f}" if avg is not None else "—"
            lines.append(
                f"- {u} ({op_id}): закрито {r['closed_count']}, рейтинг {avg_str}"
            )
        send_message(uid, "\n".join(lines), kb_admin_main())
        return

    if uid in ADMIN_INPUT:
        action = ADMIN_INPUT[uid]["action"]
        target = resolve_user_id_from_text(text)
        if not target:
            send_message(uid, "Не знайдено користувача. Введіть TG ID або @username (якщо він вже реєструвався).", kb_cancel())
            return
        if action == "add_op":
            set_operator(target, True)
            send_message(uid, "✅ Оператора додано.", kb_admin_main())
        elif action == "del_op":
            set_operator(target, False)
            send_message(uid, "✅ Оператора видалено.", kb_admin_main())
        ADMIN_INPUT.pop(uid, None)
        return

    # ---------- operator panel ----------
    if is_operator(uid) or is_admin(uid):
        # кнопка домой
        if text == "🏠 Головне меню":
            OP_CREATE.pop(uid, None)
            send_main_menu(uid)
            return

        if text == "⏳ Очікуючі чати":
            show_waiting(uid, page=0)
            return

        if text == "🟢 Активні чати":
            show_active(uid)
            return

        if text == "📌 Мої чати":
            show_my_chats(uid)
            return

        if text == "📚 Архів чатів":
            show_archive(uid)
            return

        if text == "➕ Створити чат з абонентом":
            OP_CREATE[uid] = {"step": 1}
            send_message(uid, "Введіть TG ID абонента (цифрами) або @username (якщо він уже є в базі).", kb_cancel())
            return

        # создание чата оператором (шаги)
        if uid in OP_CREATE:
            st = OP_CREATE[uid]
            step = st.get("step")

            if step == 1:
                target = resolve_user_id_from_text(text)
                if not target:
                    send_message(uid, "Не знайдено користувача. Введіть TG ID або @username (якщо він уже реєструвався).", kb_cancel())
                    return
                st["target"] = target
                st["step"] = 2
                send_message(uid, "Введіть перше повідомлення абоненту:", kb_cancel())
                return

            if step == 2:
                target = st.get("target")
                if not target:
                    OP_CREATE.pop(uid, None)
                    send_message(uid, "Не вдалося визначити абонента. Спробуйте ще раз.", kb_operator_main(is_admin(uid)))
                    return

                # если у абонента уже есть активный/ожидающий чат - используем его
                existing = find_latest_open_chat_for_client(target)
                if existing:
                    cid = int(existing["chat_id"])
                    set_current_chat(uid, cid)
                    ch = get_chat(cid)
                    if ch and ch["status"] == "waiting":
                        take_chat(cid, uid)
                    operator_send_to_chat(uid, cid, text)
                    OP_CREATE.pop(uid, None)
                    send_message(uid, "✅ Повідомлення надіслано в існуючий чат.", kb_operator_in_chat())
                    return

                cid = create_chat(target, first_text=text)
                ok = take_chat(cid, uid)
                if not ok:
                    OP_CREATE.pop(uid, None)
                    send_message(uid, "Не вдалося створити чат. Спробуйте пізніше.", kb_operator_main(is_admin(uid)))
                    return

                set_current_chat(uid, cid)
                operator_send_to_chat(uid, cid, text)
                OP_CREATE.pop(uid, None)
                send_message(uid, "✅ Чат створено. Повідомлення надіслано абоненту.", kb_operator_in_chat())
                return

        # внутри чата
        if text == "↩️ Вийти в меню (чат лишається)":
            # НЕ сбрасываем current_chat_id (чат открыт логически, но UI в меню)
            show_operator_panel(uid)
            return

        cur_chat = get_current_chat(uid)

        if text == "✅ Завершити чат":
            if not cur_chat:
                send_message(uid, "Спочатку відкрийте чат.", kb_operator_main(is_admin(uid)))
                return
            ch = get_chat(cur_chat)
            if not ch or ch["status"] != "active":
                send_message(uid, "Цей чат вже не активний.", kb_operator_main(is_admin(uid)))
                set_current_chat(uid, None)
                return

            ok = close_chat(cur_chat)
            if ok:
                inc_closed(uid)
                # клиенту
                client_id = int(ch["client_id"])
                send_message(client_id, "✅ Чат завершено оператором.", kb_client_main())
                RATING_PENDING[client_id] = {"operator_id": uid, "chat_id": cur_chat}
                send_message(client_id, "Оцініть роботу оператора:", kb_rating())
                add_message(cur_chat, "system", None, "Чат завершено оператором.")
                set_current_chat(uid, None)
                show_operator_panel(uid)
            else:
                send_message(uid, "Не вдалося завершити чат.", kb_operator_in_chat())
            return

        if text == "🔁 Передати чат":
            if not cur_chat:
                send_message(uid, "Спочатку відкрийте чат.", kb_operator_main(is_admin(uid)))
                return
            ok = transfer_chat(cur_chat, uid)
            if ok:
                ch = get_chat(cur_chat)
                client_id = int(ch["client_id"])
                add_message(cur_chat, "system", None, f"Чат передано оператором {get_user(uid)[0]}.")
                send_message(client_id, "🔁 Ваш чат передано іншому оператору. Зачекайте, будь ласка.", kb_client_in_chat())
                notify_transfer(cur_chat, uid)
                set_current_chat(uid, None)
                show_operator_panel(uid)
            else:
                send_message(uid, "Не вдалося передати чат (можливо, він не ваш або вже не активний).", kb_operator_in_chat())
            return

        if text == "📚 Історія абонента":
            if not cur_chat:
                send_message(uid, "Спочатку відкрийте чат.", kb_operator_main(is_admin(uid)))
                return
            ch = get_chat(cur_chat)
            if not ch:
                send_message(uid, "Чат не знайдено.", kb_operator_main(is_admin(uid)))
                return
            client_id = int(ch["client_id"])
            CLIENT_ARCH_PAGE[uid] = {"client_id": client_id, "page": 0}
            show_client_archive(uid)
            return

        # оператор пишет сообщение в чат
        if cur_chat and text and not text.startswith("/"):
            ch = get_chat(cur_chat)
            if ch and ch["status"] == "active" and ch["operator_id"] == uid:
                operator_send_to_chat(uid, cur_chat, text)
                return

    # ---------- client main menu ----------
    if text == "🛠 Звернутись у тех. підтримку":
        start_support(uid)
        return

    if text == "📡 Заявка на підключення":
        start_connect(uid)
        return

    # ---------- archive filter input ----------
    if uid in ARCHIVE_PAGE and ARCHIVE_PAGE[uid].get("await_filter"):
        ftype = ARCHIVE_PAGE[uid].get("await_filter")
        if ftype == "date":
            kind, iso = parse_day_or_month(text)
            if not kind:
                send_message(uid, "Невірний формат. Введіть дд.мм.рррр або мм.рррр.", kb_cancel())
                return
            ARCHIVE_PAGE[uid].pop("await_filter", None)
            ARCHIVE_PAGE[uid]["page"] = 0
            if kind == "day":
                ARCHIVE_PAGE[uid]["mode"] = "day"
                ARCHIVE_PAGE[uid]["value"] = iso
            else:
                ARCHIVE_PAGE[uid]["mode"] = "month"
                ARCHIVE_PAGE[uid]["value"] = iso
            show_archive_page(uid)
            return

        if ftype == "op":
            q = text.strip()
            if not q:
                send_message(uid, "Введіть @username або номер телефону оператора.", kb_cancel())
                return
            ARCHIVE_PAGE[uid].pop("await_filter", None)
            ARCHIVE_PAGE[uid]["op_q"] = q
            ARCHIVE_PAGE[uid]["page"] = 0
            show_archive_page(uid)
            return

        if ftype == "client":
            q = text.strip()
            if not q:
                send_message(uid, "Введіть @username або номер телефону абонента.", kb_cancel())
                return
            ARCHIVE_PAGE[uid].pop("await_filter", None)
            ARCHIVE_PAGE[uid]["client_q"] = q
            ARCHIVE_PAGE[uid]["page"] = 0
            show_archive_page(uid)
            return

    # ---------- connection FSM steps by text ----------
    if uid in CONNECT:
        st = CONNECT[uid]
        step = st["step"]
        data = st["data"]

        # step 2: address
        if step == 2:
            data["address"] = text
            if data.get("place") in ("house", "biz"):
                st["step"] = 3
                send_message(
                    uid,
                    "Надішліть геолокацію одним із способів:\n"
                    "1) Через Telegram: натисніть скріпку → Локація → Вибрати на мапі → Оберіть точку → Надіслати.\n"
                    "2) Або надішліть посилання на мапу (наприклад, Google Maps).",
                    kb_geo_request()
                )
            else:
                st["step"] = 4
                connect_ask_tariff(uid, data.get("place"))
            return

        # step 3: geo (text или location)
        if step == 3:
            if "location" in m:
                loc = m["location"]
                data["geo"] = f"{loc.get('latitude')}, {loc.get('longitude')}"
            elif text and text.strip():
                data["geo"] = text
            else:
                send_message(
                    uid,
                    "Надішліть геолокацію через скріпку → Локація → Вибрати на мапі, або посилання на мапу.",
                    kb_geo_request()
                )
                return

            if data.get("place") == "biz":
                st["step"] = 31
                send_message(uid, "Дякуємо, геолокацію отримано.", kb_remove())
                send_message(uid, "Бажаєте підключитись як фізична чи юридична особа?",
                             kb_inline([
                                 [ibtn("Фізична", "conn:person:phys")],
                                 [ibtn("Юридична", "conn:person:jur")]
                             ]))
            else:
                st["step"] = 4
                send_message(uid, "Дякуємо, геолокацію отримано.", kb_remove())
                connect_ask_tariff(uid, data.get("place"))
            return

        # step 32: email
        if step == 32:
            data["email"] = text
            st["step"] = 5
            send_message(uid, "Чи є у Вас роутер? (можете обрати або написати свій варіант)", kb_yes_no_free())
            return

        # step 5: router
        if step == 5:
            data["router"] = text
            st["step"] = 6
            send_message(uid, "Чи потрібне Вам телебачення? (можете обрати або написати свій варіант)", kb_yes_no_free())
            return

        # step 6: tv
        if step == 6:
            data["tv"] = text
            st["step"] = 7
            send_message(uid, "Як Вам зручніше продовжити?", kb_contact_format())
            return

        # step 7: contact format (строго)
        if step == 7:
            if text not in ("💬 Продовжити тут", "📞 Дзвінок від нас"):
                # игнор/повтор
                send_message(uid, "Оберіть варіант з кнопок:", kb_contact_format())
                return
            data["contact_format"] = text
            CONNECT.pop(uid, None)
            finish_connect(uid, data)
            return

    # ---------- support draft ----------
    if uid in SUPPORT_DRAFT:
        # клиент пишет текст обращения
        SUPPORT_DRAFT.pop(uid, None)
        create_support_chat(uid, text)
        return

    # ---------- client in chat: forward to operators ----------
    # если у клиента есть активный чат (status active OR waiting?) — отправляем туда
    # (логика: если waiting — операторов уведомляем как новое сообщение)
    # находим последний чат клиента со статусом waiting/active
    ch = find_latest_open_chat_for_client(uid)
    if ch:
        cid = int(ch["chat_id"])
        if text == "✅ Завершити чат":
            # клиент завершает чат
            ok = close_chat(cid)
            if ok:
                add_message(cid, "system", None, "Чат завершено абонентом.")
                send_message(uid, "✅ Чат завершено.", kb_client_main())
                ch = get_chat(cid)
                if ch and ch["operator_id"]:
                    op_id = int(ch["operator_id"])
                    if get_current_chat(op_id) == cid:
                        set_current_chat(op_id, None)
                    send_message(op_id, "✅ Чат завершено абонентом.", kb_operator_main(is_admin(op_id)))
                    inc_closed(op_id)
                    RATING_PENDING[uid] = {"operator_id": op_id, "chat_id": cid}
                    send_message(uid, "Оцініть роботу оператора:", kb_rating())
            else:
                send_message(uid, "Не вдалося завершити чат (можливо, його вже завершено).", kb_client_main())
            return

        if text == "🏠 Головне меню":
            send_message(uid, "Головне меню:", kb_client_main())
            return

        # обычный текст — в чат
        client_send_to_chat(uid, cid, text)
        return

    # fallback
    send_main_menu(uid)


def find_latest_open_chat_for_client(client_id: int):
    from db import _exec
    r = _exec("""
        SELECT * FROM chats
        WHERE client_id=? AND status IN ('waiting','active')
        ORDER BY chat_id DESC LIMIT 1
    """, (client_id,), fetchone=True)
    return r


def show_client_archive(uid: int):
    st = CLIENT_ARCH_PAGE.get(uid)
    if not st:
        return
    client_id = st["client_id"]
    page = st["page"]
    limit = 10
    offset = page * limit
    chats = get_client_chats(client_id, limit=limit, offset=offset)

    btns = []
    if page > 0:
        btns.append(ibtn("⬅️ Попередні", f"carch:page:{page-1}"))
    if len(chats) == limit:
        btns.append(ibtn("➡️ Наступні", f"carch:page:{page+1}"))

    rows = []
    for cid in chats:
        rows.append([ibtn(get_chat_label(cid, include_closed=True), f"carch_open:{cid}")])
    if btns:
        rows.append(btns)
    rows.append([ibtn("⬅️ Назад", "carch:back")])

    send_message(uid, "📚 Архів цього абонента:", reply_markup={"inline_keyboard": rows})


def handle_callback_extra(cb):
    uid = cb["from"]["id"]
    data = cb["data"]
    answer_callback(cb["id"])

    if data.startswith("carch:"):
        cmd = data.split(":")[1]
        if cmd == "back":
            # возвращаем в чат
            cur = get_current_chat(uid)
            if cur:
                send_message(uid, "Повернення в чат:", kb_operator_in_chat())
            else:
                show_operator_panel(uid)
            return
        if cmd == "page":
            page = int(data.split(":")[-1])
            CLIENT_ARCH_PAGE[uid]["page"] = page
            show_client_archive(uid)
            return

    if data.startswith("carch_open:"):
        cid = int(data.split(":")[-1])
        hist = get_chat_history(cid)
        rating = get_chat_rating(cid)
        if rating is not None:
            hist = f"{hist}\n\n⭐️ Оцінка: {rating}/5"
        send_message(uid, hist, kb_operator_in_chat())
        return

    # если не обработали — в основной
    return handle_callback(cb)


def main():
    init_db()

    offset = None
    print("Support bot FULL started")

    while True:
        upd = get_updates(offset)
        for u in upd.get("result", []):
            offset = u["update_id"] + 1

            if "callback_query" in u:
                # сначала дополнительные колбэки
                try:
                    handle_callback_extra(u["callback_query"])
                except Exception:
                    # fallback
                    handle_callback(u["callback_query"])
                continue

            if "message" in u:
                handle_message(u["message"])
        time.sleep(0.25)


if __name__ == "__main__":
    main()
