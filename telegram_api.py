import requests
from config import TOKEN

API = f"https://api.telegram.org/bot{TOKEN}"


def tg(method: str, payload: dict):
    r = requests.post(f"{API}/{method}", json=payload, timeout=30)
    return r.json()


def send_message(chat_id: int, text: str, reply_markup=None, disable_web_page_preview=True):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg("sendMessage", payload)


def edit_message(chat_id: int, message_id: int, text: str, reply_markup=None, disable_web_page_preview=True):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg("editMessageText", payload)


def answer_callback(callback_id: str, text: str = ""):
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    return tg("answerCallbackQuery", payload)


def get_updates(offset: int | None):
    params = {"timeout": 30}
    if offset is not None:
        params["offset"] = offset
    r = requests.get(f"{API}/getUpdates", params=params, timeout=35)
    return r.json()


# ---------- keyboards ----------

def kb_reply(rows: list[list[dict]], resize=True):
    return {"keyboard": rows, "resize_keyboard": resize}


def kb_inline(rows: list[list[dict]]):
    return {"inline_keyboard": rows}


def btn(text: str):
    return {"text": text}


def btn_contact(text: str):
    return {"text": text, "request_contact": True}


def btn_location(text: str):
    return {"text": text, "request_location": True}


def ibtn(text: str, data: str):
    return {"text": text, "callback_data": data}


# client menus
def kb_client_main():
    return kb_reply([
        [btn("🛠 Звернутись у тех. підтримку")],
        [btn("📡 Заявка на підключення")]
    ])


def kb_cancel():
    return kb_reply([[btn("❌ Скасувати та в меню")]])


# operator/admin menus
def kb_operator_main(is_admin: bool):
    rows = [
        [btn("⏳ Очікуючі чати"), btn("🟢 Активні чати")],
        [btn("📌 Мої чати"), btn("📚 Архів чатів")],
        [btn("➕ Створити чат з абонентом")],
    ]
    if is_admin:
        rows.append([btn("🏠 Головне меню")])
    return kb_reply(rows)


def kb_admin_main():
    return kb_reply([
        [btn("👥 Оператори"), btn("➕ Додати оператора")],
        [btn("➖ Видалити оператора"), btn("📊 Статистика")],
        [btn("🏠 Головне меню")]
    ])


def kb_admin_start():
    return kb_reply([
        [btn("🎧 Режим оператора"), btn("👑 Адмін-панель")]
    ])


def kb_operator_in_chat():
    return kb_reply([
        [btn("✅ Завершити чат"), btn("🔁 Передати чат")],
        [btn("📚 Історія абонента")],
        [btn("↩️ Вийти в меню (чат лишається)")]
    ])


def kb_client_in_chat():
    return kb_reply([
        [btn("✅ Завершити чат")],
        [btn("🏠 Головне меню")]
    ])


def kb_yes_no_free():
    return kb_reply([
        [btn("Так"), btn("Ні")],
        [btn("❌ Скасувати та в меню")]
    ])


def kb_contact_format():
    return kb_reply([
        [btn("💬 Продовжити тут")],
        [btn("📞 Дзвінок від нас")],
        [btn("❌ Скасувати та в меню")]
    ])


def kb_reg_request():
    return kb_reply([
        [btn_contact("📱 Поділитись номером телефону")],
    ])


def kb_geo_request():
    return kb_reply([
        [btn("❌ Скасувати та в меню")]
    ])


def kb_remove():
    return {"remove_keyboard": True}


def kb_rating():
    return kb_inline([
        [
            ibtn("⭐️", "rate:1"),
            ibtn("⭐️", "rate:2"),
            ibtn("⭐️", "rate:3"),
            ibtn("⭐️", "rate:4"),
            ibtn("⭐️", "rate:5"),
        ]
    ])
