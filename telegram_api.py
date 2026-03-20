from __future__ import annotations

import requests
from config import TOKEN

API = f"https://api.telegram.org/bot{TOKEN}"


class TelegramAPIError(RuntimeError):
    pass


def _check_response(resp: requests.Response) -> dict:
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok", False):
        raise TelegramAPIError(str(data))
    return data


def tg(method: str, payload: dict | None = None, *, method_type: str = "post", timeout: int = 30) -> dict:
    url = f"{API}/{method}"
    payload = payload or {}
    if method_type == "get":
        resp = requests.get(url, params=payload, timeout=timeout)
    else:
        resp = requests.post(url, json=payload, timeout=timeout)
    return _check_response(resp)


def send_message(chat_id: int, text: str, reply_markup=None, disable_web_page_preview=True):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg("sendMessage", payload)


def answer_callback(callback_id: str, text: str = ""):
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
    return tg("answerCallbackQuery", payload)


def get_updates(offset: int | None, timeout_s: int = 30):
    params = {"timeout": timeout_s}
    if offset is not None:
        params["offset"] = offset
    return tg("getUpdates", params, method_type="get", timeout=timeout_s + 5)


def kb_reply(rows: list[list[dict]], resize=True):
    return {"keyboard": rows, "resize_keyboard": resize}


def btn(text: str):
    return {"text": text}


def kb_main():
    return kb_reply([
        [btn("🛠 Звернутись у тех. підтримку")],
        [btn("📡 Заявка на підключення")],
    ])
