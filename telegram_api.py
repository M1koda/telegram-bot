from __future__ import annotations

import json
import requests

from config import TOKEN
from settings import (
    MENU_CLOSE_CHAT_BUTTON_TEXT,
    MENU_CONNECTION_BUTTON_TEXT,
    MENU_SUPPORT_BUTTON_TEXT,
    PHONE_REQUEST_BUTTON_TEXT,
)

API = f"https://api.telegram.org/bot{TOKEN}"
FILE_API = f"https://api.telegram.org/file/bot{TOKEN}"


class TelegramAPIError(RuntimeError):
    pass


def _check_response(resp: requests.Response) -> dict:
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok", False):
        raise TelegramAPIError(str(data))
    return data


def tg(
    method: str,
    payload: dict | None = None,
    *,
    method_type: str = "post",
    timeout: int = 30,
    files: dict | None = None,
) -> dict:
    url = f"{API}/{method}"
    payload = payload or {}
    if method_type == "get":
        resp = requests.get(url, params=payload, timeout=timeout)
    elif files:
        resp = requests.post(url, data=payload, files=files, timeout=timeout)
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


def edit_message(
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup=None,
    disable_web_page_preview=True,
):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg("editMessageText", payload)


def delete_message(chat_id: int, message_id: int):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
    }
    return tg("deleteMessage", payload)


def send_sticker(
    chat_id: int,
    sticker: str,
    *,
    emoji: str | None = None,
    reply_markup=None,
):
    payload = {
        "chat_id": chat_id,
        "sticker": sticker,
    }
    if emoji:
        payload["emoji"] = emoji
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg("sendSticker", payload)


def upload_sticker(
    chat_id: int,
    file_name: str,
    content: bytes,
    *,
    emoji: str | None = None,
    reply_markup=None,
):
    payload = {"chat_id": str(chat_id)}
    if emoji:
        payload["emoji"] = emoji
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    files = {"sticker": (file_name, content)}
    return tg("sendSticker", payload, files=files)


def answer_callback(callback_id: str, text: str = "", show_alert: bool = False):
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
    if show_alert:
        payload["show_alert"] = True
    return tg("answerCallbackQuery", payload)


def get_updates(offset: int | None, timeout_s: int = 30):
    params = {"timeout": timeout_s}
    if offset is not None:
        params["offset"] = offset
    return tg("getUpdates", params, method_type="get", timeout=timeout_s + 5)


def get_user_profile_photos(user_id: int, limit: int = 1):
    return tg("getUserProfilePhotos", {"user_id": user_id, "limit": limit})


def get_file(file_id: str):
    return tg("getFile", {"file_id": file_id})


def download_file(file_path: str, *, timeout: int = 30) -> tuple[bytes, str | None]:
    url = f"{FILE_API}/{str(file_path).lstrip('/')}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content, resp.headers.get("Content-Type")


def kb_reply(rows: list[list[dict]], resize=True):
    return {"keyboard": rows, "resize_keyboard": resize}


def btn(text: str):
    return {"text": text}


def contact_btn(text: str):
    return {"text": text, "request_contact": True}


def ibtn(text: str, callback_data: str):
    return {"text": text, "callback_data": callback_data}


def kb_main(*, show_close_chat: bool = False):
    rows = [
        [btn(MENU_SUPPORT_BUTTON_TEXT)],
        [btn(MENU_CONNECTION_BUTTON_TEXT)],
    ]
    if show_close_chat:
        rows.append([btn(MENU_CLOSE_CHAT_BUTTON_TEXT)])
    return kb_reply(rows)


def kb_inline(rows: list[list[dict]]):
    return {"inline_keyboard": rows}


def kb_single_button(text: str):
    return kb_reply([[btn(text)]])


def kb_request_contact(text: str = PHONE_REQUEST_BUTTON_TEXT):
    return kb_reply([[contact_btn(text)]])
