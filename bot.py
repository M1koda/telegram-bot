from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import socketio

from config import (
    CONNECTION_CALL_TARGET_CHAT_ID,
    SUPPORT_API_BASE_URL,
    SUPPORT_BOT_API_KEY,
    ZIP_SOCKET_URL,
)
from settings import (
    CHAT_CLOSE_ERROR_TEXT,
    CHAT_CLOSE_UNAVAILABLE_TEXT,
    CHAT_CLOSED_TEXT,
    CHAT_REOPENED_TEXT,
    CHAT_TAKEN_TEXT,
    CHAT_TRANSFERRED_TEXT,
    CONNECTION_ADDRESS_PROMPT_TEXT,
    CONNECTION_CALL_REQUEST_SENT_TEXT,
    CONNECTION_CONTINUE_CALL_BUTTON_TEXT,
    CONNECTION_CONTINUE_CHAT_BUTTON_TEXT,
    CONNECTION_CONTINUE_PROMPT_TEXT,
    CONNECTION_PHONE_PROMPT_TEXT,
    CONNECTION_ROOM_NON_RESIDENTIAL_BUTTON_TEXT,
    CONNECTION_ROOM_PROMPT_TEXT,
    CONNECTION_ROOM_RESIDENTIAL_BUTTON_TEXT,
    DETAILS_PROMPT_TEXT,
    MENU_CLOSE_CHAT_BUTTON_TEXT,
    MENU_CONNECTION_BUTTON_TEXT,
    MENU_SUPPORT_BUTTON_TEXT,
    NON_TEXT_MESSAGE_TEXT,
    NO_ACTIVE_CHAT_TEXT,
    OPERATOR_MESSAGE_PREFIX,
    PHONE_GATE_INVALID_CONTACT_TEXT,
    PHONE_GATE_PROMPT_TEXT,
    PHONE_GATE_SUCCESS_TEXT,
    RATING_RETURN_TO_MENU_BUTTON_TEXT,
    RATING_ALREADY_SUBMITTED_TEXT,
    RATING_CANCELED_TEXT,
    RATING_COMMENT_PROMPT_TEXT,
    RATING_COMMENT_THANK_YOU_TEXT,
    RATING_EXPIRED_TEXT,
    RATING_INVALID_BUTTON_TEXT,
    RATING_PROMPT_TEXT,
    RATING_SKIP_BUTTON_TEXT,
    RATING_SKIPPED_TEXT,
    RATING_TEMPORARY_ERROR_TEXT,
    REQUEST_CANCEL_BUTTON_TEXT,
    RESOLVED_TEXT,
    SEND_ERROR_TEXT,
    STATE_FILE,
    TELEGRAM_POLL_TIMEOUT,
    TELEGRAM_RETRY_DELAY,
    WAITING_TEXT,
    WAITING_CUSTOMER_TEXT,
    WELCOME_TEXT,
    ZIP_SOCKET_PATH,
    ZIP_SOCKET_RECONNECT_ATTEMPTS,
    ZIP_SOCKET_VERIFY_SSL,
)
from state import StateStore
from telegram_api import (
    TelegramAPIError,
    answer_callback,
    delete_message,
    download_file,
    edit_message,
    get_file,
    get_updates,
    get_user_profile_photos,
    ibtn,
    btn,
    kb_inline,
    kb_main,
    kb_request_contact,
    kb_reply,
    kb_single_button,
    send_message,
    send_sticker,
    upload_sticker,
)
from zip_client import ZipAPIError, ZipSupportClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("zip-support-bot")

DEFAULT_SUPPORT_AVATAR_CACHE_DIR = Path(__file__).resolve().parent / "support_avatars"
DEFAULT_SUPPORT_STICKER_CACHE_DIR = Path(__file__).resolve().parent / "support_stickers"


class SupportBot:
    def __init__(self):
        self.state = StateStore(STATE_FILE)
        self.zip = ZipSupportClient(SUPPORT_API_BASE_URL, SUPPORT_BOT_API_KEY)
        self.avatar_cache_dir = Path(
            os.getenv("SUPPORT_AVATAR_CACHE_DIR", str(DEFAULT_SUPPORT_AVATAR_CACHE_DIR))
        )
        self.avatar_public_base_url = os.getenv("SUPPORT_AVATAR_PUBLIC_BASE_URL", "").rstrip("/")
        self.avatar_refresh_seconds = max(0, int(os.getenv("SUPPORT_AVATAR_REFRESH_SECONDS", "86400")))
        sticker_public_base_url = os.getenv("SUPPORT_STICKER_PUBLIC_BASE_URL", self.avatar_public_base_url).rstrip("/")
        default_sticker_cache_dir = str(
            self.avatar_cache_dir if sticker_public_base_url and sticker_public_base_url == self.avatar_public_base_url
            else DEFAULT_SUPPORT_STICKER_CACHE_DIR
        )
        self.sticker_cache_dir = Path(os.getenv("SUPPORT_STICKER_CACHE_DIR", default_sticker_cache_dir))
        self.sticker_public_base_url = sticker_public_base_url
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=ZIP_SOCKET_RECONNECT_ATTEMPTS or 0,
            logger=False,
            engineio_logger=False,
            ssl_verify=ZIP_SOCKET_VERIFY_SSL,
        )
        self.offset: int | None = None
        if self.avatar_public_base_url:
            self.avatar_cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Avatar cache enabled dir=%s base_url=%s refresh_seconds=%s",
                self.avatar_cache_dir,
                self.avatar_public_base_url,
                self.avatar_refresh_seconds,
            )
        else:
            logger.warning("Avatar cache disabled: SUPPORT_AVATAR_PUBLIC_BASE_URL is not configured")
        if self.sticker_public_base_url:
            self.sticker_cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Sticker cache enabled dir=%s base_url=%s",
                self.sticker_cache_dir,
                self.sticker_public_base_url,
            )
        else:
            logger.warning("Sticker cache disabled: SUPPORT_STICKER_PUBLIC_BASE_URL is not configured")
        self._register_socket_handlers()

    def _has_open_chat(self, tg_user_id: int) -> bool:
        chat_id = self.state.get_chat_by_tg(tg_user_id)
        if chat_id is None:
            return False

        chat_meta = self.state.get_chat_meta(chat_id)
        if chat_meta is not None and str(chat_meta.get("status") or "").lower() == "closed":
            self.state.clear_chat(tg_user_id=tg_user_id)
            self.state.clear_chat(chat_id=chat_id)
            self.state.clear_welcomed(chat_id)
            return False

        return True

    def _main_keyboard(self, tg_user_id: int, *, show_close_chat: bool | None = None) -> dict[str, Any]:
        if show_close_chat is None:
            show_close_chat = self._has_open_chat(tg_user_id)
        if show_close_chat:
            return kb_single_button(MENU_CLOSE_CHAT_BUTTON_TEXT)
        return kb_main(show_close_chat=False)

    def _send_main_message(self, tg_user_id: int, text: str, *, show_close_chat: bool | None = None):
        return send_message(tg_user_id, text, self._main_keyboard(tg_user_id, show_close_chat=show_close_chat))

    @staticmethod
    def _request_draft_keyboard() -> dict[str, Any]:
        return kb_single_button(REQUEST_CANCEL_BUTTON_TEXT)

    @staticmethod
    def _phone_gate_keyboard() -> dict[str, Any]:
        return kb_request_contact()

    @staticmethod
    def _rating_comment_keyboard() -> dict[str, Any]:
        return kb_single_button(RATING_RETURN_TO_MENU_BUTTON_TEXT)

    @staticmethod
    def _connection_room_keyboard() -> dict[str, Any]:
        return kb_reply(
            [
                [
                    btn(CONNECTION_ROOM_RESIDENTIAL_BUTTON_TEXT),
                    btn(CONNECTION_ROOM_NON_RESIDENTIAL_BUTTON_TEXT),
                ],
                [btn(REQUEST_CANCEL_BUTTON_TEXT)],
            ]
        )

    @staticmethod
    def _connection_continue_keyboard() -> dict[str, Any]:
        return kb_reply(
            [
                [btn(CONNECTION_CONTINUE_CHAT_BUTTON_TEXT)],
                [btn(CONNECTION_CONTINUE_CALL_BUTTON_TEXT)],
                [btn(REQUEST_CANCEL_BUTTON_TEXT)],
            ]
        )

    @staticmethod
    def _format_connection_request_message(
        subscriber_name: str,
        tg_user_id: int,
        connection_request: dict[str, Any],
        *,
        continuation_label: str,
    ) -> str:
        lines = [
            "\U0001f4e1 \u0417\u0430\u044f\u0432\u043a\u0430 \u043d\u0430 \u043f\u0456\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u043d\u044f",
            f"\u0410\u0431\u043e\u043d\u0435\u043d\u0442: {subscriber_name}",
            f"Telegram ID: {tg_user_id}",
            f"\u0422\u0438\u043f \u043f\u0440\u0438\u043c\u0456\u0449\u0435\u043d\u043d\u044f: {connection_request.get('roomType') or '-'}",
            f"\u0410\u0434\u0440\u0435\u0441\u0430: {connection_request.get('address') or '-'}",
            f"\u041a\u043e\u043d\u0442\u0430\u043a\u0442\u043d\u0438\u0439 \u043d\u043e\u043c\u0435\u0440: {connection_request.get('phone') or '-'}",
            f"\u042f\u043a \u043f\u0440\u043e\u0434\u043e\u0432\u0436\u0438\u0442\u0438: {continuation_label}",
        ]
        return "\n".join(lines)

    def _register_socket_handlers(self):
        @self.sio.event
        def connect():
            logger.info("Connected to ZIP socket")
            self.sio.emit("support:join")

        @self.sio.event
        def disconnect():
            logger.warning("Disconnected from ZIP socket")

        @self.sio.on("support:new-message")
        def on_new_message(data: dict[str, Any]):
            try:
                self.handle_support_message_event(data)
            except Exception:
                logger.exception("Failed to process support:new-message")

        @self.sio.on("support:message-updated")
        def on_message_updated(data: dict[str, Any]):
            try:
                self.handle_message_updated_event(data)
            except Exception:
                logger.exception("Failed to process support:message-updated")

        @self.sio.on("support:message-deleted")
        def on_message_deleted(data: dict[str, Any]):
            try:
                self.handle_message_deleted_event(data)
            except Exception:
                logger.exception("Failed to process support:message-deleted")

        @self.sio.on("support:chat-closed")
        def on_chat_closed(data: dict[str, Any]):
            try:
                self.handle_chat_closed_event(data)
            except Exception:
                logger.exception("Failed to process support:chat-closed")

        @self.sio.on("support:chat-updated")
        def on_chat_updated(data: dict[str, Any]):
            try:
                self.handle_chat_updated_event(data)
            except Exception:
                logger.exception("Failed to process support:chat-updated")

        @self.sio.on("support:chat-taken")
        def on_chat_taken(data: dict[str, Any]):
            try:
                self.handle_chat_status_event(data, CHAT_TAKEN_TEXT)
            except Exception:
                logger.exception("Failed to process support:chat-taken")

        @self.sio.on("support:chat-transferred")
        def on_chat_transferred(data: dict[str, Any]):
            try:
                self.handle_chat_status_event(data, CHAT_TRANSFERRED_TEXT)
            except Exception:
                logger.exception("Failed to process support:chat-transferred")

        @self.sio.on("support:chat-reopened")
        def on_chat_reopened(data: dict[str, Any]):
            try:
                self.handle_chat_reopened_event(data)
            except Exception:
                logger.exception("Failed to process support:chat-reopened")

        @self.sio.on("support:rating-requested")
        def on_rating_requested(data: dict[str, Any]):
            try:
                self.handle_rating_requested_event(data)
            except Exception:
                logger.exception("Failed to process support:rating-requested")

        @self.sio.on("support:rating-updated")
        def on_rating_updated(data: dict[str, Any]):
            try:
                self.handle_rating_updated_event(data)
            except Exception:
                logger.exception("Failed to process support:rating-updated")

    def connect_socket_forever(self):
        while True:
            try:
                logger.info("Connecting to ZIP socket: %s", ZIP_SOCKET_URL)
                self.sio.connect(
                    ZIP_SOCKET_URL,
                    socketio_path=ZIP_SOCKET_PATH.lstrip("/"),
                    transports=["websocket", "polling"],
                    auth={"token": SUPPORT_BOT_API_KEY},
                    wait_timeout=15,
                )
                self.sio.wait()
            except Exception:
                logger.exception("ZIP socket connection failed")
                time.sleep(5)

    def run(self):
        threading.Thread(target=self.connect_socket_forever, name="zip-socket", daemon=True).start()
        self._recover_pending_ratings()
        logger.info("Support bot started")
        while True:
            try:
                updates = get_updates(self.offset, timeout_s=TELEGRAM_POLL_TIMEOUT)
                for upd in updates.get("result", []):
                    self.offset = upd["update_id"] + 1
                    if "callback_query" in upd:
                        self.handle_callback_query(upd["callback_query"])
                    elif "edited_message" in upd:
                        self.handle_edited_message(upd["edited_message"])
                    elif "message" in upd:
                        self.handle_message(upd["message"])
            except (requests.RequestException, TelegramAPIError):
                logger.exception("Telegram polling failed")
                time.sleep(TELEGRAM_RETRY_DELAY)
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.exception("Unexpected error in main loop")
                time.sleep(TELEGRAM_RETRY_DELAY)

    def handle_message(self, message: dict[str, Any]):
        chat = message.get("chat", {})
        if chat.get("type") != "private":
            return

        tg_user_id = int(chat["id"])
        contact = message.get("contact")
        raw_text = str(message.get("text") or "")
        text = raw_text.strip()
        has_sticker = isinstance(message.get("sticker"), dict)
        pending_comment = self.state.get_pending_comment_request_for_tg(tg_user_id)
        waiting_request_input = self.state.is_waiting_request_input(tg_user_id)
        connection_request = self.state.get_connection_request(tg_user_id)

        if contact and self._handle_phone_gate_contact(tg_user_id, message, contact):
            return

        if pending_comment is None and self._should_request_phone(tg_user_id):
            self.state.mark_pending_phone_gate(tg_user_id)
            send_message(tg_user_id, PHONE_GATE_PROMPT_TEXT, self._phone_gate_keyboard())
            return

        if not text and not has_sticker:
            if waiting_request_input:
                send_message(tg_user_id, NON_TEXT_MESSAGE_TEXT, self._request_draft_keyboard())
            elif connection_request is not None:
                send_message(tg_user_id, NON_TEXT_MESSAGE_TEXT, self._connection_keyboard_for_step(connection_request))
            elif pending_comment is not None:
                send_message(tg_user_id, NON_TEXT_MESSAGE_TEXT, self._rating_comment_keyboard())
            else:
                self._send_main_message(tg_user_id, NON_TEXT_MESSAGE_TEXT)
            return

        if text == "/start":
            self._clear_pending_comment_request(pending_comment)
            self.state.clear_request_draft(tg_user_id)
            self.state.clear_connection_request(tg_user_id)
            self._send_main_message(tg_user_id, WELCOME_TEXT)
            return

        if text == MENU_SUPPORT_BUTTON_TEXT:
            self._clear_pending_comment_request(pending_comment)
            if self._has_open_chat(tg_user_id):
                self.state.clear_request_draft(tg_user_id)
                self._send_main_message(tg_user_id, DETAILS_PROMPT_TEXT)
            else:
                self.state.clear_connection_request(tg_user_id)
                self.state.mark_request_draft(tg_user_id)
                send_message(tg_user_id, DETAILS_PROMPT_TEXT, self._request_draft_keyboard())
            return

        if text == MENU_CONNECTION_BUTTON_TEXT:
            self._clear_pending_comment_request(pending_comment)
            self.state.clear_request_draft(tg_user_id)
            self.state.start_connection_request(tg_user_id)
            send_message(tg_user_id, CONNECTION_ROOM_PROMPT_TEXT, self._connection_room_keyboard())
            return

        if text == MENU_CLOSE_CHAT_BUTTON_TEXT:
            self._handle_close_chat_request(tg_user_id)
            return

        if text == REQUEST_CANCEL_BUTTON_TEXT:
            self.state.clear_request_draft(tg_user_id)
            self.state.clear_connection_request(tg_user_id)
            self._send_main_message(tg_user_id, WELCOME_TEXT, show_close_chat=False)
            return

        if text == RATING_RETURN_TO_MENU_BUTTON_TEXT:
            self._clear_pending_comment_request(pending_comment)
            self._send_main_message(tg_user_id, WELCOME_TEXT, show_close_chat=False)
            return

        if connection_request is not None:
            if not text:
                send_message(tg_user_id, NON_TEXT_MESSAGE_TEXT, self._connection_keyboard_for_step(connection_request))
                return
            self._handle_connection_request_step(tg_user_id, message, text, connection_request)
            return

        if pending_comment is not None:
            if not text:
                send_message(tg_user_id, NON_TEXT_MESSAGE_TEXT, self._rating_comment_keyboard())
                return
            self._handle_rating_comment(tg_user_id, text, pending_comment)
            return

        if not waiting_request_input and not self._has_open_chat(tg_user_id):
            self._send_main_message(tg_user_id, WELCOME_TEXT, show_close_chat=False)
            return

        try:
            outbound_message = self._build_subscriber_outbound_message(message)
            if outbound_message is None:
                if waiting_request_input:
                    send_message(tg_user_id, NON_TEXT_MESSAGE_TEXT, self._request_draft_keyboard())
                else:
                    self._send_main_message(tg_user_id, NON_TEXT_MESSAGE_TEXT)
                return
            subscriber_name = self._build_subscriber_name(message)
            support_chat_id = self._forward_subscriber_message(tg_user_id, subscriber_name, outbound_message)
            self.state.clear_request_draft(tg_user_id)
            if not self.state.is_welcomed(support_chat_id):
                self._send_main_message(tg_user_id, WAITING_TEXT)
                self.state.mark_welcomed(support_chat_id)
        except ZipAPIError as exc:
            if self._handle_phone_required_error(tg_user_id, exc):
                return
            logger.exception("Failed to send message to ZIP")
            self._send_main_message(tg_user_id, SEND_ERROR_TEXT)
        except (OSError, requests.RequestException, TelegramAPIError):
            logger.exception("Failed to prepare Telegram message payload for ZIP")
            self._send_main_message(tg_user_id, SEND_ERROR_TEXT)

    def handle_edited_message(self, message: dict[str, Any]):
        chat = message.get("chat", {})
        if chat.get("type") != "private":
            return

        tg_user_id = int(chat["id"])
        telegram_message_id = self._safe_int(message.get("message_id"))
        if telegram_message_id is None:
            return

        outbound_message = self._build_subscriber_outbound_message(message)
        if outbound_message is None:
            return

        source_message_ref = self._build_source_message_ref(tg_user_id, telegram_message_id)
        mapping = self.state.get_subscriber_message_mapping(source_message_ref)
        if mapping and mapping.get("deleted"):
            return

        chat_id = self._safe_int((mapping or {}).get("chatId"))
        if chat_id is None:
            chat_id = self.state.get_chat_by_tg(tg_user_id)
        if chat_id is None:
            logger.info("Skipping edited Telegram message without mapped support chat ref=%s", source_message_ref)
            return

        try:
            updated_message = self.zip.update_message_by_source_ref(
                chat_id,
                source_message_ref,
                text=outbound_message.get("text"),
                message_type=str(outbound_message.get("message_type") or ""),
                payload=outbound_message.get("payload"),
            )
            zip_message_id = self._safe_int((updated_message or {}).get("id")) or self._safe_int(
                (mapping or {}).get("zipMessageId")
            )
            self.state.upsert_subscriber_message_mapping(
                source_message_ref,
                chat_id=chat_id,
                telegram_chat_id=tg_user_id,
                telegram_message_id=telegram_message_id,
                zip_message_id=zip_message_id,
                message_type=str(outbound_message.get("message_type") or ""),
                deleted=False,
            )
        except (ZipAPIError, OSError, requests.RequestException, TelegramAPIError):
            logger.exception(
                "Failed to sync edited Telegram message tg_chat=%s tg_message=%s",
                tg_user_id,
                telegram_message_id,
            )

    def handle_callback_query(self, query: dict[str, Any]):
        callback_id = query.get("id")
        data = (query.get("data") or "").strip()
        if not callback_id:
            return
        if not data:
            answer_callback(callback_id)
            return

        try:
            callback = self._parse_rating_callback_data(data)
        except ValueError:
            answer_callback(callback_id)
            return

        message = query.get("message") or {}
        callback_chat = message.get("chat") or {}
        prompt_chat_id = message.get("chat", {}).get("id")
        prompt_message_id = message.get("message_id")
        tg_user_id = int((query.get("from") or {}).get("id") or 0)
        if callback_chat.get("type") not in {None, "private"}:
            answer_callback(callback_id)
            return
        if tg_user_id <= 0:
            answer_callback(callback_id, RATING_INVALID_BUTTON_TEXT)
            return

        current_pending = self.state.get_pending_rating(callback["chat_id"])
        if current_pending is None:
            request_data = {
                "chatId": callback["chat_id"],
                "subscriberTelegramId": tg_user_id,
                "requestToken": callback["request_token"],
                "telegramPromptMessageId": prompt_message_id,
            }
            self._sync_pending_rating_request(request_data, source="telegram:callback")
        elif current_pending.get("requestToken") == callback["request_token"] and prompt_message_id is not None:
            self.state.set_pending_rating_prompt_message(
                callback["chat_id"],
                int(prompt_message_id),
                request_token=callback["request_token"],
            )

        if callback["kind"] == "skip":
            self._handle_rating_skip_callback(
                callback_id=callback_id,
                prompt_chat_id=self._safe_int(prompt_chat_id),
                prompt_message_id=self._safe_int(prompt_message_id),
                chat_id=callback["chat_id"],
                request_token=callback["request_token"],
            )
            return

        self._handle_rating_submit_callback(
            callback_id=callback_id,
            tg_user_id=tg_user_id,
            prompt_chat_id=self._safe_int(prompt_chat_id),
            prompt_message_id=self._safe_int(prompt_message_id),
            chat_id=callback["chat_id"],
            request_token=callback["request_token"],
            score=callback["score"],
        )

    def handle_support_message_event(self, data: dict[str, Any]):
        message = self._extract_message_payload(data)
        if message is None:
            logger.warning("No message payload in support:new-message event: %s", data)
            return

        chat = self._extract_chat_payload(data)
        if chat is not None:
            self._sync_chat_snapshot(chat, source="support:new-message")

        chat_id = self._extract_chat_id(data, chat=chat)
        if chat_id is None:
            logger.warning("No chat id in support:new-message payload: %s", data)
            return

        sender_type = str(message.get("senderType") or "").lower()
        if sender_type == "subscriber":
            self._sync_subscriber_mapping_from_zip_message(message, chat_id)
            return
        if sender_type != "operator":
            return

        zip_message_id = self._safe_int(message.get("id"))
        if zip_message_id is not None and not self.state.mark_seen_operator_message(zip_message_id):
            return

        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        if tg_user_id is None:
            logger.warning("No Telegram user found for support chat %s", chat_id)
            return

        response = self._send_operator_mirror_message(tg_user_id, message)
        telegram_message_id = self._safe_int((response or {}).get("result", {}).get("message_id"))
        if zip_message_id is not None and telegram_message_id is not None:
            self.state.upsert_operator_message_mapping(
                zip_message_id,
                chat_id=chat_id,
                telegram_chat_id=tg_user_id,
                telegram_message_id=telegram_message_id,
                message_type=self._extract_support_message_type(message),
                source_message_ref=str(message.get("sourceMessageRef") or "").strip() or None,
                deleted=False,
            )

    def handle_message_updated_event(self, data: dict[str, Any]):
        message = self._extract_message_payload(data)
        if message is None:
            logger.warning("No message payload in support:message-updated event: %s", data)
            return

        chat = self._extract_chat_payload(data)
        if chat is not None:
            self._sync_chat_snapshot(chat, source="support:message-updated")

        chat_id = self._extract_chat_id(data, chat=chat)
        sender_type = str(message.get("senderType") or "").lower()
        if sender_type == "subscriber":
            self._sync_subscriber_mapping_from_zip_message(message, chat_id)
            return
        if sender_type != "operator":
            return

        zip_message_id = self._safe_int(message.get("id"))
        if zip_message_id is None:
            logger.warning("No message id in support:message-updated event: %s", data)
            return

        mapping = self.state.get_operator_message_mapping(zip_message_id)
        if mapping is None or mapping.get("deleted"):
            logger.info("Skipping operator message update without Telegram mapping zip_message=%s", zip_message_id)
            return

        telegram_chat_id = self._safe_int(mapping.get("telegramChatId"))
        telegram_message_id = self._safe_int(mapping.get("telegramMessageId"))
        mapped_chat_id = self._safe_int(mapping.get("chatId"))
        effective_chat_id = chat_id if chat_id is not None else mapped_chat_id
        if telegram_chat_id is None or telegram_message_id is None or effective_chat_id is None:
            return

        message_type = self._extract_support_message_type(message)
        previous_message_type = str(mapping.get("messageType") or "").strip().lower()
        if message_type == "text" and previous_message_type in {"", "text"}:
            text = self._build_operator_text(message)
            if not text:
                return
            try:
                edit_message(telegram_chat_id, telegram_message_id, text)
                self.state.upsert_operator_message_mapping(
                    zip_message_id,
                    chat_id=effective_chat_id,
                    telegram_chat_id=telegram_chat_id,
                    telegram_message_id=telegram_message_id,
                    message_type=message_type,
                    source_message_ref=str(message.get("sourceMessageRef") or "").strip() or None,
                    deleted=False,
                )
            except TelegramAPIError:
                logger.exception(
                    "Failed to edit Telegram mirror chat=%s message=%s for zip message=%s",
                    telegram_chat_id,
                    telegram_message_id,
                    zip_message_id,
                )
                self._replace_operator_mirror_message(
                    zip_message_id=zip_message_id,
                    message=message,
                    chat_id=effective_chat_id,
                    telegram_chat_id=telegram_chat_id,
                    old_telegram_message_id=telegram_message_id,
                )
            return

        self._replace_operator_mirror_message(
            zip_message_id=zip_message_id,
            message=message,
            chat_id=effective_chat_id,
            telegram_chat_id=telegram_chat_id,
            old_telegram_message_id=telegram_message_id,
        )

    def handle_message_deleted_event(self, data: dict[str, Any]):
        message = self._extract_message_payload(data)
        chat = self._extract_chat_payload(data)
        if chat is not None:
            self._sync_chat_snapshot(chat, source="support:message-deleted")

        chat_id = self._extract_chat_id(data, chat=chat)
        zip_message_id = self._extract_message_id(data, message=message)
        if zip_message_id is None:
            logger.warning("No message id in support:message-deleted event: %s", data)
            return

        sender_type = str((message or {}).get("senderType") or "").lower()
        operator_mapping = self.state.get_operator_message_mapping(zip_message_id)
        if sender_type == "operator" or operator_mapping is not None:
            if operator_mapping is None:
                return
            telegram_chat_id = self._safe_int(operator_mapping.get("telegramChatId"))
            telegram_message_id = self._safe_int(operator_mapping.get("telegramMessageId"))
            if telegram_chat_id is not None and telegram_message_id is not None:
                try:
                    delete_message(telegram_chat_id, telegram_message_id)
                except TelegramAPIError:
                    logger.exception(
                        "Failed to delete Telegram mirror chat=%s message=%s for zip message=%s",
                        telegram_chat_id,
                        telegram_message_id,
                        zip_message_id,
                    )
            self.state.mark_operator_message_deleted(zip_message_id)
            return

        if sender_type == "subscriber":
            source_message_ref = str((message or {}).get("sourceMessageRef") or "").strip() or None
            self.state.mark_subscriber_message_deleted(
                source_message_ref=source_message_ref,
                zip_message_id=zip_message_id,
            )
            return

        subscriber_mapping = self.state.get_subscriber_message_mapping_by_zip_message(zip_message_id)
        if subscriber_mapping is not None:
            self.state.mark_subscriber_message_deleted(zip_message_id=zip_message_id)

    def handle_chat_closed_event(self, data: dict[str, Any]):
        chat = self._extract_chat_payload(data) or {}
        previous, current = self._sync_chat_snapshot(chat, source="support:chat-closed")
        previous_status = (previous or {}).get("status")

        chat_id = self._extract_chat_id(data, chat=current or chat)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id, allow_remote_lookup=False)

        if tg_user_id is not None and previous_status != "closed":
            self.state.clear_chat(tg_user_id=tg_user_id)
            self._send_main_message(tg_user_id, CHAT_CLOSED_TEXT, show_close_chat=False)
        if chat_id is not None:
            self.state.clear_chat(chat_id=chat_id)
            self.state.clear_welcomed(chat_id)

    def handle_chat_status_event(self, data: dict[str, Any], text: str):
        chat = self._extract_chat_payload(data) or {}
        _, current = self._sync_chat_snapshot(chat, source="support:chat-status")
        chat_id = self._extract_chat_id(data, chat=current or chat)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        if tg_user_id is not None:
            self._send_main_message(tg_user_id, text)

    def handle_chat_reopened_event(self, data: dict[str, Any]):
        chat = self._extract_chat_payload(data) or {}
        previous, current = self._sync_chat_snapshot(chat, source="support:chat-reopened")
        chat_id = self._extract_chat_id(data, chat=current or chat)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        previous_status = (previous or {}).get("status")
        current_status = (current or {}).get("status")

        if chat_id is not None:
            self.state.clear_welcomed(chat_id)
            self.state.clear_pending_rating(chat_id)
        if tg_user_id is not None and previous_status != current_status:
            self._send_main_message(tg_user_id, CHAT_REOPENED_TEXT)

    def handle_chat_updated_event(self, data: dict[str, Any]):
        chat = self._extract_chat_payload(data)
        if chat is None:
            logger.warning("No chat snapshot in support:chat-updated payload: %s", data)
            return

        previous, current = self._sync_chat_snapshot(chat, source="support:chat-updated")
        if current is None:
            return

        current_status = current.get("status")
        if current_status not in {"waiting_customer", "resolved"}:
            return

        previous_status = data.get("previousStatus") or chat.get("previousStatus") or (previous or {}).get("status")
        if previous_status is None or previous_status == current_status:
            return

        chat_id = self._extract_chat_id(data, chat=current)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        if tg_user_id is None:
            logger.warning("No Telegram user found for support chat %s", chat_id)
            return

        workflow_text = WAITING_CUSTOMER_TEXT if current_status == "waiting_customer" else RESOLVED_TEXT
        self._send_main_message(tg_user_id, workflow_text)

    def handle_rating_requested_event(self, data: dict[str, Any]):
        request = self._extract_rating_request(data)
        if request is None:
            logger.warning("No rating request in support:rating-requested payload: %s", data)
            return

        _, current = self._sync_pending_rating_request(request, source="support:rating-requested")
        if current is None:
            logger.warning("Invalid rating request payload: %s", data)
            return

        self._ensure_rating_prompt(current, source="support:rating-requested")

    def handle_rating_updated_event(self, data: dict[str, Any]):
        chat = self._extract_chat_payload(data)
        if chat is not None:
            self._sync_chat_snapshot(chat, source="support:rating-updated")

        rating = data.get("rating")
        if not isinstance(rating, dict):
            logger.warning("No rating payload in support:rating-updated event: %s", data)
            return

        chat_id = self._extract_chat_id(data, chat=chat)
        if chat_id is None:
            logger.warning("No chat id in support:rating-updated event: %s", data)
            return

        current = self.state.get_pending_rating(chat_id)
        if current is None:
            return

        score = rating.get("score")
        if score is not None:
            self.state.mark_rating_score(
                chat_id,
                int(score),
                comment_requested=bool(current.get("commentRequested") and not rating.get("comment")),
            )

        comment = rating.get("comment")
        if comment:
            self.state.mark_rating_comment_submitted(chat_id, comment=str(comment))
            self.state.clear_pending_rating(chat_id)
            return

        status = str(rating.get("status") or "").lower()
        if status in {"skipped", "canceled", "expired"}:
            self.state.clear_pending_rating(chat_id)

    def _resolve_tg_user_id(self, support_chat_id: int) -> int | None:
        tg_user_id = self.state.get_tg_by_chat(support_chat_id)
        if tg_user_id is not None:
            return tg_user_id
        try:
            chat = self.zip.get_chat(support_chat_id)
        except ZipAPIError:
            logger.exception("Failed to resolve support chat %s", support_chat_id)
            return None
        self._sync_chat_snapshot(chat, source="get-chat")
        tg_user_id = self._extract_subscriber_tg_id(chat)
        return tg_user_id

    def _forward_subscriber_message(
        self,
        tg_user_id: int,
        subscriber_name: str,
        outbound_message: str | dict[str, Any],
    ) -> int:
        prepared_message = self._normalize_outbound_subscriber_message(outbound_message)
        subscriber_phone = self.state.get_subscriber_phone(tg_user_id)
        subscriber_avatar_url = self._ensure_subscriber_avatar_url(tg_user_id)
        chat_data = self.zip.ensure_chat(
            tg_user_id,
            subscriber_name,
            subscriber_phone=subscriber_phone,
            subscriber_avatar_url=subscriber_avatar_url,
        )
        _, current = self._sync_chat_snapshot(chat_data, source="ensure-chat")
        support_chat_id = self._extract_chat_id(chat_data, chat=current or chat_data)
        if support_chat_id is None:
            raise ZipAPIError("ZIP API returned chat payload without id")
        self.state.set_chat(tg_user_id, support_chat_id)

        try:
            created_message = self.zip.send_subscriber_message(
                support_chat_id,
                prepared_message.get("text"),
                message_type=str(prepared_message.get("message_type") or ""),
                payload=prepared_message.get("payload"),
                source_message_ref=str(prepared_message.get("source_message_ref") or "") or None,
            )
            self._record_subscriber_message_mapping(support_chat_id, prepared_message, created_message)
            return support_chat_id
        except ZipAPIError as exc:
            if not self._is_stale_chat_error(exc):
                raise

            logger.warning(
                "Support chat %s became stale while sending message, re-opening flow once",
                support_chat_id,
            )
            refreshed_chat = self.zip.ensure_chat(
                tg_user_id,
                subscriber_name,
                subscriber_phone=subscriber_phone,
                subscriber_avatar_url=subscriber_avatar_url,
            )
            _, refreshed_current = self._sync_chat_snapshot(refreshed_chat, source="ensure-chat-retry")
            refreshed_chat_id = self._extract_chat_id(refreshed_chat, chat=refreshed_current or refreshed_chat)
            if refreshed_chat_id is None:
                raise ZipAPIError("ZIP API returned retry chat payload without id") from exc
            self.state.set_chat(tg_user_id, refreshed_chat_id)

            created_message = self.zip.send_subscriber_message(
                refreshed_chat_id,
                prepared_message.get("text"),
                message_type=str(prepared_message.get("message_type") or ""),
                payload=prepared_message.get("payload"),
                source_message_ref=str(prepared_message.get("source_message_ref") or "") or None,
            )
            self._record_subscriber_message_mapping(refreshed_chat_id, prepared_message, created_message)
            return refreshed_chat_id

    def _handle_close_chat_request(self, tg_user_id: int):
        chat_id = self.state.get_chat_by_tg(tg_user_id)
        if chat_id is None:
            self._send_main_message(tg_user_id, NO_ACTIVE_CHAT_TEXT, show_close_chat=False)
            return

        try:
            chat_data = self.zip.close_chat(chat_id)
        except ZipAPIError as exc:
            details = exc.details_text().lower()
            if exc.status_code == 404 and ("endpoint" in details or "\u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e" in details):
                logger.warning(
                    "Close-chat endpoint is unavailable for bot API chat=%s tg=%s details=%s",
                    chat_id,
                    tg_user_id,
                    exc.details_text(),
                )
                self._send_main_message(tg_user_id, CHAT_CLOSE_UNAVAILABLE_TEXT, show_close_chat=True)
                return
            if self._is_stale_chat_error(exc):
                self.state.clear_chat(tg_user_id=tg_user_id)
                self.state.clear_chat(chat_id=chat_id)
                self.state.clear_welcomed(chat_id)
                self._send_main_message(tg_user_id, CHAT_CLOSED_TEXT, show_close_chat=False)
                return

            logger.exception(
                "Failed to close chat=%s for tg=%s details=%s",
                chat_id,
                tg_user_id,
                exc.details_text(),
            )
            self._send_main_message(tg_user_id, CHAT_CLOSE_ERROR_TEXT, show_close_chat=True)
            return

        previous, current = self._sync_chat_snapshot(chat_data, source="bot:close-chat")
        previous_status = (previous or {}).get("status")
        actual_chat_id = self._extract_chat_id(chat_data, chat=current or chat_data) or chat_id
        self.state.clear_chat(tg_user_id=tg_user_id)
        self.state.clear_chat(chat_id=actual_chat_id)
        self.state.clear_welcomed(actual_chat_id)
        if previous_status != "closed":
            self._send_main_message(tg_user_id, CHAT_CLOSED_TEXT, show_close_chat=False)

    def _connection_keyboard_for_step(self, connection_request: dict[str, Any]) -> dict[str, Any]:
        step = str(connection_request.get("step") or "")
        if step == "room_type":
            return self._connection_room_keyboard()
        if step == "continue":
            return self._connection_continue_keyboard()
        return self._request_draft_keyboard()

    def _handle_connection_request_step(
        self,
        tg_user_id: int,
        message: dict[str, Any],
        text: str,
        connection_request: dict[str, Any],
    ):
        step = str(connection_request.get("step") or "")

        if step == "room_type":
            if text not in {
                CONNECTION_ROOM_RESIDENTIAL_BUTTON_TEXT,
                CONNECTION_ROOM_NON_RESIDENTIAL_BUTTON_TEXT,
            }:
                send_message(tg_user_id, CONNECTION_ROOM_PROMPT_TEXT, self._connection_room_keyboard())
                return
            self.state.update_connection_request(
                tg_user_id,
                step="address",
                roomType=text,
            )
            send_message(tg_user_id, CONNECTION_ADDRESS_PROMPT_TEXT, self._request_draft_keyboard())
            return

        if step == "address":
            self.state.update_connection_request(
                tg_user_id,
                step="phone",
                address=text,
            )
            send_message(tg_user_id, CONNECTION_PHONE_PROMPT_TEXT, self._request_draft_keyboard())
            return

        if step == "phone":
            self.state.update_connection_request(
                tg_user_id,
                step="continue",
                phone=text,
            )
            send_message(tg_user_id, CONNECTION_CONTINUE_PROMPT_TEXT, self._connection_continue_keyboard())
            return

        if step != "continue":
            self.state.clear_connection_request(tg_user_id)
            self._send_main_message(tg_user_id, WELCOME_TEXT, show_close_chat=False)
            return

        if text == CONNECTION_CONTINUE_CHAT_BUTTON_TEXT:
            self._open_connection_chat_with_operator(tg_user_id, message, connection_request)
            return

        if text == CONNECTION_CONTINUE_CALL_BUTTON_TEXT:
            self._submit_connection_call_request(tg_user_id, message, connection_request)
            return

        send_message(tg_user_id, CONNECTION_CONTINUE_PROMPT_TEXT, self._connection_continue_keyboard())

    def _open_connection_chat_with_operator(
        self,
        tg_user_id: int,
        message: dict[str, Any],
        connection_request: dict[str, Any],
    ):
        subscriber_name = self._build_subscriber_name(message)
        request_text = self._format_connection_request_message(
            subscriber_name,
            tg_user_id,
            connection_request,
            continuation_label=CONNECTION_CONTINUE_CHAT_BUTTON_TEXT,
        )

        try:
            support_chat_id = self._forward_subscriber_message(tg_user_id, subscriber_name, request_text)
        except ZipAPIError as exc:
            if self._handle_phone_required_error(tg_user_id, exc):
                return
            logger.exception("Failed to open connection-request support chat")
            self._send_main_message(tg_user_id, SEND_ERROR_TEXT, show_close_chat=False)
            return

        self.state.clear_connection_request(tg_user_id)
        self.state.clear_request_draft(tg_user_id)
        if not self.state.is_welcomed(support_chat_id):
            self._send_main_message(tg_user_id, WAITING_TEXT, show_close_chat=True)
            self.state.mark_welcomed(support_chat_id)

    def _submit_connection_call_request(
        self,
        tg_user_id: int,
        message: dict[str, Any],
        connection_request: dict[str, Any],
    ):
        subscriber_name = self._build_subscriber_name(message)
        request_text = self._format_connection_request_message(
            subscriber_name,
            tg_user_id,
            connection_request,
            continuation_label=CONNECTION_CONTINUE_CALL_BUTTON_TEXT,
        )

        try:
            send_message(CONNECTION_CALL_TARGET_CHAT_ID, request_text)
        except (requests.RequestException, TelegramAPIError):
            logger.exception("Failed to forward connection call request to Telegram chat")
            self._send_main_message(tg_user_id, SEND_ERROR_TEXT, show_close_chat=False)
            return

        self.state.clear_connection_request(tg_user_id)
        self.state.clear_request_draft(tg_user_id)
        self._send_main_message(tg_user_id, CONNECTION_CALL_REQUEST_SENT_TEXT, show_close_chat=False)

    def _handle_rating_submit_callback(
        self,
        *,
        callback_id: str,
        tg_user_id: int,
        prompt_chat_id: int | None,
        prompt_message_id: int | None,
        chat_id: int,
        request_token: str,
        score: int,
    ):
        try:
            self.zip.submit_rating(chat_id, request_token, score)
        except ZipAPIError as exc:
            if self._handle_rating_request_error(
                exc,
                callback_id=callback_id,
                prompt_chat_id=prompt_chat_id,
                prompt_message_id=prompt_message_id,
                chat_id=chat_id,
                request_token=request_token,
                known_score=score,
            ):
                return
            logger.exception(
                "Failed to submit rating chat=%s score=%s token=%s details=%s",
                chat_id,
                score,
                request_token,
                exc.details_text(),
            )
            answer_callback(callback_id, RATING_TEMPORARY_ERROR_TEXT, show_alert=True)
            return

        if prompt_message_id is not None:
            self.state.set_pending_rating_prompt_message(chat_id, prompt_message_id, request_token=request_token)
        self.state.mark_rating_score(chat_id, score, request_token=request_token, comment_requested=True)

        self._try_edit_rating_prompt(prompt_chat_id, prompt_message_id, self._rating_thank_you_text(score))
        send_message(tg_user_id, RATING_COMMENT_PROMPT_TEXT, self._rating_comment_keyboard())
        answer_callback(callback_id)

    def _handle_rating_skip_callback(
        self,
        *,
        callback_id: str,
        prompt_chat_id: int | None,
        prompt_message_id: int | None,
        chat_id: int,
        request_token: str,
    ):
        try:
            self.zip.skip_rating(chat_id, request_token)
        except ZipAPIError as exc:
            if self._handle_rating_request_error(
                exc,
                callback_id=callback_id,
                prompt_chat_id=prompt_chat_id,
                prompt_message_id=prompt_message_id,
                chat_id=chat_id,
                request_token=request_token,
            ):
                return
            logger.exception(
                "Failed to skip rating chat=%s token=%s details=%s",
                chat_id,
                request_token,
                exc.details_text(),
            )
            answer_callback(callback_id, RATING_TEMPORARY_ERROR_TEXT, show_alert=True)
            return

        self.state.clear_pending_rating(chat_id, request_token=request_token)
        self._try_edit_rating_prompt(prompt_chat_id, prompt_message_id, RATING_SKIPPED_TEXT)
        answer_callback(callback_id)

    def _handle_rating_comment(self, tg_user_id: int, text: str, pending_rating: dict[str, Any]):
        chat_id = int(pending_rating["chatId"])
        request_token = str(pending_rating["requestToken"])
        try:
            self.zip.patch_rating_comment(chat_id, request_token, text)
        except ZipAPIError as exc:
            mapped_text = self._map_rating_error(exc)
            if mapped_text is not None:
                self._send_main_message(tg_user_id, mapped_text, show_close_chat=False)
                self.state.clear_pending_rating(chat_id, request_token=request_token)
                return
            logger.exception(
                "Failed to submit rating comment chat=%s token=%s details=%s",
                chat_id,
                request_token,
                exc.details_text(),
            )
            self._send_main_message(tg_user_id, RATING_TEMPORARY_ERROR_TEXT, show_close_chat=False)
            return

        self.state.mark_rating_comment_submitted(chat_id, request_token=request_token, comment=text)
        self.state.clear_pending_rating(chat_id, request_token=request_token)
        self._send_main_message(tg_user_id, RATING_COMMENT_THANK_YOU_TEXT, show_close_chat=False)

    def _recover_pending_ratings(self):
        try:
            page = 1
            recovered = 0
            while True:
                items = self.zip.get_pending_ratings(page=page, page_size=50)
                if not items:
                    break

                for request in items:
                    _, current = self._sync_pending_rating_request(request, source=f"ratings/pending:{page}")
                    if current is None:
                        continue
                    if current.get("telegramPromptMessageId"):
                        continue
                    if self._ensure_rating_prompt(current, source=f"ratings/pending:{page}"):
                        recovered += 1

                if len(items) < 50:
                    break
                page += 1

            logger.info("Recovered %s pending rating prompts", recovered)
        except (requests.RequestException, TelegramAPIError, ZipAPIError):
            logger.exception("Failed to recover pending rating requests")

    def _resolve_event_tg_user_id(
        self,
        chat: dict[str, Any] | None,
        chat_id: int | None,
        *,
        allow_remote_lookup: bool = True,
    ) -> int | None:
        if chat:
            tg_user_id = self._extract_subscriber_tg_id(chat)
            if tg_user_id is not None:
                return tg_user_id

        if chat_id is None:
            return None

        tg_user_id = self.state.get_tg_by_chat(chat_id)
        if tg_user_id is not None or not allow_remote_lookup:
            return tg_user_id

        return self._resolve_tg_user_id(chat_id)

    def _sync_chat_snapshot(
        self,
        chat_data: dict[str, Any] | None,
        *,
        source: str,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        if not chat_data:
            return None, None

        previous, current = self.state.sync_chat(chat_data)
        if current is not None:
            logger.info(
                "Synced chat %s via %s status=%s priority=%s topic=%r",
                current.get("id"),
                source,
                current.get("status"),
                current.get("priority"),
                current.get("topic"),
            )
        return previous, current

    @staticmethod
    def _normalize_outbound_subscriber_message(outbound_message: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(outbound_message, str):
            return {
                "text": outbound_message,
                "message_type": "text",
                "payload": None,
                "source_message_ref": None,
                "telegram_chat_id": None,
                "telegram_message_id": None,
            }

        normalized = dict(outbound_message)
        normalized.setdefault("message_type", "text" if normalized.get("text") is not None else None)
        normalized.setdefault("payload", None)
        normalized.setdefault("source_message_ref", None)
        normalized.setdefault("telegram_chat_id", None)
        normalized.setdefault("telegram_message_id", None)
        return normalized

    def _record_subscriber_message_mapping(
        self,
        chat_id: int,
        outbound_message: dict[str, Any],
        created_message: dict[str, Any] | None,
    ):
        source_message_ref = str(outbound_message.get("source_message_ref") or "").strip()
        telegram_chat_id = self._safe_int(outbound_message.get("telegram_chat_id"))
        telegram_message_id = self._safe_int(outbound_message.get("telegram_message_id"))
        if not source_message_ref or telegram_chat_id is None or telegram_message_id is None:
            return

        created_payload = self._extract_message_payload({"message": created_message or {}})
        if created_payload is None and isinstance(created_message, dict):
            created_payload = created_message
        zip_message_id = self._safe_int((created_payload or {}).get("id"))
        self.state.upsert_subscriber_message_mapping(
            source_message_ref,
            chat_id=chat_id,
            telegram_chat_id=telegram_chat_id,
            telegram_message_id=telegram_message_id,
            zip_message_id=zip_message_id,
            message_type=str(outbound_message.get("message_type") or ""),
            deleted=False,
        )

    def _build_subscriber_outbound_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        if isinstance(message.get("sticker"), dict):
            return self._build_subscriber_sticker_message(message)
        if message.get("text") is not None:
            return self._build_subscriber_text_message(message)
        return None

    @staticmethod
    def _build_source_message_ref(telegram_chat_id: int, telegram_message_id: int) -> str:
        return f"telegram:{telegram_chat_id}:{telegram_message_id}"

    def _build_subscriber_text_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        chat = message.get("chat") or {}
        telegram_chat_id = self._safe_int(chat.get("id"))
        telegram_message_id = self._safe_int(message.get("message_id"))
        text = str(message.get("text") or "")
        if telegram_chat_id is None or telegram_message_id is None or not text:
            return None

        return {
            "text": text,
            "message_type": "text",
            "payload": {
                "customEmojiEntities": self._extract_custom_emoji_entities(text, message.get("entities")),
            },
            "source_message_ref": self._build_source_message_ref(telegram_chat_id, telegram_message_id),
            "telegram_chat_id": telegram_chat_id,
            "telegram_message_id": telegram_message_id,
        }

    def _build_subscriber_sticker_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        chat = message.get("chat") or {}
        telegram_chat_id = self._safe_int(chat.get("id"))
        telegram_message_id = self._safe_int(message.get("message_id"))
        sticker = message.get("sticker")
        if telegram_chat_id is None or telegram_message_id is None or not isinstance(sticker, dict):
            return None

        sticker_payload = self._build_sticker_payload_from_telegram_sticker(sticker)
        if sticker_payload is None:
            return None

        return {
            "text": None,
            "message_type": "sticker",
            "payload": {"sticker": sticker_payload},
            "source_message_ref": self._build_source_message_ref(telegram_chat_id, telegram_message_id),
            "telegram_chat_id": telegram_chat_id,
            "telegram_message_id": telegram_message_id,
        }

    def _extract_custom_emoji_entities(self, text: str, entities: Any) -> list[dict[str, Any]]:
        if not isinstance(entities, list):
            return []

        items: list[dict[str, Any]] = []
        for entity in entities:
            if not isinstance(entity, dict) or str(entity.get("type") or "") != "custom_emoji":
                continue
            offset = self._safe_int(entity.get("offset"))
            length = self._safe_int(entity.get("length"))
            custom_emoji_id = str(entity.get("custom_emoji_id") or "").strip()
            if offset is None or length is None or length <= 0 or not custom_emoji_id:
                continue
            items.append(
                {
                    "offset": offset,
                    "length": length,
                    "emojiId": custom_emoji_id,
                    "alt": self._slice_text_by_utf16(text, offset, length),
                }
            )
        return items

    @staticmethod
    def _slice_text_by_utf16(text: str, offset: int, length: int) -> str:
        encoded = text.encode("utf-16-le")
        start = max(0, int(offset) * 2)
        end = max(start, (int(offset) + int(length)) * 2)
        if start >= len(encoded):
            return ""
        return encoded[start:end].decode("utf-16-le", errors="ignore")

    def _build_sticker_payload_from_telegram_sticker(self, sticker: dict[str, Any]) -> dict[str, Any] | None:
        if not self.sticker_public_base_url:
            logger.warning("Skipping Telegram sticker sync because sticker cache is not configured")
            return None

        file_id = str(sticker.get("file_id") or "").strip()
        if not file_id:
            return None

        file_payload = get_file(file_id)
        file_info = file_payload.get("result") or {}
        telegram_file_path = str(file_info.get("file_path") or "").strip()
        if not telegram_file_path:
            raise TelegramAPIError("Telegram getFile returned empty sticker file_path")

        content, content_type = download_file(telegram_file_path)
        file_name = self._store_sticker_file(sticker, telegram_file_path, content, content_type=content_type)
        sticker_format = self._detect_telegram_sticker_format(sticker)

        payload: dict[str, Any] = {
            "url": self._build_sticker_public_url(file_name),
            "format": sticker_format,
        }
        if sticker_format == "static":
            payload["previewUrl"] = payload["url"]

        emoji = str(sticker.get("emoji") or "").strip()
        if emoji:
            payload["emoji"] = emoji

        set_name = str(sticker.get("set_name") or "").strip()
        if set_name:
            payload["setName"] = set_name

        width = self._safe_int(sticker.get("width"))
        height = self._safe_int(sticker.get("height"))
        if width is not None:
            payload["width"] = width
        if height is not None:
            payload["height"] = height

        return payload

    def _sync_subscriber_mapping_from_zip_message(self, message: dict[str, Any], chat_id: int | None):
        source_message_ref = str(message.get("sourceMessageRef") or "").strip()
        zip_message_id = self._safe_int(message.get("id"))
        if not source_message_ref:
            return

        current = self.state.get_subscriber_message_mapping(source_message_ref)
        if current is None:
            return

        mapped_chat_id = self._safe_int(current.get("chatId"))
        telegram_chat_id = self._safe_int(current.get("telegramChatId"))
        telegram_message_id = self._safe_int(current.get("telegramMessageId"))
        if telegram_chat_id is None or telegram_message_id is None:
            return

        effective_chat_id = chat_id if chat_id is not None else mapped_chat_id
        if effective_chat_id is None:
            return

        self.state.upsert_subscriber_message_mapping(
            source_message_ref,
            chat_id=effective_chat_id,
            telegram_chat_id=telegram_chat_id,
            telegram_message_id=telegram_message_id,
            zip_message_id=zip_message_id,
            message_type=self._extract_support_message_type(message),
            deleted=self._is_support_message_deleted(message),
        )

    @staticmethod
    def _extract_support_message_type(message: dict[str, Any]) -> str:
        message_type = str(message.get("messageType") or message.get("type") or "").strip().lower()
        if message_type:
            return message_type

        payload = message.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("sticker"), dict):
            return "sticker"
        if isinstance(message.get("sticker"), dict):
            return "sticker"
        return "text"

    @staticmethod
    def _is_support_message_deleted(message: dict[str, Any]) -> bool:
        return bool(message.get("isDeleted") or message.get("deletedAt") or message.get("deleted"))

    def _build_operator_text(self, message: dict[str, Any]) -> str:
        text = str(message.get("text") or "").strip()
        if not text:
            return ""
        return f"{OPERATOR_MESSAGE_PREFIX}{text}"

    def _send_operator_mirror_message(self, tg_user_id: int, message: dict[str, Any]) -> dict[str, Any] | None:
        if self._extract_support_message_type(message) == "sticker":
            return self._send_operator_sticker_message(tg_user_id, message)

        text = self._build_operator_text(message)
        if not text:
            return None
        return send_message(tg_user_id, text)

    def _send_operator_sticker_message(self, tg_user_id: int, message: dict[str, Any]) -> dict[str, Any] | None:
        payload = message.get("payload")
        sticker = payload.get("sticker") if isinstance(payload, dict) else None
        if not isinstance(sticker, dict):
            return None

        sticker_format = str(sticker.get("format") or "static").strip().lower()
        sticker_url = self._select_operator_sticker_url(sticker, sticker_format=sticker_format)
        if not sticker_url:
            return None

        emoji = str(sticker.get("emoji") or "").strip() or None
        if sticker_format == "static":
            return send_sticker(tg_user_id, sticker_url, emoji=emoji)

        file_name, content = self._download_remote_sticker_asset(sticker_url, sticker_format=sticker_format)
        return upload_sticker(tg_user_id, file_name, content, emoji=emoji)

    def _replace_operator_mirror_message(
        self,
        *,
        zip_message_id: int,
        message: dict[str, Any],
        chat_id: int,
        telegram_chat_id: int,
        old_telegram_message_id: int,
    ):
        response = self._send_operator_mirror_message(telegram_chat_id, message)
        new_telegram_message_id = self._safe_int((response or {}).get("result", {}).get("message_id"))
        if new_telegram_message_id is None:
            return

        try:
            delete_message(telegram_chat_id, old_telegram_message_id)
        except TelegramAPIError:
            logger.exception(
                "Failed to delete old Telegram mirror chat=%s message=%s for zip message=%s",
                telegram_chat_id,
                old_telegram_message_id,
                zip_message_id,
            )

        self.state.upsert_operator_message_mapping(
            zip_message_id,
            chat_id=chat_id,
            telegram_chat_id=telegram_chat_id,
            telegram_message_id=new_telegram_message_id,
            message_type=self._extract_support_message_type(message),
            source_message_ref=str(message.get("sourceMessageRef") or "").strip() or None,
            deleted=False,
        )

    @staticmethod
    def _select_operator_sticker_url(sticker: dict[str, Any], *, sticker_format: str) -> str | None:
        if sticker_format in {"animated", "video"}:
            value = sticker.get("animationUrl") or sticker.get("url")
        else:
            value = sticker.get("url")
        normalized = str(value or "").strip()
        return normalized or None

    def _download_remote_sticker_asset(self, sticker_url: str, *, sticker_format: str) -> tuple[str, bytes]:
        resp = requests.get(sticker_url, timeout=30)
        resp.raise_for_status()
        extension = Path(sticker_url.split("?", 1)[0]).suffix.lower()
        if not extension:
            extension = {
                "animated": ".tgs",
                "video": ".webm",
            }.get(sticker_format, ".webp")
        file_name = f"operator_sticker_{int(time.time() * 1000)}{extension}"
        return file_name, resp.content

    @staticmethod
    def _detect_telegram_sticker_format(sticker: dict[str, Any]) -> str:
        if sticker.get("is_video"):
            return "video"
        if sticker.get("is_animated"):
            return "animated"
        return "static"

    def _store_sticker_file(
        self,
        sticker: dict[str, Any],
        telegram_file_path: str,
        content: bytes,
        *,
        content_type: str | None = None,
    ) -> str:
        extension = self._infer_sticker_extension(sticker, telegram_file_path, content_type=content_type)
        raw_token = str(sticker.get("file_unique_id") or sticker.get("file_id") or int(time.time() * 1000))
        safe_token = "".join(char for char in raw_token if char.isalnum() or char in {"-", "_"})
        file_name = f"sticker_{safe_token or int(time.time() * 1000)}{extension}"
        destination = self._sticker_path(file_name)
        temp_destination = destination.with_name(f"{destination.name}.tmp")

        self.sticker_cache_dir.mkdir(parents=True, exist_ok=True)
        temp_destination.write_bytes(content)
        temp_destination.replace(destination)
        return file_name

    def _build_sticker_public_url(self, file_name: str) -> str:
        return f"{self.sticker_public_base_url}/{quote(file_name)}"

    def _sticker_path(self, file_name: str) -> Path:
        return self.sticker_cache_dir / file_name

    @staticmethod
    def _infer_sticker_extension(
        sticker: dict[str, Any],
        telegram_file_path: str,
        *,
        content_type: str | None = None,
    ) -> str:
        extension = Path(telegram_file_path).suffix.lower()
        if extension in {".webp", ".tgs", ".webm", ".png"}:
            return extension

        sticker_format = SupportBot._detect_telegram_sticker_format(sticker)
        if sticker_format == "animated":
            return ".tgs"
        if sticker_format == "video":
            return ".webm"

        content_type_map = {
            "image/webp": ".webp",
            "application/x-tgsticker": ".tgs",
            "application/gzip": ".tgs",
            "video/webm": ".webm",
            "image/png": ".png",
        }
        normalized_content_type = str(content_type or "").split(";", 1)[0].strip().lower()
        return content_type_map.get(normalized_content_type, ".webp")

    def _ensure_subscriber_avatar_url(self, tg_user_id: int) -> str | None:
        avatar_meta = self.state.get_subscriber_avatar(tg_user_id)
        cached_url = self._get_cached_avatar_url(avatar_meta)
        refresh_due = self._is_avatar_refresh_due(avatar_meta)

        if cached_url and not refresh_due:
            return cached_url
        if avatar_meta and avatar_meta.get("hasAvatar") is False and not refresh_due:
            return None
        if not self.avatar_public_base_url:
            return cached_url

        try:
            payload = get_user_profile_photos(tg_user_id, limit=1)
            result = payload.get("result") or {}
            photos = result.get("photos") or []
            if not photos:
                self._delete_cached_avatar_file(avatar_meta)
                self.state.set_subscriber_avatar(tg_user_id, has_avatar=False)
                return None

            photo = self._select_profile_photo(photos[0])
            file_id = str((photo or {}).get("file_id") or "").strip()
            if not file_id:
                self._delete_cached_avatar_file(avatar_meta)
                self.state.set_subscriber_avatar(tg_user_id, has_avatar=False)
                return None

            file_payload = get_file(file_id)
            file_info = file_payload.get("result") or {}
            telegram_file_path = str(file_info.get("file_path") or "").strip()
            if not telegram_file_path:
                raise TelegramAPIError("Telegram getFile returned empty file_path")

            content, content_type = download_file(telegram_file_path)
            file_name = self._store_subscriber_avatar(
                tg_user_id,
                telegram_file_path,
                content,
                content_type=content_type,
                avatar_meta=avatar_meta,
            )
            avatar_url = self._build_avatar_public_url(file_name)
            self.state.set_subscriber_avatar(
                tg_user_id,
                url=avatar_url,
                file_name=file_name,
                has_avatar=True,
            )
            return avatar_url
        except (OSError, requests.RequestException, TelegramAPIError):
            logger.exception("Failed to sync subscriber avatar for Telegram user %s", tg_user_id)
            return cached_url

    def _get_cached_avatar_url(self, avatar_meta: dict[str, Any] | None) -> str | None:
        if not isinstance(avatar_meta, dict):
            return None

        avatar_url = str(avatar_meta.get("url") or "").strip()
        file_name = str(avatar_meta.get("fileName") or "").strip()
        if not avatar_url or not file_name:
            return None

        if self._avatar_path(file_name).exists():
            return avatar_url

        logger.warning("Cached avatar file is missing for %s", file_name)
        return None

    def _is_avatar_refresh_due(self, avatar_meta: dict[str, Any] | None) -> bool:
        if not isinstance(avatar_meta, dict):
            return True

        checked_at = self._safe_int(avatar_meta.get("checkedAt"))
        if checked_at is None:
            return True
        if self.avatar_refresh_seconds <= 0:
            return False
        return (int(time.time()) - checked_at) >= self.avatar_refresh_seconds

    def _store_subscriber_avatar(
        self,
        tg_user_id: int,
        telegram_file_path: str,
        content: bytes,
        *,
        content_type: str | None = None,
        avatar_meta: dict[str, Any] | None = None,
    ) -> str:
        extension = self._infer_avatar_extension(telegram_file_path, content_type=content_type)
        file_name = f"{tg_user_id}{extension}"
        destination = self._avatar_path(file_name)
        temp_destination = destination.with_name(f"{destination.name}.tmp")

        self.avatar_cache_dir.mkdir(parents=True, exist_ok=True)
        temp_destination.write_bytes(content)
        temp_destination.replace(destination)

        previous_file_name = str((avatar_meta or {}).get("fileName") or "").strip()
        if previous_file_name and previous_file_name != file_name:
            self._avatar_path(previous_file_name).unlink(missing_ok=True)

        return file_name

    def _build_avatar_public_url(self, file_name: str) -> str:
        return f"{self.avatar_public_base_url}/{quote(file_name)}"

    def _avatar_path(self, file_name: str) -> Path:
        return self.avatar_cache_dir / file_name

    def _delete_cached_avatar_file(self, avatar_meta: dict[str, Any] | None):
        file_name = str((avatar_meta or {}).get("fileName") or "").strip()
        if file_name:
            self._avatar_path(file_name).unlink(missing_ok=True)

    @staticmethod
    def _infer_avatar_extension(telegram_file_path: str, *, content_type: str | None = None) -> str:
        extension = Path(telegram_file_path).suffix.lower()
        if extension in {".jpg", ".jpeg", ".png", ".webp"}:
            return extension

        content_type_map = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
        }
        normalized_content_type = str(content_type or "").split(";", 1)[0].strip().lower()
        return content_type_map.get(normalized_content_type, ".jpg")

    @staticmethod
    def _select_profile_photo(photo_sizes: Any) -> dict[str, Any] | None:
        if not isinstance(photo_sizes, list):
            return None

        candidates = [item for item in photo_sizes if isinstance(item, dict) and item.get("file_id")]
        if not candidates:
            return None

        return max(
            candidates,
            key=lambda item: (
                int(item.get("file_size") or 0),
                int(item.get("width") or 0) * int(item.get("height") or 0),
            ),
        )

    def _should_request_phone(self, tg_user_id: int) -> bool:
        if self._has_open_chat(tg_user_id):
            return False
        return not self.state.has_subscriber_phone(tg_user_id)

    def _handle_phone_gate_contact(
        self,
        tg_user_id: int,
        message: dict[str, Any],
        contact: dict[str, Any],
    ) -> bool:
        if not self.state.is_waiting_phone_gate(tg_user_id) and not self._should_request_phone(tg_user_id):
            return False

        contact_user_id = self._safe_int(contact.get("user_id"))
        from_user_id = self._safe_int((message.get("from") or {}).get("id"))
        if contact_user_id is None or from_user_id is None or contact_user_id != from_user_id:
            send_message(tg_user_id, PHONE_GATE_INVALID_CONTACT_TEXT, self._phone_gate_keyboard())
            return True

        normalized_phone = self._normalize_phone(contact.get("phone_number"))
        if not normalized_phone:
            send_message(tg_user_id, PHONE_GATE_PROMPT_TEXT, self._phone_gate_keyboard())
            return True

        self.state.set_subscriber_phone(tg_user_id, normalized_phone)
        self.state.clear_pending_phone_gate(tg_user_id)
        self._send_main_message(tg_user_id, PHONE_GATE_SUCCESS_TEXT, show_close_chat=False)
        return True

    def _handle_phone_required_error(self, tg_user_id: int, exc: ZipAPIError) -> bool:
        if not self._is_phone_required_error(exc):
            return False
        self.state.mark_pending_phone_gate(tg_user_id)
        send_message(tg_user_id, PHONE_GATE_PROMPT_TEXT, self._phone_gate_keyboard())
        return True

    def _sync_pending_rating_request(
        self,
        request_data: dict[str, Any],
        *,
        source: str,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        previous, current = self.state.sync_pending_rating(request_data)
        if current is not None:
            logger.info(
                "Synced pending rating chat=%s token=%s via %s prompt=%s score=%s",
                current.get("chatId"),
                current.get("requestToken"),
                source,
                current.get("telegramPromptMessageId"),
                current.get("score"),
            )
        return previous, current

    def _ensure_rating_prompt(self, pending_rating: dict[str, Any], *, source: str) -> bool:
        chat_id = self._safe_int(pending_rating.get("chatId"))
        tg_user_id = self._safe_int(pending_rating.get("subscriberTelegramId"))
        request_token = pending_rating.get("requestToken")
        if chat_id is None or tg_user_id is None or not request_token:
            return False

        if pending_rating.get("telegramPromptMessageId"):
            return False

        response = send_message(
            tg_user_id,
            RATING_PROMPT_TEXT,
            self._build_rating_keyboard(chat_id, str(request_token)),
        )
        message_id = self._safe_int((response.get("result") or {}).get("message_id"))
        if message_id is not None:
            self.state.set_pending_rating_prompt_message(chat_id, message_id, request_token=str(request_token))
        logger.info("Sent rating prompt for chat %s via %s", chat_id, source)
        return True

    def _build_rating_keyboard(self, chat_id: int, request_token: str) -> dict[str, Any]:
        stars = [ibtn("\u2b50", f"sr:{chat_id}:{request_token}:{score}") for score in range(1, 6)]
        return kb_inline(
            [
                stars,
                [ibtn(RATING_SKIP_BUTTON_TEXT, f"srskip:{chat_id}:{request_token}")],
            ]
        )

    def _handle_rating_request_error(
        self,
        exc: ZipAPIError,
        *,
        callback_id: str,
        prompt_chat_id: int | None,
        prompt_message_id: int | None,
        chat_id: int,
        request_token: str,
        known_score: int | None = None,
    ) -> bool:
        mapped_text = self._map_rating_error(exc)
        if mapped_text is None:
            return False

        answer_callback(callback_id, mapped_text)

        if mapped_text == RATING_ALREADY_SUBMITTED_TEXT:
            score = known_score
            current = self.state.get_pending_rating(chat_id)
            if current is not None and current.get("score") is not None:
                score = int(current["score"])
            if score is not None:
                self.state.mark_rating_score(chat_id, score, request_token=request_token, comment_requested=True)
                self._try_edit_rating_prompt(prompt_chat_id, prompt_message_id, self._rating_thank_you_text(score))
            return True

        self._try_edit_rating_prompt(prompt_chat_id, prompt_message_id, mapped_text)
        self.state.clear_pending_rating(chat_id, request_token=request_token)
        return True

    def _try_edit_rating_prompt(self, chat_id: int | None, message_id: int | None, text: str):
        if chat_id is None or message_id is None:
            return
        try:
            edit_message(chat_id, message_id, text, reply_markup=kb_inline([]))
        except (requests.RequestException, TelegramAPIError):
            logger.exception("Failed to edit Telegram rating prompt chat=%s message=%s", chat_id, message_id)

    def _clear_pending_comment_request(self, pending_comment: dict[str, Any] | None):
        if pending_comment is None:
            return
        chat_id = self._safe_int(pending_comment.get("chatId"))
        request_token = pending_comment.get("requestToken")
        if chat_id is not None and request_token:
            self.state.clear_pending_rating(chat_id, request_token=str(request_token))

    @staticmethod
    def _map_rating_error(exc: ZipAPIError) -> str | None:
        details = exc.details_text().lower()
        if "invalid" in details and "token" in details:
            return RATING_INVALID_BUTTON_TEXT
        if "expired" in details:
            return RATING_EXPIRED_TEXT
        if "canceled" in details or "not closed" in details:
            return RATING_CANCELED_TEXT
        if "already submitted" in details:
            return RATING_ALREADY_SUBMITTED_TEXT
        return None

    @staticmethod
    def _parse_rating_callback_data(data: str) -> dict[str, Any]:
        if data.startswith("srskip:"):
            parts = data.split(":", 2)
            if len(parts) != 3:
                raise ValueError("Invalid skip callback data")
            return {
                "kind": "skip",
                "chat_id": int(parts[1]),
                "request_token": parts[2],
            }

        if data.startswith("sr:"):
            parts = data.split(":", 3)
            if len(parts) != 4:
                raise ValueError("Invalid score callback data")
            score = int(parts[3])
            if score < 1 or score > 5:
                raise ValueError("Invalid rating score")
            return {
                "kind": "score",
                "chat_id": int(parts[1]),
                "request_token": parts[2],
                "score": score,
            }

        raise ValueError("Unsupported callback data")

    @staticmethod
    def _extract_rating_request(data: dict[str, Any]) -> dict[str, Any] | None:
        def looks_like_request(candidate: Any) -> bool:
            if not isinstance(candidate, dict):
                return False
            required = ("chatId", "subscriberTelegramId", "requestToken")
            return all(candidate.get(field) is not None for field in required)

        direct_request = data.get("request")
        if looks_like_request(direct_request):
            return direct_request

        nested = data.get("data")
        if isinstance(nested, dict):
            nested_request = nested.get("request")
            if looks_like_request(nested_request):
                return nested_request
            if looks_like_request(nested):
                return nested

        if looks_like_request(data):
            return data

        return None

    @staticmethod
    def _is_stale_chat_error(exc: ZipAPIError) -> bool:
        details = exc.details_text().lower()
        if exc.status_code in {409, 410}:
            return True
        if exc.status_code in {400, 404, 422} and "chat" in details and any(
            marker in details for marker in ("closed", "stale", "not found")
        ):
            return True
        return False

    @staticmethod
    def _is_phone_required_error(exc: ZipAPIError) -> bool:
        return "subscriber phone is required before creating a chat" in exc.details_text().lower()

    @staticmethod
    def _extract_message_payload(data: dict[str, Any]) -> dict[str, Any] | None:
        def looks_like_message(candidate: Any) -> bool:
            if not isinstance(candidate, dict):
                return False
            if candidate.get("senderType") is not None:
                return True
            if candidate.get("sourceMessageRef") is not None:
                return True
            if candidate.get("messageType") is not None:
                return True
            payload = candidate.get("payload")
            return isinstance(payload, dict) and bool(payload)

        candidates = []
        direct_message = data.get("message")
        if looks_like_message(direct_message):
            candidates.append(direct_message)

        nested = data.get("data")
        if isinstance(nested, dict):
            nested_message = nested.get("message")
            if looks_like_message(nested_message):
                candidates.append(nested_message)
            if looks_like_message(nested):
                candidates.append(nested)

        if looks_like_message(data):
            candidates.append(data)

        return candidates[0] if candidates else None

    @staticmethod
    def _extract_message_id(data: dict[str, Any], *, message: dict[str, Any] | None = None) -> int | None:
        value = data.get("messageId")
        if value is None and isinstance(message, dict):
            value = message.get("id")
        if value is None:
            nested = data.get("data")
            if isinstance(nested, dict):
                value = nested.get("messageId")
                if value is None:
                    nested_message = nested.get("message")
                    if isinstance(nested_message, dict):
                        value = nested_message.get("id")
        return int(value) if value is not None else None

    @staticmethod
    def _extract_chat_payload(data: dict[str, Any]) -> dict[str, Any] | None:
        def looks_like_chat(candidate: Any) -> bool:
            if not isinstance(candidate, dict):
                return False
            if any(key in candidate for key in ("subscriberTelegramId", "status", "priority", "topic", "closeReason")):
                return True
            return "id" in candidate and "updatedAt" in candidate

        candidates = []
        direct_chat = data.get("chat")
        if looks_like_chat(direct_chat):
            candidates.append(direct_chat)

        nested = data.get("data")
        if isinstance(nested, dict):
            nested_chat = nested.get("chat")
            if looks_like_chat(nested_chat):
                candidates.append(nested_chat)
            if looks_like_chat(nested):
                candidates.append(nested)

        if looks_like_chat(data):
            candidates.append(data)

        return candidates[0] if candidates else None

    @staticmethod
    def _extract_chat_id(data: dict[str, Any], *, chat: dict[str, Any] | None = None) -> int | None:
        value = data.get("chatId")
        if value is None:
            message = data.get("message")
            if isinstance(message, dict):
                value = message.get("chatId")
        if value is None:
            nested = data.get("data")
            if isinstance(nested, dict):
                value = nested.get("chatId")
                if value is None:
                    nested_message = nested.get("message")
                    if isinstance(nested_message, dict):
                        value = nested_message.get("chatId")
        if value is None and chat is not None:
            value = chat.get("id")
        return int(value) if value is not None else None

    @staticmethod
    def _build_subscriber_name(message: dict[str, Any]) -> str:
        frm = message.get("from", {})
        username = frm.get("username")
        full_name = " ".join(
            part for part in [frm.get("first_name", "").strip(), frm.get("last_name", "").strip()] if part
        ).strip()
        if username and full_name:
            return f"{full_name} (@{username})"
        if username:
            return f"@{username}"
        return full_name or str(frm.get("id", "Subscriber"))

    @staticmethod
    def _extract_subscriber_tg_id(chat_data: dict[str, Any]) -> int | None:
        value = chat_data.get("subscriberTelegramId")
        return int(value) if value is not None else None

    @staticmethod
    def _rating_thank_you_text(score: int) -> str:
        return f"\u0414\u044f\u043a\u0443\u0454\u043c\u043e \u0437\u0430 \u043e\u0446\u0456\u043d\u043a\u0443: {score}/5"

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_phone(value: Any) -> str | None:
        if value is None:
            return None
        phone = str(value).strip()
        if not phone:
            return None
        if phone.startswith("+"):
            return phone
        return f"+{phone}"


if __name__ == "__main__":
    SupportBot().run()
