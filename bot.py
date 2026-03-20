from __future__ import annotations

import logging
import threading
import time
from typing import Any

import requests
import socketio

from config import (
    CHAT_CLOSED_TEXT,
    CHAT_REOPENED_TEXT,
    CHAT_TAKEN_TEXT,
    CHAT_TRANSFERRED_TEXT,
    RESOLVED_TEXT,
    STATE_FILE,
    SUPPORT_API_BASE_URL,
    SUPPORT_BOT_API_KEY,
    TELEGRAM_POLL_TIMEOUT,
    TELEGRAM_RETRY_DELAY,
    WAITING_TEXT,
    WAITING_CUSTOMER_TEXT,
    WELCOME_TEXT,
    ZIP_SOCKET_PATH,
    ZIP_SOCKET_RECONNECT_ATTEMPTS,
    ZIP_SOCKET_URL,
    ZIP_SOCKET_VERIFY_SSL,
)
from state import StateStore
from telegram_api import TelegramAPIError, get_updates, kb_main, send_message
from zip_client import ZipAPIError, ZipSupportClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("zip-support-bot")


class SupportBot:
    def __init__(self):
        self.state = StateStore(STATE_FILE)
        self.zip = ZipSupportClient(SUPPORT_API_BASE_URL, SUPPORT_BOT_API_KEY)
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=ZIP_SOCKET_RECONNECT_ATTEMPTS or 0,
            logger=False,
            engineio_logger=False,
            ssl_verify=ZIP_SOCKET_VERIFY_SSL,
        )
        self.offset: int | None = None
        self._register_socket_handlers()

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
                self.handle_operator_message_event(data)
            except Exception:
                logger.exception("Failed to process support:new-message")

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
        logger.info("Support bot started")
        while True:
            try:
                updates = get_updates(self.offset, timeout_s=TELEGRAM_POLL_TIMEOUT)
                for upd in updates.get("result", []):
                    self.offset = upd["update_id"] + 1
                    if "message" in upd:
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
        text = (message.get("text") or "").strip()
        if not text:
            send_message(tg_user_id, "Поки що підтримуються лише текстові повідомлення.", kb_main())
            return

        if text == "/start":
            send_message(tg_user_id, WELCOME_TEXT, kb_main())
            return

        if text in {"🛠 Звернутись у тех. підтримку", "📡 Заявка на підключення"}:
            send_message(tg_user_id, "Напишіть деталі одним повідомленням — я передам їх оператору.", kb_main())
            return

        try:
            subscriber_name = self._build_subscriber_name(message)
            support_chat_id = self._forward_subscriber_message(tg_user_id, subscriber_name, text)
            if not self.state.is_welcomed(support_chat_id):
                send_message(tg_user_id, WAITING_TEXT, kb_main())
                self.state.mark_welcomed(support_chat_id)
        except ZipAPIError:
            logger.exception("Failed to send message to ZIP")
            send_message(tg_user_id, "⚠️ Не вдалося передати повідомлення в систему підтримки. Спробуйте ще раз трохи пізніше.", kb_main())

    def handle_operator_message_event(self, data: dict[str, Any]):
        message = data.get("message") or {}
        if message.get("senderType") != "operator":
            return

        chat = self._extract_chat_payload(data)
        if chat is not None:
            self._sync_chat_snapshot(chat, source="support:new-message")

        message_id = message.get("id")
        if isinstance(message_id, int) and not self.state.mark_seen_operator_message(message_id):
            return

        chat_id = self._extract_chat_id(data, chat=chat)
        if chat_id is None:
            logger.warning("No chat id in support:new-message payload: %s", data)
            return

        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        if tg_user_id is None:
            logger.warning("No Telegram user found for support chat %s", chat_id)
            return

        text = (message.get("text") or "").strip()
        if not text:
            return

        send_message(tg_user_id, f"👨‍💻 Оператор: {text}", kb_main())

    def handle_chat_closed_event(self, data: dict[str, Any]):
        chat = self._extract_chat_payload(data) or {}
        previous, current = self._sync_chat_snapshot(chat, source="support:chat-closed")
        previous_status = (previous or {}).get("status")

        chat_id = self._extract_chat_id(data, chat=current or chat)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id, allow_remote_lookup=False)

        if tg_user_id is not None and previous_status != "closed":
            send_message(tg_user_id, CHAT_CLOSED_TEXT, kb_main())
            self.state.clear_chat(tg_user_id=tg_user_id)
        if chat_id is not None:
            self.state.clear_chat(chat_id=chat_id)
            self.state.clear_welcomed(chat_id)

    def handle_chat_status_event(self, data: dict[str, Any], text: str):
        chat = self._extract_chat_payload(data) or {}
        _, current = self._sync_chat_snapshot(chat, source="support:chat-status")
        chat_id = self._extract_chat_id(data, chat=current or chat)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        if tg_user_id is not None:
            send_message(tg_user_id, text, kb_main())

    def handle_chat_reopened_event(self, data: dict[str, Any]):
        chat = self._extract_chat_payload(data) or {}
        previous, current = self._sync_chat_snapshot(chat, source="support:chat-reopened")
        chat_id = self._extract_chat_id(data, chat=current or chat)
        tg_user_id = self._resolve_event_tg_user_id(chat, chat_id)
        previous_status = (previous or {}).get("status")
        current_status = (current or {}).get("status")

        if chat_id is not None:
            self.state.clear_welcomed(chat_id)
        if tg_user_id is not None and previous_status != current_status:
            send_message(tg_user_id, CHAT_REOPENED_TEXT, kb_main())

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
        send_message(tg_user_id, workflow_text, kb_main())

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

    def _forward_subscriber_message(self, tg_user_id: int, subscriber_name: str, text: str) -> int:
        chat_data = self.zip.ensure_chat(tg_user_id, subscriber_name)
        _, current = self._sync_chat_snapshot(chat_data, source="ensure-chat")
        support_chat_id = self._extract_chat_id(chat_data, chat=current or chat_data)
        if support_chat_id is None:
            raise ZipAPIError("ZIP API returned chat payload without id")
        self.state.set_chat(tg_user_id, support_chat_id)

        try:
            self.zip.send_subscriber_message(support_chat_id, text)
            return support_chat_id
        except ZipAPIError as exc:
            if not self._is_stale_chat_error(exc):
                raise

            logger.warning(
                "Support chat %s became stale while sending message, re-opening flow once",
                support_chat_id,
            )
            refreshed_chat = self.zip.ensure_chat(tg_user_id, subscriber_name)
            _, refreshed_current = self._sync_chat_snapshot(refreshed_chat, source="ensure-chat-retry")
            refreshed_chat_id = self._extract_chat_id(refreshed_chat, chat=refreshed_current or refreshed_chat)
            if refreshed_chat_id is None:
                raise ZipAPIError("ZIP API returned retry chat payload without id") from exc
            self.state.set_chat(tg_user_id, refreshed_chat_id)

            self.zip.send_subscriber_message(refreshed_chat_id, text)
            return refreshed_chat_id

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


if __name__ == "__main__":
    SupportBot().run()
