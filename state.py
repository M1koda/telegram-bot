from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.data = {
            "tg_to_chat": {},
            "chat_to_tg": {},
            "subscriber_phones": {},
            "subscriber_avatars": {},
            "pending_phone_gate_users": [],
            "seen_operator_message_ids": [],
            "welcomed_chat_ids": [],
            "chat_meta": {},
            "pending_ratings": {},
            "draft_request_users": [],
            "connection_requests": {},
        }
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                self.data.update(json.loads(self.path.read_text(encoding="utf-8")))
            except Exception:
                pass
        self.data.setdefault("tg_to_chat", {})
        self.data.setdefault("chat_to_tg", {})
        self.data.setdefault("subscriber_phones", {})
        self.data.setdefault("subscriber_avatars", {})
        self.data.setdefault("pending_phone_gate_users", [])
        self.data.setdefault("seen_operator_message_ids", [])
        self.data.setdefault("welcomed_chat_ids", [])
        self.data.setdefault("chat_meta", {})
        self.data.setdefault("pending_ratings", {})
        self.data.setdefault("draft_request_users", [])
        self.data.setdefault("connection_requests", {})

    def _save(self):
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def set_chat(self, tg_user_id: int, chat_id: int):
        with self.lock:
            self._set_chat_locked(tg_user_id, chat_id)
            self._save()

    def sync_chat(self, chat_data: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        chat_id = chat_data.get("id")
        if chat_id is None:
            return None, None

        normalized_chat_id = int(chat_id)
        subscriber_telegram_id = chat_data.get("subscriberTelegramId")
        subscriber_phone = self._extract_subscriber_phone(chat_data)

        with self.lock:
            if subscriber_telegram_id is not None:
                self._set_chat_locked(int(subscriber_telegram_id), normalized_chat_id)
                if subscriber_phone:
                    self.data["subscriber_phones"][str(int(subscriber_telegram_id))] = subscriber_phone
                    pending_phone_gate = self.data["pending_phone_gate_users"]
                    if int(subscriber_telegram_id) in pending_phone_gate:
                        pending_phone_gate.remove(int(subscriber_telegram_id))

            chat_key = str(normalized_chat_id)
            previous = self.data["chat_meta"].get(chat_key)
            previous_copy = dict(previous) if isinstance(previous, dict) else None

            current = previous_copy or {}
            current.update(self._normalize_chat_meta(chat_data))
            self.data["chat_meta"][chat_key] = current
            self._save()

            return previous_copy, dict(current)

    def get_chat_by_tg(self, tg_user_id: int) -> int | None:
        with self.lock:
            value = self.data["tg_to_chat"].get(str(tg_user_id))
            return int(value) if value is not None else None

    def get_tg_by_chat(self, chat_id: int) -> int | None:
        with self.lock:
            value = self.data["chat_to_tg"].get(str(chat_id))
            return int(value) if value is not None else None

    def get_chat_meta(self, chat_id: int) -> dict[str, Any] | None:
        with self.lock:
            value = self.data["chat_meta"].get(str(chat_id))
            return dict(value) if isinstance(value, dict) else None

    def get_subscriber_phone(self, tg_user_id: int) -> str | None:
        with self.lock:
            value = self.data["subscriber_phones"].get(str(tg_user_id))
            return str(value) if value else None

    def has_subscriber_phone(self, tg_user_id: int) -> bool:
        return bool(self.get_subscriber_phone(tg_user_id))

    def get_subscriber_avatar(self, tg_user_id: int) -> dict[str, Any] | None:
        with self.lock:
            value = self.data["subscriber_avatars"].get(str(tg_user_id))
            return dict(value) if isinstance(value, dict) else None

    def set_subscriber_avatar(
        self,
        tg_user_id: int,
        *,
        url: str | None = None,
        file_name: str | None = None,
        checked_at: int | None = None,
        has_avatar: bool | None = None,
    ) -> dict[str, Any]:
        normalized_checked_at = int(checked_at or time.time())
        normalized_has_avatar = bool(has_avatar if has_avatar is not None else (url and file_name))

        with self.lock:
            current = self.data["subscriber_avatars"].get(str(tg_user_id))
            if not isinstance(current, dict):
                current = {}

            current["checkedAt"] = normalized_checked_at
            current["hasAvatar"] = normalized_has_avatar

            if normalized_has_avatar and url and file_name:
                current["url"] = str(url).strip()
                current["fileName"] = str(file_name).strip()
            else:
                current.pop("url", None)
                current.pop("fileName", None)

            self.data["subscriber_avatars"][str(tg_user_id)] = current
            self._save()
            return dict(current)

    def set_subscriber_phone(self, tg_user_id: int, phone: str):
        normalized_phone = str(phone).strip()
        if not normalized_phone:
            return
        with self.lock:
            self.data["subscriber_phones"][str(tg_user_id)] = normalized_phone
            pending = self.data["pending_phone_gate_users"]
            if tg_user_id in pending:
                pending.remove(tg_user_id)
            self._save()

    def mark_pending_phone_gate(self, tg_user_id: int):
        with self.lock:
            pending = self.data["pending_phone_gate_users"]
            if tg_user_id not in pending:
                pending.append(tg_user_id)
                self._save()

    def clear_pending_phone_gate(self, tg_user_id: int):
        with self.lock:
            pending = self.data["pending_phone_gate_users"]
            if tg_user_id in pending:
                pending.remove(tg_user_id)
                self._save()

    def is_waiting_phone_gate(self, tg_user_id: int) -> bool:
        with self.lock:
            return tg_user_id in self.data["pending_phone_gate_users"]

    def clear_chat(self, *, tg_user_id: int | None = None, chat_id: int | None = None):
        with self.lock:
            if tg_user_id is not None:
                mapped_chat = self.data["tg_to_chat"].pop(str(tg_user_id), None)
                if mapped_chat is not None:
                    self.data["chat_to_tg"].pop(str(mapped_chat), None)
            if chat_id is not None:
                mapped_tg = self.data["chat_to_tg"].pop(str(chat_id), None)
                if mapped_tg is not None:
                    self.data["tg_to_chat"].pop(str(mapped_tg), None)
            self._save()

    def mark_seen_operator_message(self, message_id: int) -> bool:
        with self.lock:
            seen = self.data["seen_operator_message_ids"]
            if message_id in seen:
                return False
            seen.append(message_id)
            if len(seen) > 2000:
                del seen[:-1000]
            self._save()
            return True

    def mark_welcomed(self, chat_id: int):
        with self.lock:
            welcomed = self.data["welcomed_chat_ids"]
            if chat_id not in welcomed:
                welcomed.append(chat_id)
                if len(welcomed) > 500:
                    del welcomed[:-250]
                self._save()

    def is_welcomed(self, chat_id: int) -> bool:
        with self.lock:
            return chat_id in self.data["welcomed_chat_ids"]

    def clear_welcomed(self, chat_id: int):
        with self.lock:
            welcomed = self.data["welcomed_chat_ids"]
            if chat_id in welcomed:
                welcomed.remove(chat_id)
                self._save()

    def mark_request_draft(self, tg_user_id: int):
        with self.lock:
            drafts = self.data["draft_request_users"]
            if tg_user_id not in drafts:
                drafts.append(tg_user_id)
                self._save()

    def clear_request_draft(self, tg_user_id: int):
        with self.lock:
            drafts = self.data["draft_request_users"]
            if tg_user_id in drafts:
                drafts.remove(tg_user_id)
                self._save()

    def is_waiting_request_input(self, tg_user_id: int) -> bool:
        with self.lock:
            return tg_user_id in self.data["draft_request_users"]

    def start_connection_request(self, tg_user_id: int):
        with self.lock:
            self.data["connection_requests"][str(tg_user_id)] = {"step": "room_type"}
            self._save()

    def get_connection_request(self, tg_user_id: int) -> dict[str, Any] | None:
        with self.lock:
            value = self.data["connection_requests"].get(str(tg_user_id))
            return dict(value) if isinstance(value, dict) else None

    def update_connection_request(self, tg_user_id: int, **fields) -> dict[str, Any]:
        with self.lock:
            tg_key = str(tg_user_id)
            current = self.data["connection_requests"].get(tg_key)
            if not isinstance(current, dict):
                current = {"step": "room_type"}
            current.update(fields)
            self.data["connection_requests"][tg_key] = current
            self._save()
            return dict(current)

    def clear_connection_request(self, tg_user_id: int):
        with self.lock:
            if self.data["connection_requests"].pop(str(tg_user_id), None) is not None:
                self._save()

    def sync_pending_rating(
        self,
        request_data: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        chat_id = request_data.get("chatId") or request_data.get("chat_id")
        subscriber_tg_id = request_data.get("subscriberTelegramId")
        request_token = request_data.get("requestToken")
        if chat_id is None or subscriber_tg_id is None or not request_token:
            return None, None

        normalized = self._normalize_pending_rating(request_data)
        chat_key = str(int(chat_id))

        with self.lock:
            previous = self.data["pending_ratings"].get(chat_key)
            previous_copy = dict(previous) if isinstance(previous, dict) else None

            current = previous_copy or {}
            if current.get("requestToken") != request_token:
                current = {}
            current.update(normalized)
            current.setdefault("telegramPromptMessageId", None)
            current.setdefault("score", None)
            current.setdefault("commentRequested", False)
            current.setdefault("commentSubmitted", False)

            self.data["pending_ratings"][chat_key] = current
            self._save()

            return previous_copy, dict(current)

    def get_pending_rating(self, chat_id: int) -> dict[str, Any] | None:
        with self.lock:
            value = self.data["pending_ratings"].get(str(chat_id))
            return dict(value) if isinstance(value, dict) else None

    def list_pending_ratings(self) -> list[dict[str, Any]]:
        with self.lock:
            items = []
            for value in self.data["pending_ratings"].values():
                if isinstance(value, dict):
                    items.append(dict(value))
            return items

    def set_pending_rating_prompt_message(
        self,
        chat_id: int,
        message_id: int,
        *,
        request_token: str | None = None,
    ) -> dict[str, Any] | None:
        with self.lock:
            current = self.data["pending_ratings"].get(str(chat_id))
            if not isinstance(current, dict):
                return None
            if request_token and current.get("requestToken") != request_token:
                return None
            current["telegramPromptMessageId"] = int(message_id)
            self._save()
            return dict(current)

    def mark_rating_score(
        self,
        chat_id: int,
        score: int,
        *,
        request_token: str | None = None,
        comment_requested: bool = True,
    ) -> dict[str, Any] | None:
        with self.lock:
            current = self.data["pending_ratings"].get(str(chat_id))
            if not isinstance(current, dict):
                return None
            if request_token and current.get("requestToken") != request_token:
                return None
            current["score"] = int(score)
            current["commentRequested"] = bool(comment_requested)
            current["commentSubmitted"] = False
            self._save()
            return dict(current)

    def mark_rating_comment_submitted(
        self,
        chat_id: int,
        *,
        request_token: str | None = None,
        comment: str | None = None,
    ) -> dict[str, Any] | None:
        with self.lock:
            current = self.data["pending_ratings"].get(str(chat_id))
            if not isinstance(current, dict):
                return None
            if request_token and current.get("requestToken") != request_token:
                return None
            current["commentRequested"] = False
            current["commentSubmitted"] = True
            if comment is not None:
                current["comment"] = comment
            self._save()
            return dict(current)

    def clear_pending_rating(self, chat_id: int, *, request_token: str | None = None):
        with self.lock:
            chat_key = str(chat_id)
            current = self.data["pending_ratings"].get(chat_key)
            if not isinstance(current, dict):
                return
            if request_token and current.get("requestToken") != request_token:
                return
            self.data["pending_ratings"].pop(chat_key, None)
            self._save()

    def get_pending_comment_request_for_tg(self, tg_user_id: int) -> dict[str, Any] | None:
        with self.lock:
            candidates = []
            for value in self.data["pending_ratings"].values():
                if not isinstance(value, dict):
                    continue
                if int(value.get("subscriberTelegramId", 0) or 0) != tg_user_id:
                    continue
                if not value.get("commentRequested") or value.get("commentSubmitted"):
                    continue
                candidates.append(dict(value))

            if not candidates:
                return None

            candidates.sort(
                key=lambda item: (
                    str(item.get("requestedAt") or ""),
                    int(item.get("chatId") or 0),
                ),
                reverse=True,
            )
            return candidates[0]

    def _set_chat_locked(self, tg_user_id: int, chat_id: int):
        tg_key = str(tg_user_id)
        chat_key = str(chat_id)

        previous_chat = self.data["tg_to_chat"].get(tg_key)
        if previous_chat is not None and str(previous_chat) != chat_key:
            self.data["chat_to_tg"].pop(str(previous_chat), None)

        previous_tg = self.data["chat_to_tg"].get(chat_key)
        if previous_tg is not None and str(previous_tg) != tg_key:
            self.data["tg_to_chat"].pop(str(previous_tg), None)

        self.data["tg_to_chat"][tg_key] = chat_id
        self.data["chat_to_tg"][chat_key] = tg_user_id

    @staticmethod
    def _normalize_chat_meta(chat_data: dict[str, Any]) -> dict[str, Any]:
        fields = (
            "id",
            "subscriberTelegramId",
            "subscriberName",
            "subscriberPhone",
            "subscriberAvatarUrl",
            "phone",
            "contractNumber",
            "contractNotCreated",
            "operatorId",
            "operatorUsername",
            "status",
            "priority",
            "topic",
            "closeReason",
            "createdAt",
            "updatedAt",
            "closedAt",
            "resolvedAt",
            "assignedAt",
            "firstResponseAt",
            "lastSubscriberMessageAt",
            "lastOperatorMessageAt",
            "lastMessage",
            "unreadCount",
        )
        return {field: chat_data[field] for field in fields if field in chat_data}

    @staticmethod
    def _extract_subscriber_phone(chat_data: dict[str, Any]) -> str | None:
        for field in ("subscriberPhone", "phone"):
            value = chat_data.get(field)
            if value:
                return str(value).strip()
        return None

    @staticmethod
    def _normalize_pending_rating(request_data: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "chatId": int(request_data["chatId"]),
            "subscriberTelegramId": int(request_data["subscriberTelegramId"]),
            "requestToken": str(request_data["requestToken"]),
        }
        optional_fields = (
            "subscriberName",
            "requestedAt",
            "expiresAt",
            "telegramPromptMessageId",
            "score",
            "comment",
            "commentRequested",
            "commentSubmitted",
            "status",
        )
        for field in optional_fields:
            if field in request_data:
                normalized[field] = request_data[field]
        if "telegramPromptMessageId" not in normalized:
            normalized["telegramPromptMessageId"] = None
        normalized["score"] = int(normalized["score"]) if normalized.get("score") is not None else None
        normalized["commentRequested"] = bool(normalized.get("commentRequested", False))
        normalized["commentSubmitted"] = bool(normalized.get("commentSubmitted", False))
        return normalized
