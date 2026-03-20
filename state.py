from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.data = {
            "tg_to_chat": {},
            "chat_to_tg": {},
            "seen_operator_message_ids": [],
            "welcomed_chat_ids": [],
            "chat_meta": {},
        }
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                self.data.update(json.loads(self.path.read_text(encoding="utf-8")))
            except Exception:
                pass

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

        with self.lock:
            if subscriber_telegram_id is not None:
                self._set_chat_locked(int(subscriber_telegram_id), normalized_chat_id)

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
