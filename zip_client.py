from __future__ import annotations

import json
from typing import Any
import requests


class ZipAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        payload: Any | None = None,
        response_text: str | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload
        self.response_text = response_text

    def details_text(self) -> str:
        parts = [str(self)]
        if self.payload is not None:
            try:
                parts.append(json.dumps(self.payload, ensure_ascii=False))
            except TypeError:
                parts.append(str(self.payload))
        elif self.response_text:
            parts.append(self.response_text)
        return " ".join(part for part in parts if part)


class ZipSupportClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        try:
            resp = self.session.request(method, self._url(path), timeout=30, **kwargs)
        except requests.RequestException as exc:
            raise ZipAPIError(
                "ZIP API request failed",
                response_text=str(exc),
            ) from exc
        try:
            payload = resp.json()
        except Exception as exc:
            raise ZipAPIError(
                f"ZIP API non-JSON response ({resp.status_code})",
                status_code=resp.status_code,
                response_text=resp.text[:500],
            ) from exc
        if not resp.ok or not payload.get("success", False):
            raise ZipAPIError(
                f"ZIP API error {resp.status_code}",
                status_code=resp.status_code,
                payload=payload,
                response_text=resp.text[:500],
            )
        return payload

    @staticmethod
    def _extract_data(payload: dict[str, Any]) -> Any:
        return payload.get("data", payload)

    def ensure_chat(
        self,
        subscriber_telegram_id: int,
        subscriber_name: str | None = None,
        subscriber_phone: str | None = None,
    ) -> dict[str, Any]:
        payload = {"subscriberTelegramId": subscriber_telegram_id}
        if subscriber_name:
            payload["subscriberName"] = subscriber_name[:255]
        if subscriber_phone:
            payload["subscriberPhone"] = subscriber_phone
        return self._extract_data(self._request("POST", "/chats", json=payload))

    def send_subscriber_message(self, chat_id: int, text: str) -> dict[str, Any]:
        return self._extract_data(self._request("POST", f"/chats/{chat_id}/messages", json={"text": text}))

    def close_chat(self, chat_id: int) -> dict[str, Any]:
        return self._extract_data(self._request("POST", f"/chats/{chat_id}/close"))

    def get_chat(self, chat_id: int) -> dict[str, Any]:
        return self._extract_data(self._request("GET", f"/chats/{chat_id}"))

    def get_messages(self, chat_id: int, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        return self._request("GET", f"/chats/{chat_id}/messages", params={"page": page, "pageSize": page_size})

    def get_pending_ratings(self, page: int = 1, page_size: int = 50) -> list[dict[str, Any]]:
        payload = self._request("GET", "/ratings/pending", params={"page": page, "pageSize": page_size})
        data = self._extract_data(payload)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("items"), list):
                return data["items"]
            if isinstance(data.get("results"), list):
                return data["results"]
        if isinstance(payload.get("items"), list):
            return payload["items"]
        if isinstance(payload.get("results"), list):
            return payload["results"]
        return []

    def submit_rating(
        self,
        chat_id: int,
        request_token: str,
        score: int,
        *,
        comment: str | None = None,
    ) -> dict[str, Any]:
        payload = {"requestToken": request_token, "score": int(score)}
        if comment:
            payload["comment"] = comment
        return self._extract_data(self._request("POST", f"/chats/{chat_id}/rating", json=payload))

    def patch_rating_comment(self, chat_id: int, request_token: str, comment: str) -> dict[str, Any]:
        payload = {"requestToken": request_token, "comment": comment}
        return self._extract_data(self._request("PATCH", f"/chats/{chat_id}/rating", json=payload))

    def skip_rating(self, chat_id: int, request_token: str) -> dict[str, Any]:
        payload = {"requestToken": request_token}
        return self._extract_data(self._request("POST", f"/chats/{chat_id}/rating/skip", json=payload))
