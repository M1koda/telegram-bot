# Copilot / AI Agent Instructions

Short, actionable notes to make an AI coding agent productive in this repo.

1. Big picture
- Single-process Telegram support bot implemented in `bot.py` (entrypoint: `main()`).
- Telegram HTTP wrapper and keyboard helpers live in `telegram_api.py` (`send_message`, `get_updates`, `kb_*`, `ibtn`).
- Persistent storage: SQLite via `db.py` (call `init_db()` on startup creates `support.db`).
- There is an older/in-memory helper set in `storage.py` / `logic.py` used for simple dev mocks — production code imports `db.py`.

2. How the runtime flows
- `bot.py` calls `init_db()` then long-polls Telegram via `telegram_api.get_updates`.
- Update handling is centralized: `handle_callback` / `handle_callback_extra` handle callback_query; `handle_message` handles text/messages.
- State machines use global in-memory dicts in `bot.py` (e.g. `REG_STATE`, `CONNECT`, `SUPPORT_DRAFT`, `ADMIN_INPUT`) — modify carefully and keep single-thread assumptions in mind.

3. Key patterns & conventions
- UI: reply keyboards vs inline keyboards: use `kb_reply(...)` for client/operator menus and `kb_inline(...)` + `ibtn(...)` for inline callbacks.
- Callback data: colon-separated tokens (example: `wait_take:123`, `conn:tariff:unlim299`). Parse with `str.split(":")`.
- DB helpers are thin wrappers: use `create_chat(client_id, first_text)`, `add_message(chat_id, role, id, text)`, `get_chat_history(chat_id)` from `db.py`.
- Role checks: `is_operator(uid)`, `is_admin(uid)` and `SUPERADMIN_ID` in `config.py` (superadmin also receives admin/operator flags via `ensure_superadmin`).

4. Integration & infra
- Telegram: uses `requests` to call `https://api.telegram.org/bot{TOKEN}`. Token is in `config.py`.
- SQLite: file `support.db` created next to code. `db.py` sets WAL and a threading lock `_db_lock` — keep DB access inside provided helpers.
- Channel notifications: `CALL_REQUESTS_CHANNEL_ID` in `config.py` is used for phone-request submissions.

5. Developer workflows (how to run / debug)
- Install deps: `pip install -r requirements.txt` (contains `requests`, `pytz`).
- Run locally: `python bot.py`. Bot uses long-polling; ensure `config.TOKEN` is valid and bot is added to any channels referenced (for `CALL_REQUESTS_CHANNEL_ID`).
- DB reset: delete `support.db` to recreate schema on next run (or call `init_db()` in REPL).

6. When changing code — concise rules for AI edits
- Add new UI buttons: add keyboard helper in `telegram_api.py` and handle corresponding callback/text in `bot.py`.
- Add DB columns/tables: update `init_db()` in `db.py` and provide migration logic (simple projects recreate DB — prefer additive migrations).
- Prefer using `db.py` helpers over touching `_conn` directly. Tests / dev mocks can use `storage.py` but production code uses `db.py`.
- Keep strings/messages in-place (Ukrainian). Follow existing phrasing and emoji usage for UX consistency.

7. Useful examples (copy-paste)
- Create a chat and add first message:
  - `cid = create_chat(uid, first_text="Заявка на підключення")`
  - `add_message(cid, "client", uid, text)`
- Send a keyboarded message:
  - `send_message(uid, "Оберіть режим:", kb_admin_start())`
- Inline button data parsing:
  - `if data.startswith("wait_take:"):
       cid = int(data.split(":")[-1])`

8. Notes / gotchas discovered
- Two storage models coexist: `db.py` (SQLite, used by `bot.py`) and `storage.py`/`logic.py` (in-memory). Prefer `db.py` for any persistent change.
- Global FSM dicts are not persisted — restarting the bot loses in-progress flows.
- Bot is single-process long-polling; avoid adding blocking operations to the main loop.

If anything is unclear or you want me to include more examples or coding guardrails (migrations, tests, or a small local dev harness), tell me what to add. I'll iterate.
