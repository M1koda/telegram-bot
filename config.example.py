"""Server-local configuration template.

Copy this file to `config.py` on the target machine and fill in real values.
Do not commit the resulting `config.py`.
"""

# Telegram
TOKEN = "PUT_TELEGRAM_BOT_TOKEN_HERE"
TELEGRAM_POLL_TIMEOUT = 30
TELEGRAM_RETRY_DELAY = 2.0

# ZIP Support API
SUPPORT_API_BASE_URL = "https://your-host.example/api/bot/support"
SUPPORT_BOT_API_KEY = "PUT_SUPPORT_BOT_API_KEY_HERE"

# ZIP Socket.IO
ZIP_SOCKET_URL = "https://your-host.example"
ZIP_SOCKET_PATH = "/ws"
ZIP_SOCKET_VERIFY_SSL = True
ZIP_SOCKET_RECONNECT_ATTEMPTS = 0

# Local state
STATE_FILE = "bot_state.json"

# UX texts
WELCOME_TEXT = "Support bot welcome message."
WAITING_TEXT = "Your message has been delivered to support."
WAITING_CUSTOMER_TEXT = "We are waiting for your reply."
RESOLVED_TEXT = "The issue is marked as preliminarily resolved."
CHAT_CLOSED_TEXT = "Your support chat has been closed."
CHAT_TAKEN_TEXT = "An operator joined your request."
CHAT_TRANSFERRED_TEXT = "Your chat was transferred to another operator."
CHAT_REOPENED_TEXT = "Your chat was reopened."
