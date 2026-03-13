# ===== РОЛІ (Telegram user_id) =====
OPERATORS = {
    508492199,
}

ADMINS = {
    508492199,
}

# ===== КЛІЄНТИ =====
# user_id -> {phone, username, first_name}
USERS = {}

# ===== ЧАТИ =====
# chat_id -> dict
CHATS = {}
# {
#   "client_id": int,
#   "operator_id": int | None,
#   "status": "waiting" | "active" | "closed",
#   "history": list[str]
# }

NEXT_CHAT_ID = 1