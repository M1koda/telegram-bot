from datetime import datetime
from storage import USERS, CHATS, NEXT_CHAT_ID


def is_registered(user_id):
    return user_id in USERS


def register_user(user_id, phone, username, first_name):
    USERS[user_id] = {
        "phone": phone,
        "username": f"@{username}" if username else "—",
        "first_name": first_name or "—"
    }


def create_chat(client_id):
    global NEXT_CHAT_ID
    chat_id = NEXT_CHAT_ID
    NEXT_CHAT_ID += 1

    CHATS[chat_id] = {
        "client_id": client_id,
        "operator_id": None,
        "status": "waiting",
        "history": []
    }
    return chat_id


def add_history(chat_id, line):
    CHATS[chat_id]["history"].append(line)


def close_chat(chat_id, by_role):
    CHATS[chat_id]["status"] = "closed"
    add_history(chat_id, f"СИСТЕМА: Чат завершено ({by_role})")


def active_chat_for_operator(operator_id):
    for cid, c in CHATS.items():
        if c["operator_id"] == operator_id and c["status"] == "active":
            return cid
    return None
