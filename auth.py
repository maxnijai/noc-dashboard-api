"""
auth.py
-------
NOR2026 / BBTEC NOC — site-wide login.

Users are stored in a "Users" tab in the same spreadsheet as TicketWorkLog
(REALTIME_SHEET_ID) - columns: phone, name, company, department,
password_hash, must_change_password, created_at.

Login identifier is PHONE NUMBER (normalized to digits only, so "086-398-2366"
and "0863982366" match the same account). Passwords are hashed with
werkzeug.security (bundled with Flask - no extra dependency). Default
password for a freshly-seeded account is the last 4 digits of their phone
number, and must_change_password is set True so the app forces them to a
change-password screen before they can do anything else.
"""

import logging
import re
from datetime import datetime

from werkzeug.security import generate_password_hash, check_password_hash

from pending_trend import get_drive_and_sheets_clients, bangkok_now
from realtime_monitor import REALTIME_SHEET_ID

log = logging.getLogger(__name__)

USERS_SHEET = "Users"
USERS_HEADER = ["phone", "name", "company", "department", "password_hash", "must_change_password", "created_at"]


def normalize_phone(raw):
    """Strip everything but digits, so '086-398-2366' and '0863982366' are the same account."""
    return re.sub(r"\D", "", raw or "")


def _ensure_users_tab(spreadsheet):
    try:
        return spreadsheet.worksheet(USERS_SHEET)
    except Exception:
        ws = spreadsheet.add_worksheet(title=USERS_SHEET, rows=200, cols=len(USERS_HEADER))
        ws.append_row(USERS_HEADER)
        return ws


def _get_users_ws(gs_client):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    return _ensure_users_tab(sh)


def _row_to_user(row):
    padded = row + [""] * (len(USERS_HEADER) - len(row))
    return {
        "phone": padded[0], "name": padded[1], "company": padded[2], "department": padded[3],
        "password_hash": padded[4], "must_change_password": padded[5].strip().upper() == "TRUE",
        "created_at": padded[6],
    }


def get_all_users(gs_client=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()
    ws = _get_users_ws(gs_client)
    records = ws.get_all_values()[1:]
    return [_row_to_user(r) for r in records if r and r[0]]


def find_user_by_phone(gs_client, phone):
    phone_norm = normalize_phone(phone)
    for u in get_all_users(gs_client):
        if normalize_phone(u["phone"]) == phone_norm:
            return u
    return None


def verify_login(gs_client, phone, password):
    """Returns the user dict on success, None on failure."""
    user = find_user_by_phone(gs_client, phone)
    if not user or not user["password_hash"]:
        return None
    if check_password_hash(user["password_hash"], password):
        return user
    return None


def set_password(gs_client, phone, new_password, clear_must_change=True):
    ws = _get_users_ws(gs_client)
    phone_norm = normalize_phone(phone)
    values = ws.get_all_values()
    for idx, row in enumerate(values[1:], start=2):  # data starts row 2
        if row and normalize_phone(row[0]) == phone_norm:
            new_hash = generate_password_hash(new_password)
            ws.update(f"E{idx}:F{idx}", [[new_hash, "" if clear_must_change else "TRUE"]])
            return True
    return False


def seed_user(gs_client, phone, name, company, department, default_password=None):
    """Adds a user row if one doesn't already exist for this phone. Default
    password is the last 4 digits of the phone unless overridden; the account
    is flagged must_change_password so they're forced to set their own on
    first login. Safe to call repeatedly (skips existing phones)."""
    if find_user_by_phone(gs_client, phone):
        return False
    ws = _get_users_ws(gs_client)
    phone_norm = normalize_phone(phone)
    pw = default_password or phone_norm[-4:]
    ws.append_row([
        phone, name, company, department,
        generate_password_hash(pw), "TRUE",
        bangkok_now().strftime("%Y-%m-%d %H:%M:%S"),
    ])
    return True
