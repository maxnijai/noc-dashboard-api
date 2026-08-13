"""
auth.py
-------
NOR2026 / BBTEC NOC — site-wide login.

Users are stored in a "Users" tab in the same spreadsheet as TicketWorkLog
(REALTIME_SHEET_ID) - columns: email, phone, name, company, department,
password_hash, must_change_password, created_at.

Login identifier is EMAIL (matched case-insensitively, trimmed). Passwords
are hashed with werkzeug.security (bundled with Flask - no extra dependency).
Default password for a freshly-seeded account is the last 4 digits of their
phone number, and must_change_password is set True so the app forces them to
a change-password screen before they can do anything else.
"""

import logging
import re
from datetime import datetime

from werkzeug.security import generate_password_hash, check_password_hash

from pending_trend import get_drive_and_sheets_clients, bangkok_now
from realtime_monitor import REALTIME_SHEET_ID

log = logging.getLogger(__name__)

USERS_SHEET = "Users"
USERS_HEADER = ["email", "phone", "name", "company", "department", "password_hash", "must_change_password", "created_at"]


def normalize_email(raw):
    return (raw or "").strip().lower()


def normalize_phone(raw):
    """Strip everything but digits, so '086-398-2366' and '0863982366' are the same number."""
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
        "email": padded[0], "phone": padded[1], "name": padded[2], "company": padded[3],
        "department": padded[4], "password_hash": padded[5],
        "must_change_password": padded[6].strip().upper() == "TRUE",
        "created_at": padded[7],
    }


def get_all_users(gs_client=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()
    ws = _get_users_ws(gs_client)
    records = ws.get_all_values()[1:]
    return [_row_to_user(r) for r in records if r and r[0]]


def find_user_by_email(gs_client, email):
    email_norm = normalize_email(email)
    for u in get_all_users(gs_client):
        if normalize_email(u["email"]) == email_norm:
            return u
    return None


def verify_login(gs_client, email, password):
    """Returns the user dict on success, None on failure."""
    user = find_user_by_email(gs_client, email)
    if not user or not user["password_hash"]:
        return None
    if check_password_hash(user["password_hash"], password):
        return user
    return None


def set_password(gs_client, email, new_password, clear_must_change=True):
    ws = _get_users_ws(gs_client)
    email_norm = normalize_email(email)
    values = ws.get_all_values()
    for idx, row in enumerate(values[1:], start=2):  # data starts row 2
        if row and normalize_email(row[0]) == email_norm:
            new_hash = generate_password_hash(new_password)
            ws.update(f"F{idx}:G{idx}", [[new_hash, "" if clear_must_change else "TRUE"]])
            return True
    return False


def seed_user(gs_client, email, phone, name="", company="", department="", default_password=None):
    """Adds a user row if one doesn't already exist for this email. Default
    password is the last 4 digits of the phone unless overridden; the account
    is flagged must_change_password so they're forced to set their own on
    first login. Safe to call repeatedly (skips existing emails).

    NOTE: for seeding more than a handful of users, prefer batch_seed_users
    below - this one does a fresh read+write round trip per call, which
    burns through the Sheets API per-minute read quota fast."""
    if not email:
        return False
    if find_user_by_email(gs_client, email):
        return False
    ws = _get_users_ws(gs_client)
    phone_norm = normalize_phone(phone)
    pw = default_password or phone_norm[-4:] or "0000"
    ws.append_row([
        normalize_email(email), phone, name, company, department,
        generate_password_hash(pw), "TRUE",
        bangkok_now().strftime("%Y-%m-%d %H:%M:%S"),
    ])
    return True


def batch_seed_users(gs_client, users):
    """Same effect as calling seed_user() once per entry in `users` (each a
    dict with email/phone/name/company/department/password), but does it
    with ONE read (to find existing emails) and ONE write (to append every
    new row at once) instead of a read+write per user - avoids hitting the
    Sheets API per-minute read-request quota when seeding many accounts at
    once. Returns (created_count, skipped_count)."""
    ws = _get_users_ws(gs_client)
    existing_emails = {normalize_email(u["email"]) for u in get_all_users(gs_client)}

    rows = []
    created = 0
    skipped = 0
    seen_this_batch = set()
    for u in users:
        email = u.get("email", "")
        if not email:
            skipped += 1
            continue
        email_norm = normalize_email(email)
        if email_norm in existing_emails or email_norm in seen_this_batch:
            skipped += 1
            continue
        seen_this_batch.add(email_norm)
        phone = u.get("phone", "")
        phone_norm = normalize_phone(phone)
        pw = u.get("password") or phone_norm[-4:] or "0000"
        rows.append([
            email_norm, phone, u.get("name", ""), u.get("company", ""), u.get("department", ""),
            generate_password_hash(pw), "TRUE",
            bangkok_now().strftime("%Y-%m-%d %H:%M:%S"),
        ])
        created += 1

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
    return created, skipped


def batch_reset_passwords(gs_client, updates):
    """Force-sets the password for each {email, password} in `updates`,
    re-flagging must_change_password=TRUE so they still get the
    change-password prompt on next login. Does ONE read (to find each
    email's row) and ONE batched write (all cells at once) - same
    quota-friendly approach as batch_seed_users. Only touches emails that
    already exist; unknown emails are silently skipped. Returns the count
    of rows actually updated."""
    ws = _get_users_ws(gs_client)
    values = ws.get_all_values()
    email_to_row = {}
    for idx, row in enumerate(values[1:], start=2):  # data starts row 2
        if row and row[0]:
            email_to_row[normalize_email(row[0])] = idx

    batch_data = []
    updated = 0
    for u in updates:
        email_norm = normalize_email(u.get("email", ""))
        row_idx = email_to_row.get(email_norm)
        if not row_idx:
            continue
        new_hash = generate_password_hash(u.get("password") or "0000")
        batch_data.append({"range": f"F{row_idx}:G{row_idx}", "values": [[new_hash, "TRUE"]]})
        updated += 1

    if batch_data:
        ws.batch_update(batch_data)
    return updated
