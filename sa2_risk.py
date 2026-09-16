"""SA2 Risk - manual tracking board (explicit request). INOC staff type in
rows for at-risk tickets by hand; a row stays visible until someone
changes its Status to Closed and then explicitly deletes it (server-side
enforced: delete is only permitted for rows already marked Closed, so a
still-Pending row can never be silently removed). This is NOT an Excel
import like the other tabs - it's the primary data entry surface itself,
persisted directly to a dedicated Google Sheet tab (same gspread pattern
as temp_point_improvement.py's ManualOverrides sheet) so every viewer
sees the same live board and nothing is lost on a server restart.

SA2_RISK_SHEET_ID must be set to a real spreadsheet ID before this works
(see the module-level comment next to the constant) - there is no sheet
to default to, unlike the read-only Excel-import tabs.
"""

import logging
import uuid
from datetime import datetime

import gspread

log = logging.getLogger(__name__)

# ── REQUIRED ONE-TIME SETUP ──────────────────────────────────────────
# 1. Create a blank Google Sheet.
# 2. Share it (Editor access) with this app's service account email -
#    the same one already used for every other sheet this app reads/
#    writes (see the "client_email" field in the GOOGLE_CREDENTIALS_JSON
#    environment variable's JSON value).
# 3. Copy the sheet ID from its URL
#    (https://docs.google.com/spreadsheets/d/<THIS PART>/edit) and paste
#    it below. The worksheet tab itself is created automatically on
#    first use - only the spreadsheet needs to exist first.
SA2_RISK_SHEET_ID = "PUT_YOUR_SHEET_ID_HERE"
SA2_RISK_WORKSHEET_NAME = "SA2_Risk"

INOC_NAMES = ["INOC หนึ่ง", "INOC นิว", "INOC ปาย", "INOC เกมส์", "INOC ปอ", "INOC ป๊อบ",
              "INOC แฮ็ค", "INOC เต้ย", "INOC ภู", "INOC ทิพย์", "INOC หนุ่ม"]

SUP_NAMES = ["Konlachai Chulaphun", "Anuchit Phimmaha", "Jatuphon Janthong", "Anuchet Panyadee",
             "Sawat Yawirat", "Kriangkai Kenkhao", "Chetsada Khongkamhaeng", "khajornsak bhuttham",
             "Jutipon Tarasugarit", "Nakane Wongboonma", "Sutee Thiraworachai", "Ratthakarn Promyoo",
             "Maroot Sayti", "Wattana Pumpuang", "Pratchaya Runrack", "Nawin Bouryam",
             "Jirapan Suksuyon", "Chamrat Thonsang-in", "Somchai Panyatha", "Phraison Yaemsuriyothai",
             "Puchit Intawong", "Piyapol Nalee", "Nuttapon Promwongkhar", "Piyanat Krajangsri",
             "Teerapol Sueasen", "Sittichai Pluemsammanen", "Kamon Homwan", "Sanor Sukchim"]

PROVINCES = ["TRUE-TH-BBT-NOR1-CMI1-NOP", "TRUE-TH-BBT-NOR1-CMI2-NOP", "TRUE-TH-BBT-NOR1-CRI-NOP",
             "TRUE-TH-BBT-NOR1-LPG-NOP", "TRUE-TH-BBT-NOR1-LPN-NOP", "TRUE-TH-BBT-NOR1-MHS-NOP",
             "TRUE-TH-BBT-NOR1-NAN-NOP", "TRUE-TH-BBT-NOR1-PHE-NOP", "TRUE-TH-BBT-NOR1-PYO-NOP",
             "TRUE-TH-BBT-NOR2-KPP-NOP", "TRUE-TH-BBT-NOR2-PCB-NOP", "TRUE-TH-BBT-NOR2-PCT-NOP",
             "TRUE-TH-BBT-NOR2-PSN-NOP", "TRUE-TH-BBT-NOR2-SKT-NOP", "TRUE-TH-BBT-NOR2-TAK-NOP",
             "TRUE-TH-BBT-NOR2-UTR-NOP"]

TOPO_RING_OPTIONS = ["Send done", "ยังไม่ส่ง"]
CHANNEL_OPTIONS = ["Tel", "Tel & Line", "Line"]
STATUS_OPTIONS = ["Pending", "Closed"]

# Sheet column order - "id" and "updated_by"/"updated_at" are bookkeeping
# columns, never shown as form fields, but kept so a row can be found
# again for a status change or delete, and so it's clear who touched it
# last.
COLUMNS = [
    "id", "ticket_id", "create_dt", "inoc_open_dt", "inoc_name", "topo_ring",
    "channel", "sup_name", "sup_ack_dt", "risk_sites", "subject", "province",
    "site_name", "status", "closed_dt", "updated_by", "updated_at",
]


class SA2RiskError(Exception):
    pass


def _get_worksheet(gc):
    if SA2_RISK_SHEET_ID == "PUT_YOUR_SHEET_ID_HERE":
        raise SA2RiskError(
            "ยังไม่ได้ตั้งค่า SA2_RISK_SHEET_ID ในไฟล์ sa2_risk.py — "
            "สร้าง Google Sheet เปล่าๆ, แชร์ให้ Service Account ของระบบนี้ (Editor), "
            "แล้วเอา Sheet ID ไปใส่ในตัวแปร SA2_RISK_SHEET_ID"
        )
    try:
        sh = gc.open_by_key(SA2_RISK_SHEET_ID)
    except Exception as e:
        raise SA2RiskError(f"เปิด Google Sheet ไม่สำเร็จ (เช็คว่าแชร์สิทธิ์ Editor ให้ Service Account แล้วหรือยัง): {e}")
    try:
        ws = sh.worksheet(SA2_RISK_WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SA2_RISK_WORKSHEET_NAME, rows=500, cols=len(COLUMNS))
        ws.update([COLUMNS])
    return ws


def _row_to_dict(header, raw):
    idx = {h: i for i, h in enumerate(header)}
    get = lambda col: raw[idx[col]] if col in idx and idx[col] < len(raw) else ""
    return {col: get(col) for col in COLUMNS}


def get_all_rows(gc):
    ws = _get_worksheet(gc)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []
    header = values[0]
    rows = [_row_to_dict(header, raw) for raw in values[1:] if any(c.strip() for c in raw)]
    return rows


def add_row(gc, data, updated_by=None):
    """data: dict with the 13 person-entered fields (ticket_id,
    create_dt, inoc_open_dt, inoc_name, topo_ring, channel, sup_name,
    sup_ack_dt, risk_sites, subject, province, site_name, status).
    ticket_id is the one truly required field - everything else can be
    filled in gradually, since this is a live-incident form someone is
    filling out under time pressure, not a strict validated import."""
    ticket_id = (data.get("ticket_id") or "").strip()
    if not ticket_id:
        raise SA2RiskError("กรุณากรอก Ticket ID")

    ws = _get_worksheet(gc)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_id = uuid.uuid4().hex[:12]
    status = data.get("status") or "Pending"
    row = {
        "id": row_id,
        "ticket_id": ticket_id,
        "create_dt": data.get("create_dt") or "",
        "inoc_open_dt": data.get("inoc_open_dt") or "",
        "inoc_name": data.get("inoc_name") or "",
        "topo_ring": data.get("topo_ring") or "Send done",
        "channel": data.get("channel") or "",
        "sup_name": data.get("sup_name") or "",
        "sup_ack_dt": data.get("sup_ack_dt") or "",
        "risk_sites": data.get("risk_sites") or 0,
        "subject": data.get("subject") or "",
        "province": data.get("province") or "",
        "site_name": data.get("site_name") or "",
        "status": status,
        "closed_dt": now if status == "Closed" else "",
        "updated_by": updated_by or "",
        "updated_at": now,
    }
    ws.append_row([row[c] for c in COLUMNS])
    return row


def _find_row_index(ws, row_id):
    """Returns the 1-indexed sheet row number for the given id, or None.
    Scans the id column directly rather than get_all_values() for every
    call - fine at this data volume (a manual-entry board, never
    thousands of rows) and keeps each status/delete request to a single
    small read."""
    id_col = ws.find(row_id, in_column=COLUMNS.index("id") + 1)
    return id_col.row if id_col else None


def update_status(gc, row_id, new_status, updated_by=None):
    if new_status not in STATUS_OPTIONS:
        raise SA2RiskError(f"Status ไม่ถูกต้อง: {new_status}")
    ws = _get_worksheet(gc)
    row_num = _find_row_index(ws, row_id)
    if row_num is None:
        raise SA2RiskError("ไม่พบแถวนี้ (อาจถูกลบไปแล้ว)")

    status_col = COLUMNS.index("status") + 1
    closed_col = COLUMNS.index("closed_dt") + 1
    updated_by_col = COLUMNS.index("updated_by") + 1
    updated_at_col = COLUMNS.index("updated_at") + 1
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ws.update_cell(row_num, status_col, new_status)
    ws.update_cell(row_num, closed_col, now if new_status == "Closed" else "")
    ws.update_cell(row_num, updated_by_col, updated_by or "")
    ws.update_cell(row_num, updated_at_col, now)
    return True


def delete_row(gc, row_id):
    """Only permits deleting a row whose CURRENT status (re-checked here,
    not trusted from the request) is Closed - a Pending row can never be
    removed via this endpoint, even if the client-side UI were bypassed."""
    ws = _get_worksheet(gc)
    row_num = _find_row_index(ws, row_id)
    if row_num is None:
        raise SA2RiskError("ไม่พบแถวนี้ (อาจถูกลบไปแล้ว)")

    status_col = COLUMNS.index("status") + 1
    current_status = ws.cell(row_num, status_col).value
    if current_status != "Closed":
        raise SA2RiskError("ลบได้เฉพาะแถวที่ Status เป็น Closed เท่านั้น")

    ws.delete_rows(row_num)
    return True


def build_options():
    """Static dropdown option lists for the Add Row form - served from
    the backend (rather than hardcoded twice in JS too) so there's one
    place to update the name/province lists."""
    return {
        "inoc_names": INOC_NAMES,
        "sup_names": SUP_NAMES,
        "provinces": PROVINCES,
        "topo_ring_options": TOPO_RING_OPTIONS,
        "channel_options": CHANNEL_OPTIONS,
        "status_options": STATUS_OPTIONS,
    }
