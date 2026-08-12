"""
pending_ticket.py
------------------
NOR2026 / BBTEC NOC — "Pending Ticket" tab: an operational worksheet where the
team logs what's being done about each pending ticket (problem group, action
team, free-text notes, a work-photo link, a planned close date).

Data flow:
  - Ticket facts (TICKETID, CREATIONDATE, SEVERITY, ...) are read LIVE from
    the same raw sheet realtime_monitor.py uses (REALTIME_SHEET_ID, gid=0).
    That tab is overwritten wholesale by an external export process every
    hour, so nothing the team types can live there - it would vanish on the
    next refresh.
  - Team-entered fields are instead stored in a SEPARATE tab within the SAME
    spreadsheet, "TicketWorkLog", keyed by TICKETID. Every page load joins
    live ticket rows with any saved work-log row for that TICKETID. A ticket
    that's still pending next hour just gets rejoined with the same saved
    work-log data - nothing is lost. A ticket that clears (stops appearing in
    the live sheet) simply stops showing up; its old work-log row is left in
    place (harmless leftover, not actively cleaned up).

Priority (P0/P1/P2): identical formula to realtime_monitor.py.
NANO flag: NN_ClusterID not empty / not "None" -> "NANO".
"""

import logging
from datetime import datetime, date

from pending_trend import get_drive_and_sheets_clients, bangkok_now, ALLOWED_REGIONS
from realtime_monitor import REALTIME_SHEET_ID, REALTIME_WORKSHEET_GID, _parse_dt, _classify_priority

log = logging.getLogger(__name__)

ALLOWED_SEVERITIES = {"SA1", "SA2", "SA3", "SA4", "NSA1", "NSA2", "NSA3", "NSA4"}

# Sort/group order: named bookmarks first (in this exact order), then
# anything else lumped into "Others".
BOOKMARK_SORT_ORDER = ["7.MB with SA1-4", "4.FBB with SA1-4", "3. All NW Incident NSA1-2"]

LIVE_COLUMNS = [
    "TICKETID", "CREATIONDATE", "TARGETFINISH", "SEVERITY", "SUBJECT", "CINAME",
    "TRUEOWNERGROUP", "Bookmark", "Aging_Flag_Group", "SUBDISTRICT", "DISTRICT",
    "Tech_Team", "Tech_Status",
]

GROUP_PROBLEM_OPTIONS = [
    "Spare part - Spare part not available",
    "Spare part - Wait on site to replace",
    "Budget required - Approve in progress",
    "Site renovate - area is still closed",
    "Site permission - Requesting or pending approval",
    "Site permission - Approved and awaiting site entry",
    "Site dismantle - Wait for configuration removal",
    "Improvement - Awarded subcons",
    "Improvement - Track B",
    "Wait for other parties - Back office/Vendor support",
    "Wait for other parties - Other operators/Provider",
    "Risk Area - Protest/Dangerous area",
    "Disaster - Strom/Flood/Earthquake",
    "Difficulte work - Site equipment was stolen",
    "Difficulte work - Work at high",
    "Fiber optic - Fire burn/High voltage short circuit",
    "Fiber optic \u2013 Accident/Constuction",
    "Fiber optic - PEA/MEA Maintenance transmission line",
    "Fiber optic - High loss/Degrade/Animal",
    "Workload - Assigning team access site",
    "Workload - Assigning team access island/border area",
    "Solar Cell - Off Grid",
    "Clear",
]

ACTION_TEAM_OPTIONS = ["OFC", "NODE", "SUP", "ENG Zone", "Specail Team"]

WORK_LOG_SHEET = "TicketWorkLog"
WORK_LOG_HEADER = [
    "ticket_id", "group_problem", "action_team", "detail", "image_link",
    "plan_closed_date", "updated_at", "updated_by",
]


# ---------------------------------------------------------------------------
# Live ticket rows
# ---------------------------------------------------------------------------

def _get_worksheet(gs_client):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    for ws in sh.worksheets():
        if ws.id == REALTIME_WORKSHEET_GID:
            return ws
    return sh.sheet1


def fetch_live_rows(gs_client):
    ws = _get_worksheet(gs_client)
    return ws.get_all_records()


def _bookmark_sort_key(bookmark):
    try:
        return BOOKMARK_SORT_ORDER.index(bookmark)
    except ValueError:
        return len(BOOKMARK_SORT_ORDER)  # "Others" - sorts after the named ones


# ---------------------------------------------------------------------------
# Work-log persistence (separate tab, survives the live sheet's hourly wipe)
# ---------------------------------------------------------------------------

def _ensure_work_log_tab(spreadsheet):
    try:
        return spreadsheet.worksheet(WORK_LOG_SHEET)
    except Exception:
        ws = spreadsheet.add_worksheet(title=WORK_LOG_SHEET, rows=2000, cols=len(WORK_LOG_HEADER))
        ws.append_row(WORK_LOG_HEADER)
        return ws


def load_work_log(gs_client):
    """Returns {ticket_id: {group_problem, action_team, detail, image_link,
    plan_closed_date, updated_at, updated_by}} for every saved row."""
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_work_log_tab(sh)
    records = ws.get_all_values()[1:]  # skip header
    out = {}
    for row in records:
        if not row or not row[0]:
            continue
        padded = row + [""] * (len(WORK_LOG_HEADER) - len(row))
        out[padded[0]] = {
            "group_problem": padded[1], "action_team": padded[2], "detail": padded[3],
            "image_link": padded[4], "plan_closed_date": padded[5],
            "updated_at": padded[6], "updated_by": padded[7],
        }
    return out


def save_work_log_entry(gs_client, ticket_id, fields, updated_by=None):
    """fields: dict with any of group_problem/action_team/detail/image_link/
    plan_closed_date. Upserts the row for ticket_id, stamping updated_at (and
    updated_by once a login system exists - for now defaults to 'unknown')."""
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_work_log_tab(sh)
    existing = ws.col_values(1)  # ticket_id column

    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    row_values = [
        ticket_id,
        fields.get("group_problem", ""),
        fields.get("action_team", ""),
        fields.get("detail", ""),
        fields.get("image_link", ""),
        fields.get("plan_closed_date", ""),
        now_str,
        updated_by or "unknown",
    ]

    if ticket_id in existing:
        row_idx = existing.index(ticket_id) + 1
        ws.update(f"A{row_idx}:H{row_idx}", [row_values])
    else:
        ws.append_row(row_values)
    return row_values


# ---------------------------------------------------------------------------
# Combined response
# ---------------------------------------------------------------------------

def build_pending_ticket_response(gs_client=None, bookmark_filter=None, trueowner_filter=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()

    all_rows = fetch_live_rows(gs_client)
    now_dt = bangkok_now()
    today = now_dt.date()

    scoped = [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in ALLOWED_REGIONS
        and str(r.get("SEVERITY", "")).strip() in ALLOWED_SEVERITIES
    ]

    filter_options = {
        "bookmarks": sorted({str(r.get("Bookmark", "")).strip() for r in scoped if r.get("Bookmark")}),
        "trueowners": sorted({str(r.get("TRUEOWNERGROUP", "")).strip() for r in scoped if r.get("TRUEOWNERGROUP")}),
    }

    matched = scoped
    if bookmark_filter:
        matched = [r for r in matched if str(r.get("Bookmark", "")).strip() == bookmark_filter]
    if trueowner_filter:
        matched = [r for r in matched if str(r.get("TRUEOWNERGROUP", "")).strip() == trueowner_filter]

    work_log = load_work_log(gs_client)

    tickets = []
    for r in matched:
        ticket_id = str(r.get("TICKETID", "")).strip()
        nn_cluster = r.get("NN_ClusterID")
        nano = "NANO" if nn_cluster not in (None, "", "None") else ""
        over_sla_day = r.get("Over_SLA_Day")
        try:
            over_sla_day = float(over_sla_day)
        except (TypeError, ValueError):
            over_sla_day = 0

        entry = {c: r.get(c) for c in LIVE_COLUMNS}
        entry["over_sla_day"] = over_sla_day
        entry["priority"] = _classify_priority(r.get("TARGETFINISH"), now_dt)
        entry["nano"] = nano
        wl = work_log.get(ticket_id, {})
        entry["group_problem"] = wl.get("group_problem", "")
        entry["action_team"] = wl.get("action_team", "")
        entry["detail"] = wl.get("detail", "")
        entry["image_link"] = wl.get("image_link", "")
        entry["plan_closed_date"] = wl.get("plan_closed_date", "")
        entry["updated_at"] = wl.get("updated_at", "")
        entry["updated_by"] = wl.get("updated_by", "")
        entry["plan_closed_overdue"] = False
        if entry["plan_closed_date"]:
            try:
                pcd = datetime.strptime(entry["plan_closed_date"], "%Y-%m-%d").date()
                entry["plan_closed_overdue"] = pcd < today
            except ValueError:
                pass
        tickets.append(entry)

    tickets.sort(key=lambda t: (_bookmark_sort_key(str(t.get("Bookmark", "")).strip()), -t["over_sla_day"]))

    return {
        "total": len(tickets),
        "filter_options": filter_options,
        "group_problem_options": GROUP_PROBLEM_OPTIONS,
        "action_team_options": ACTION_TEAM_OPTIONS,
        "insert_time": all_rows[0].get("insert_time") if all_rows else None,
        "tickets": tickets,
    }
