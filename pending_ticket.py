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

from pending_trend import get_drive_and_sheets_clients, bangkok_now, AGING_COLORS
from realtime_monitor import REALTIME_SHEET_ID, REALTIME_WORKSHEET_GID, _parse_dt, _classify_priority

log = logging.getLogger(__name__)

ALLOWED_SEVERITIES = {"SA1", "SA2", "SA3", "SA4", "NSA1", "NSA2", "NSA3", "NSA4"}
PENDING_TICKET_REGIONS = {"NOR1", "NOR2"}  # narrower than pending_trend's ALLOWED_REGIONS on purpose

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

# External mirror: every build of the Pending Ticket table also gets written
# here in full, exactly matching what's shown on the web page, plus an
# insert_time column stamping when that write happened.
EXPORT_SHEET_ID = "10Y3Pyp6-MqlrcxiDXdAjTNWWZLgDWp94dZvU51lOxWU"
EXPORT_WORKSHEET_GID = 1260767128
EXPORT_HEADER = [
    "TICKETID", "CINAME", "SUBJECT", "Priority", "CREATIONDATE", "TARGETFINISH",
    "SEVERITY", "TRUEOWNERGROUP", "Bookmark", "Aging_Flag_Group", "SUBDISTRICT",
    "DISTRICT", "Tech_Team", "Tech_Status", "NANO", "Group_Problem", "Action_Team",
    "Detail", "Image_Link", "Plan_Closed_Date", "Updated_At", "Updated_By", "insert_time",
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


def _get_export_worksheet(gs_client):
    sh = gs_client.open_by_key(EXPORT_SHEET_ID)
    for ws in sh.worksheets():
        if ws.id == EXPORT_WORKSHEET_GID:
            return ws
    return sh.sheet1


def export_to_external_sheet(gs_client, tickets, insert_time_str):
    """Mirrors the full current Pending Ticket table (exactly what's on the
    web page) into the external sheet, overwriting whatever was there before.
    Called once per build_pending_ticket_response(), so it stays in sync
    every time the tab is loaded or refreshed."""
    ws = _get_export_worksheet(gs_client)
    rows = [[
        t.get("TICKETID", ""), t.get("CINAME", ""), t.get("SUBJECT", ""), t.get("priority", ""),
        t.get("CREATIONDATE", ""), t.get("TARGETFINISH", ""), t.get("SEVERITY", ""),
        t.get("TRUEOWNERGROUP", ""), t.get("Bookmark", ""), t.get("Aging_Flag_Group", ""),
        t.get("SUBDISTRICT", ""), t.get("DISTRICT", ""), t.get("Tech_Team", ""), t.get("Tech_Status", ""),
        t.get("nano", ""), t.get("group_problem", ""), t.get("action_team", ""), t.get("detail", ""),
        t.get("image_link", ""), t.get("plan_closed_date", ""), t.get("updated_at", ""), t.get("updated_by", ""),
        insert_time_str,
    ] for t in tickets]
    ws.clear()
    ws.update("A1", [EXPORT_HEADER] + rows, value_input_option="RAW")
    log.info("Exported %d rows to external Pending Ticket mirror sheet", len(rows))


# ---------------------------------------------------------------------------
# Combined response
# ---------------------------------------------------------------------------

def _multi_filter(entries, key, values):
    if not values:
        return entries
    values_set = set(values)
    return [e for e in entries if str(e.get(key, "")).strip() in values_set]


def build_pending_ticket_response(gs_client=None, bookmark_filter=None, trueowner_filter=None,
                                   severity_filter=None, district_filter=None):
    """bookmark_filter/trueowner_filter/severity_filter/district_filter: each an
    optional LIST of values (multi-select) - None or empty means "no filter,
    include everything in that dimension"."""
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()

    all_rows = fetch_live_rows(gs_client)
    now_dt = bangkok_now()
    today = now_dt.date()

    scoped = [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in PENDING_TICKET_REGIONS
        and str(r.get("SEVERITY", "")).strip() in ALLOWED_SEVERITIES
    ]

    work_log = load_work_log(gs_client)

    def build_entry(r):
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
        return entry

    all_entries = [build_entry(r) for r in scoped]
    all_entries.sort(key=lambda t: (_bookmark_sort_key(str(t.get("Bookmark", "")).strip()), -t["over_sla_day"]))

    filter_options = {
        "bookmarks": sorted({str(e.get("Bookmark", "")).strip() for e in all_entries if e.get("Bookmark")}),
        "trueowners": sorted({str(e.get("TRUEOWNERGROUP", "")).strip() for e in all_entries if e.get("TRUEOWNERGROUP")}),
        "severities": sorted({str(e.get("SEVERITY", "")).strip() for e in all_entries if e.get("SEVERITY")}),
    }

    # District counts are faceted on the OTHER active filters (bookmark/trueowner/
    # severity) so the numbers next to each district option stay accurate as
    # those filters change - just not on the district filter itself.
    pre_district = _multi_filter(all_entries, "Bookmark", bookmark_filter)
    pre_district = _multi_filter(pre_district, "TRUEOWNERGROUP", trueowner_filter)
    pre_district = _multi_filter(pre_district, "SEVERITY", severity_filter)
    district_counts = {}
    for e in pre_district:
        d = str(e.get("DISTRICT", "")).strip()
        if d:
            district_counts[d] = district_counts.get(d, 0) + 1
    filter_options["districts"] = [
        {"district": d, "count": c}
        for d, c in sorted(district_counts.items(), key=lambda kv: kv[1], reverse=True)
    ]

    matched_entries = _multi_filter(pre_district, "DISTRICT", district_filter)

    export_insert_time = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        export_to_external_sheet(gs_client, all_entries, export_insert_time)  # always the FULL, unfiltered set
    except Exception:
        log.exception("Failed to export Pending Ticket table to external mirror sheet - continuing anyway")

    return {
        "total": len(matched_entries),
        "filter_options": filter_options,
        "group_problem_options": GROUP_PROBLEM_OPTIONS,
        "action_team_options": ACTION_TEAM_OPTIONS,
        "aging_colors": AGING_COLORS,
        "insert_time": all_rows[0].get("insert_time") if all_rows else None,
        "export_insert_time": export_insert_time,
        "tickets": matched_entries,
    }
