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
import threading
import time
from datetime import datetime, date

from pending_trend import get_drive_and_sheets_clients, bangkok_now, AGING_COLORS, AGING_ORDER, OVER_24H_AGING_KEYS
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
    "Solar Cell - Off Grid",
    "Clear",
]

ACTION_TEAM_OPTIONS = ["OFC", "NODE", "SUP", "ENG Zone", "Special Team", "Planwork", "Track 7", "E//", "Other Vendor"]

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


# Two very different caching needs share this file:
#
# - live ticket rows: fed by an EXTERNAL system (SCCD+ITSM) that re-stamps
#   `insert_time` on its own frequent cadence (every few minutes, as new
#   tickets stream in) - not the rarely-changing kind of value the
#   insert_time trick works well for. An earlier version of this function
#   read that cell first as a "cheap check" before deciding whether to do
#   the full get_all_records() read, mirroring realtime_monitor.py - but
#   because insert_time here changes on nearly every load anyway, that
#   turned one Sheets API round trip into two almost every time (the cheap
#   check, then the full read right after since insert_time had already
#   moved on) - net SLOWER, which is what caused the "หน้านี้มันนานไป"
#   report. Reverted to a plain short-TTL cache below: no extra round trip,
#   just reuse the same read for anyone loading within the TTL window.
# - work log: written entirely through save_work_log_entry() in THIS app, so
#   it explicitly invalidates its cache on every save - safe to cache for a
#   long time since staleness can only come from someone editing the sheet
#   directly outside the app.
#
# Both caches are safe to rely on only because Gunicorn runs a single worker
# process (see Procfile) - this in-memory cache is shared by every request.
# Raising the worker count would split it across processes and let stale
# reads slip through; use more threads instead if more concurrency is ever
# needed, not more workers.
LIVE_ROWS_CACHE_TTL_SECONDS = 15    # short: this sheet changes every few minutes anyway
WORK_LOG_CACHE_TTL_SECONDS = 7200   # 2h; save_work_log_entry invalidates explicitly
_live_rows_cache = {"data": None, "ts": 0}
_work_log_cache = {"data": None, "ts": 0}
_cache_lock = threading.Lock()


def fetch_live_rows(gs_client, use_cache=True):
    now = time.monotonic()
    if use_cache:
        with _cache_lock:
            if _live_rows_cache["data"] is not None and (now - _live_rows_cache["ts"]) < LIVE_ROWS_CACHE_TTL_SECONDS:
                return _live_rows_cache["data"]

    ws = _get_worksheet(gs_client)
    rows = ws.get_all_records()
    if use_cache:
        with _cache_lock:
            _live_rows_cache["data"] = rows
            _live_rows_cache["ts"] = now
    return rows



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


def load_work_log(gs_client, use_cache=True):
    """Returns {ticket_id: {group_problem, action_team, detail, image_link,
    plan_closed_date, updated_at, updated_by}} for every saved row."""
    now = time.monotonic()
    if use_cache:
        with _cache_lock:
            if _work_log_cache["data"] is not None and (now - _work_log_cache["ts"]) < WORK_LOG_CACHE_TTL_SECONDS:
                return _work_log_cache["data"]
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
    if use_cache:
        with _cache_lock:
            _work_log_cache["data"] = out
            _work_log_cache["ts"] = now
    return out


def _invalidate_work_log_cache():
    with _cache_lock:
        _work_log_cache["data"] = None
        _work_log_cache["ts"] = 0


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
    _invalidate_work_log_cache()
    return row_values


def rename_group_problem_value(gs_client, old_value, new_value):
    """One-time cleanup: renames every occurrence of `old_value` in the
    group_problem column of TicketWorkLog to `new_value` (e.g. consolidating
    two similar options into one after the dropdown list changes). Returns
    how many rows were updated."""
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_work_log_tab(sh)
    values = ws.get_all_values()
    if not values:
        return 0
    updates = []
    changed = 0
    for i, row in enumerate(values[1:], start=2):  # skip header
        if len(row) > 1 and row[1] == old_value:
            updates.append({"range": f"B{i}", "values": [[new_value]]})
            changed += 1
    if updates:
        ws.batch_update(updates, value_input_option="RAW")
        _invalidate_work_log_cache()
    return changed


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
                                   severity_filter=None, district_filter=None, group_problem_filter=None,
                                   aging_filter=None):
    """bookmark_filter/trueowner_filter/severity_filter/district_filter/group_problem_filter/
    aging_filter: each an optional LIST of values (multi-select) - None or empty means "no
    filter, include everything in that dimension"."""
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

    present_agings = {str(e.get("Aging_Flag_Group", "")).strip() for e in all_entries if e.get("Aging_Flag_Group")}
    filter_options = {
        "bookmarks": sorted({str(e.get("Bookmark", "")).strip() for e in all_entries if e.get("Bookmark")}),
        "trueowners": sorted({str(e.get("TRUEOWNERGROUP", "")).strip() for e in all_entries if e.get("TRUEOWNERGROUP")}),
        "severities": sorted({str(e.get("SEVERITY", "")).strip() for e in all_entries if e.get("SEVERITY")}),
        # Kept in canonical Aging_Flag_Group order (not alphabetical) so the
        # dropdown reads 1)->6) like everywhere else Aging is shown.
        "agings": [a for a in AGING_ORDER if a in present_agings],
    }

    # District counts are faceted on the OTHER active filters (bookmark/trueowner/
    # severity/aging) so the numbers next to each district option stay accurate
    # as those filters change - just not on the district filter itself.
    pre_district = _multi_filter(all_entries, "Bookmark", bookmark_filter)
    pre_district = _multi_filter(pre_district, "TRUEOWNERGROUP", trueowner_filter)
    pre_district = _multi_filter(pre_district, "SEVERITY", severity_filter)
    pre_district = _multi_filter(pre_district, "Aging_Flag_Group", aging_filter)
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
    matched_entries = _multi_filter(matched_entries, "group_problem", group_problem_filter)

    filter_options["group_problems"] = sorted({
        str(e.get("group_problem", "")).strip() for e in all_entries if e.get("group_problem")
    })

    # Mirrors the full table to an external tracking sheet. This write can be
    # slow (700+ rows, full clear+rewrite) and the page doesn't need to wait
    # on it, so it runs in the background instead of blocking the response -
    # this was previously the single biggest contributor to page load time.
    export_insert_time = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")

    def _export_in_background(entries, insert_time_str):
        try:
            export_to_external_sheet(gs_client, entries, insert_time_str)
        except Exception:
            log.exception("Failed to export Pending Ticket table to external mirror sheet - continuing anyway")

    threading.Thread(target=_export_in_background, args=(all_entries, export_insert_time), daemon=True).start()

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


# ---------------------------------------------------------------------------
# Exclusive Pending — morning-meeting summary: for each Bookmark, a
# Group Problem x Aging_Flag_Group matrix, plus the ticket-level detail for
# anything sitting in the four "over SLA" aging buckets (1-4; excludes
# "< 1 day" and "Within SLA" since those aren't meeting-worthy yet).
# ---------------------------------------------------------------------------

EXCLUSIVE_BOOKMARK_ORDER = BOOKMARK_SORT_ORDER + ["Others"]
UNSPECIFIED_GROUP_PROBLEM = "(ยังไม่ระบุ)"
UNSPECIFIED_AGING = "(ไม่ระบุ)"


def _exclusive_bookmark_label(raw):
    raw = str(raw or "").strip()
    return raw if raw in BOOKMARK_SORT_ORDER else "Others"


def build_exclusive_pending_response(gs_client=None, priority_filter=None, restrict_to_over_sla=True):
    """Morning-meeting view: how many pending tickets per Bookmark are stuck
    on which Group Problem, broken out by aging bucket - plus the full
    ticket-level list for anything already over SLA (aging buckets 1-4),
    so a team lead can answer CNO questions ticket-by-ticket without
    switching tabs. `priority_filter` (e.g. "P0") restricts EVERYTHING -
    matrix, province breakdowns, plan-date matrix, detail lists - to just
    that priority, for a page like "P0 Only" that needs the exact same
    view scoped down to the most urgent tickets. `restrict_to_over_sla`
    controls the DETAIL list only: True (default, normal Exclusive
    Pending) limits it to the four over-SLA aging buckets; False includes
    every ticket regardless of aging bucket - used by "P0 Only" so it
    shows literally every P0 ticket, not just the ones already overdue."""
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()

    all_rows = fetch_live_rows(gs_client)
    now_dt = bangkok_now()
    scoped = [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in PENDING_TICKET_REGIONS
        and str(r.get("SEVERITY", "")).strip() in ALLOWED_SEVERITIES
    ]
    work_log = load_work_log(gs_client)

    entries = []
    for r in scoped:
        ticket_id = str(r.get("TICKETID", "")).strip()
        wl = work_log.get(ticket_id, {})
        over_sla_day = r.get("Over_SLA_Day")
        try:
            over_sla_day = float(over_sla_day)
        except (TypeError, ValueError):
            over_sla_day = 0
        priority = _classify_priority(r.get("TARGETFINISH"), now_dt)
        if priority_filter and priority != priority_filter:
            continue
        entries.append({
            "TICKETID": ticket_id,
            "SUBJECT": r.get("SUBJECT", ""),
            "CINAME": r.get("CINAME", ""),
            "DISTRICT": r.get("DISTRICT", ""),
            "PROVINCE": r.get("PROVINCE", ""),
            "TRUEOWNERGROUP": r.get("TRUEOWNERGROUP", ""),
            "priority": priority,
            "Bookmark": _exclusive_bookmark_label(r.get("Bookmark")),
            "Aging_Flag_Group": str(r.get("Aging_Flag_Group", "")).strip() or UNSPECIFIED_AGING,
            "group_problem": wl.get("group_problem") or UNSPECIFIED_GROUP_PROBLEM,
            "action_team": wl.get("action_team", ""),
            "detail": wl.get("detail", ""),
            "plan_closed_date": wl.get("plan_closed_date", ""),
            "over_sla_day": over_sla_day,
        })

    # Summary matrix: bookmark -> group_problem -> aging_key -> count
    summary = {}
    for e in entries:
        (summary.setdefault(e["Bookmark"], {})
                 .setdefault(e["group_problem"], {})
                 .setdefault(e["Aging_Flag_Group"], 0))
        summary[e["Bookmark"]][e["group_problem"]][e["Aging_Flag_Group"]] += 1

    summary_out = []
    for bm in EXCLUSIVE_BOOKMARK_ORDER:
        if bm not in summary:
            continue
        rows = []
        for gp, counts in summary[bm].items():
            over_total = sum(counts.get(k, 0) for k in OVER_24H_AGING_KEYS)
            row = {"group_problem": gp, "over_total": over_total, "counts": {
                ag: counts.get(ag, 0) for ag in AGING_ORDER
            }}
            rows.append(row)
        rows.sort(key=lambda r: -r["over_total"])
        bookmark_total = sum(r["over_total"] for r in rows)
        summary_out.append({"bookmark": bm, "rows": rows, "over_total": bookmark_total})

    # Detail: over-SLA aging buckets (1-4) only by default, matching the
    # normal Exclusive Pending view - unless restrict_to_over_sla is off
    # (P0 Only), in which case every entry that made it this far (already
    # priority-filtered above) is included regardless of aging bucket.
    detail_entries = [e for e in entries if not restrict_to_over_sla or e["Aging_Flag_Group"] in OVER_24H_AGING_KEYS]
    detail_entries.sort(key=lambda e: -e["over_sla_day"])

    detail_out = []
    unspecified_by_province_out = []
    group_problem_by_plan_date_out = []
    NO_PLAN_DATE = "(ยังไม่วางแผน)"
    for bm in EXCLUSIVE_BOOKMARK_ORDER:
        tickets = [e for e in detail_entries if e["Bookmark"] == bm]
        if not tickets:
            continue
        detail_out.append({"bookmark": bm, "tickets": tickets, "total": len(tickets)})

        # Among this Bookmark's over-SLA tickets, break down by PROVINCE how
        # many are still missing each of the four fields a lead needs to
        # answer CNO with (Group Problem, Action Team, รายละเอียด, Plan
        # Closed Date) - most affected province first, so gaps in triage
        # are visible at a glance.
        province_counts = {}
        for e in tickets:
            prov = str(e["PROVINCE"]).strip() or "(ไม่ระบุจังหวัด)"
            row = province_counts.setdefault(prov, {
                "missing_group_problem": 0, "missing_action_team": 0,
                "missing_detail": 0, "missing_plan_closed_date": 0,
            })
            if e["group_problem"] == UNSPECIFIED_GROUP_PROBLEM:
                row["missing_group_problem"] += 1
            if not str(e["action_team"]).strip():
                row["missing_action_team"] += 1
            if not str(e["detail"]).strip():
                row["missing_detail"] += 1
            if not str(e["plan_closed_date"]).strip():
                row["missing_plan_closed_date"] += 1
        province_rows = sorted(
            (
                {"province": p, **counts, "total_missing": sum(counts.values())}
                for p, counts in province_counts.items()
                if sum(counts.values()) > 0
            ),
            key=lambda x: (-x["missing_group_problem"], -x["total_missing"])
        )
        if province_rows:
            unspecified_by_province_out.append({"bookmark": bm, "provinces": province_rows})

        # Group Problem x Plan Closed Date matrix - who's stuck on what, and
        # when it's actually planned to close (or not planned at all yet).
        plan_dates = sorted({t["plan_closed_date"] for t in tickets if t["plan_closed_date"]})
        date_columns = plan_dates + [NO_PLAN_DATE]
        gp_plan_matrix = {}
        for e in tickets:
            col = e["plan_closed_date"] or NO_PLAN_DATE
            gp_plan_matrix.setdefault(e["group_problem"], {}).setdefault(col, 0)
            gp_plan_matrix[e["group_problem"]][col] += 1
        gp_plan_rows = []
        for gp, counts in gp_plan_matrix.items():
            row_total = sum(counts.values())
            gp_plan_rows.append({
                "group_problem": gp,
                "counts": {c: counts.get(c, 0) for c in date_columns},
                "total": row_total,
            })
        gp_plan_rows.sort(key=lambda r: -r["total"])
        if gp_plan_rows:
            group_problem_by_plan_date_out.append({
                "bookmark": bm, "date_columns": date_columns, "rows": gp_plan_rows,
            })

    return {
        "aging_order": AGING_ORDER,
        "over_aging_keys": OVER_24H_AGING_KEYS,
        "aging_colors": AGING_COLORS,
        "summary": summary_out,
        "detail": detail_out,
        "unspecified_by_province": unspecified_by_province_out,
        "group_problem_by_plan_date": group_problem_by_plan_date_out,
        "total_over_sla": len(detail_entries),
        "generated_at": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }
