"""Online SA1-4 Real Time Monitoring - a NEW tab, reusing existing live
data/priority/mateline/team logic end to end (see the docstring on each
reused function for what it already does). NOT restricted to P0 only -
shows every currently-open Online SA1-4 ticket (P0/P1/P2), sorted by
remaining time to TARGETFINISH ascending, most urgent first.

Remaining Time itself is meant to visibly tick on screen without
re-querying Sheets - this module sends each entry's raw TARGETFINISH once
per fetch, and the frontend recomputes "remaining" locally against its
own clock on a short interval. That split is the whole point: it's
exactly the pattern that avoids the per-minute Sheets API quota problem
already diagnosed and fixed elsewhere in this app (see
realtime_monitor.get_insert_time's docstring for that same lesson).
"""

import logging

from pending_ticket import (
    fetch_live_rows, PENDING_TICKET_REGIONS, ALLOWED_SEVERITIES,
    load_work_log, UNSPECIFIED_AGING, UNSPECIFIED_GROUP_PROBLEM,
)
from mateline_status import build_mateline_status_lookup
from team_planner import build_team_assignment_lookup
from realtime_monitor import _classify_priority, _parse_dt
from pending_trend import bangkok_now
import sla_improvement  # reused only for its CI_Name -> Node ID -> Lat/Lon mapping (get_mapping_lookup/match_ci_to_node) - no other coupling to that module

log = logging.getLogger(__name__)

ONLINE_BOOKMARK_RAW = "4.FBB with SA1-4"


def _remaining_hours(target_finish_str, now_dt):
    t = _parse_dt(target_finish_str)
    if not t:
        return None
    return round((t - now_dt).total_seconds() / 3600, 3)


def build_online_realtime_response(gs_client=None):
    if gs_client is None:
        from pending_trend import get_drive_and_sheets_clients
        _, gs_client = get_drive_and_sheets_clients()

    now_dt = bangkok_now()
    all_rows = fetch_live_rows(gs_client)
    scoped = [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in PENDING_TICKET_REGIONS
        and str(r.get("SEVERITY", "")).strip() in ALLOWED_SEVERITIES
        and str(r.get("Bookmark", "")).strip() == ONLINE_BOOKMARK_RAW
    ]
    work_log = load_work_log(gs_client)
    try:
        mateline_lookup = build_mateline_status_lookup(gs_client, now_dt.strftime("%Y-%m-%d"))
    except Exception:
        log.exception("GGS Daily mateline lookup failed for Online Real Time - falling back to empty for this response")
        mateline_lookup = {}
    try:
        team_lookup, _team_workload = build_team_assignment_lookup(gs_client)
    except Exception:
        log.exception("GGS Daily team lookup failed for Online Real Time - falling back to empty for this response")
        team_lookup = {}
    # Optional - only present if someone has imported the CI/Node mapping
    # file on the SLA Improvement tab; if not, every entry below just gets
    # no lat/lon (map simply shows nothing plottable) rather than erroring.
    mapping_lookup = sla_improvement.get_mapping_lookup()

    entries = []
    for r in scoped:
        priority = _classify_priority(r.get("TARGETFINISH"), now_dt)
        if priority is None:
            continue  # unparseable TARGETFINISH - can't classify or rank this one at all

        ticket_id = str(r.get("TICKETID", "")).strip()
        wl = work_log.get(ticket_id, {})
        mateline = mateline_lookup.get(ticket_id.upper()) or {
            "status_mateline": "(ไม่พบใน MatelineX)", "mateline_wo_status": "",
        }
        team_info = team_lookup.get(ticket_id.upper())  # None if not found - shown as N/A, ticket is NEVER dropped for this
        over_sla_day = r.get("Over_SLA_Day")
        try:
            over_sla_day = float(over_sla_day)
        except (TypeError, ValueError):
            over_sla_day = 0
        ci_name = str(r.get("CINAME", "")).strip()
        node = sla_improvement.match_ci_to_node(ci_name, mapping_lookup) if ci_name else None

        entries.append({
            "TICKETID": ticket_id,
            "priority": priority,
            "SUBJECT": r.get("SUBJECT", ""),
            "PROVINCE": str(r.get("PROVINCE", "")).strip() or "(ไม่ระบุ)",
            "DISTRICT": str(r.get("DISTRICT", "")).strip() or "(ไม่ระบุ)",
            "Region": r.get("Region", ""),
            "Aging_Flag_Group": str(r.get("Aging_Flag_Group", "")).strip() or UNSPECIFIED_AGING,
            "over_sla_day": over_sla_day,
            "TARGETFINISH": r.get("TARGETFINISH", ""),  # raw string - frontend parses + ticks this itself
            "remaining_hours": _remaining_hours(r.get("TARGETFINISH"), now_dt),  # snapshot at fetch time, for the initial sort/render only
            "team": team_info["team"] if team_info else "N/A",
            "CINAME": ci_name,
            "node_id": node["node_id"] if node else None,
            "latitude": node["latitude"] if node else None,
            "longitude": node["longitude"] if node else None,
            "status_mateline": mateline["status_mateline"],
            "mateline_wo_status": mateline["mateline_wo_status"],
            "group_problem": wl.get("group_problem") or UNSPECIFIED_GROUP_PROBLEM,
            "action_team": wl.get("action_team", ""),
            "detail": wl.get("detail", ""),
            "plan_closed_date": wl.get("plan_closed_date", ""),
        })

    entries.sort(key=lambda e: e["remaining_hours"] if e["remaining_hours"] is not None else float("inf"))

    total = len(entries)
    over = sum(1 for e in entries if e["remaining_hours"] is not None and e["remaining_hours"] < 0)
    near = sum(1 for e in entries if e["remaining_hours"] is not None and 0 <= e["remaining_hours"] <= 2)
    p0 = sum(1 for e in entries if e["priority"] == "P0")
    p1 = sum(1 for e in entries if e["priority"] == "P1")
    p2 = sum(1 for e in entries if e["priority"] == "P2")

    insert_time = scoped[0].get("insert_time") if scoped else None
    return {
        "entries": entries,
        "kpi": {"total": total, "over": over, "near": near, "p0": p0, "p1": p1, "p2": p2},
        "generated_at": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "insert_time": insert_time,
    }
