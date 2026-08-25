"""Smart Team Planning: recommends how to plan field-team assignments for
TOMORROW's P0 tickets, ahead of time (built for the night-time planning
window - by the time everyone's off for the day, tomorrow's P0 list is
already knowable and worth staging in advance).

Combines three things:
1. Live ticket data (same REALTIME_SHEET_ID sheet every other tab already
   reads) classified against a reference point one day further out than
   the normal "tomorrow" P0 calculation - i.e. what WILL be P0 by tomorrow
   night, not what already is P0 tonight.
2. Team/Skill assignment, matched from the external GGS "Raw Data OWS"
   sheet by TICKETID == Source Ticket ID or External TicketID (same
   matching approach as mateline_status.py, different sheet/tab and
   different fields pulled out).
3. Each ticket's own LATITUDE/LONGITUDE/PROVINCE/DISTRICT/SUBDISTRICT
   (already present on every ticket row in REALTIME_SHEET_ID - no separate
   site-coordinate master needed).
"""

import logging
import threading
import time
from math import radians, sin, cos, sqrt, atan2

log = logging.getLogger(__name__)

GGS_SHEET_ID = "1PsJnXf8X7rBbA6G96L0ojE_ioby4mKqYcJQLp7fFiOw"
GGS_RAW_TAB = "Raw Data OWS(not edit)"

# Normal capacity per skill - how many jobs a team can reasonably carry at
# once. Skills without an explicit number here fall back to DEFAULT_CAPACITY.
TEAM_CAPACITY = {"NODE": 3, "OFC": 2}
DEFAULT_CAPACITY = 3

SEVERITY_GROUPS = {
    "SA1-4": {"SA1", "SA2", "SA3", "SA4"},
    "NSA1-2": {"NSA1", "NSA2"},
    "NSA3-4": {"NSA3", "NSA4"},
}
SEVERITY_GROUP_ORDER = ["SA1-4", "NSA1-2", "NSA3-4"]

_ggs_raw_cache = {"data": None, "ts": 0}
_ggs_raw_lock = threading.Lock()
GGS_RAW_CACHE_TTL_SECONDS = 300  # 5 min - external field-ops sheet, doesn't need second-by-second freshness


def fetch_ggs_raw_rows(gs_client, use_cache=True):
    now = time.monotonic()
    if use_cache:
        with _ggs_raw_lock:
            if _ggs_raw_cache["data"] is not None and (now - _ggs_raw_cache["ts"]) < GGS_RAW_CACHE_TTL_SECONDS:
                return _ggs_raw_cache["data"]
    sh = gs_client.open_by_key(GGS_SHEET_ID)
    ws = sh.worksheet(GGS_RAW_TAB)
    rows = ws.get_all_values()
    if use_cache:
        with _ggs_raw_lock:
            _ggs_raw_cache["data"] = rows
            _ggs_raw_cache["ts"] = now
    return rows


def build_team_assignment_lookup(gs_client):
    """Returns {ticket_id_upper: {"team", "skill"}}, keyed by both Source
    Ticket ID and External TicketID (whichever a NOC TICKETID matches)."""
    rows = fetch_ggs_raw_rows(gs_client)
    if not rows:
        return {}
    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def get(row, name):
        i = col.get(name)
        if i is None or i >= len(row):
            return ""
        return row[i]

    lookup = {}
    for row in rows[1:]:
        if not row or not any(row):
            continue
        team = get(row, "Team").strip()
        if not team:
            continue
        skill = get(row, "Skill").strip()
        entry = {"team": team, "skill": skill}
        for tid in (get(row, "Source Ticket ID").strip(), get(row, "External TicketID").strip()):
            if tid:
                lookup[tid.upper()] = entry
    return lookup


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _to_float(v):
    try:
        if v in (None, "", "None"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def build_team_plan(gs_client, severity_group="SA1-4"):
    from pending_ticket import fetch_live_rows, PENDING_TICKET_REGIONS, _classify_priority_at
    from pending_trend import bangkok_now
    from datetime import timedelta

    now_dt = bangkok_now()
    # One step further than the usual "tomorrow 01:15" P0 reference - what
    # WILL be P0 by tomorrow night, so tonight's planning covers tomorrow's
    # whole day rather than reacting to it as it happens.
    reference_dt = (now_dt + timedelta(days=2)).replace(hour=1, minute=15, second=0, microsecond=0)

    allowed_sevs = SEVERITY_GROUPS.get(severity_group, SEVERITY_GROUPS["SA1-4"])
    all_rows = fetch_live_rows(gs_client)

    try:
        team_lookup = build_team_assignment_lookup(gs_client)
    except Exception:
        log.exception("GGS Raw Data OWS team lookup failed - continuing with no team assignments")
        team_lookup = {}

    tickets = []
    for r in all_rows:
        region = str(r.get("Region", "")).strip()
        if region not in PENDING_TICKET_REGIONS:
            continue
        sev = str(r.get("SEVERITY", "")).strip()
        if sev not in allowed_sevs:
            continue
        priority = _classify_priority_at(r.get("TARGETFINISH"), reference_dt)
        if priority != "P0":
            continue

        ticket_id = str(r.get("TICKETID", "")).strip()
        assignment = team_lookup.get(ticket_id.upper())
        tickets.append({
            "ticket_id": ticket_id,
            "subject": r.get("SUBJECT", ""),
            "severity": sev,
            "region": region,
            "province": str(r.get("PROVINCE", "")).strip() or "(ไม่ระบุจังหวัด)",
            "district": str(r.get("DISTRICT", "")).strip(),
            "subdistrict": str(r.get("SUBDISTRICT", "")).strip(),
            "lat": _to_float(r.get("LATITUDE")),
            "lon": _to_float(r.get("LONGITUDE")),
            "team": assignment["team"] if assignment else None,
            "skill": assignment["skill"] if assignment else None,
            "target_finish": r.get("TARGETFINISH", ""),
        })

    # Region -> Province breakdown
    region_province = {}
    for t in tickets:
        region_province.setdefault(t["region"], {}).setdefault(t["province"], 0)
        region_province[t["region"]][t["province"]] += 1
    region_summary = []
    for region in sorted(region_province):
        provinces = [
            {"province": p, "count": c}
            for p, c in sorted(region_province[region].items(), key=lambda kv: -kv[1])
        ]
        region_summary.append({
            "region": region, "total": sum(p["count"] for p in provinces), "provinces": provinces,
        })

    # Team load vs capacity
    team_tickets = {}
    for t in tickets:
        if not t["team"]:
            continue
        team_tickets.setdefault(t["team"], {"skill": t["skill"], "tickets": []})
        team_tickets[t["team"]]["tickets"].append(t)

    teams = []
    for team_name, info in team_tickets.items():
        skill = info["skill"]
        capacity = TEAM_CAPACITY.get(skill, DEFAULT_CAPACITY)
        load = len(info["tickets"])
        teams.append({
            "team": team_name, "skill": skill, "load": load, "capacity": capacity,
            "over_capacity": max(0, load - capacity),
            "remaining_capacity": max(0, capacity - load),
            "status": "overloaded" if load > capacity else ("full" if load == capacity else "available"),
        })
    teams.sort(key=lambda tm: (-tm["over_capacity"], -tm["load"]))

    unassigned = [t for t in tickets if not t["team"]]

    # Reassignment recommendations: for each overloaded team, take its
    # excess tickets (the ones farthest from that team's own centroid -
    # i.e. the least "on the way" for them) and match each to whichever
    # same-skill team with spare capacity already has a ticket nearest to
    # it (a team already heading that direction is the natural pickup).
    recommendations = []
    for team in teams:
        if team["over_capacity"] <= 0:
            continue
        team_ticket_list = team_tickets[team["team"]]["tickets"]
        located = [t for t in team_ticket_list if t["lat"] is not None and t["lon"] is not None]
        if not located:
            continue
        centroid_lat = sum(t["lat"] for t in located) / len(located)
        centroid_lon = sum(t["lon"] for t in located) / len(located)
        located.sort(key=lambda t: -_haversine_km(centroid_lat, centroid_lon, t["lat"], t["lon"]))
        excess_tickets = located[:team["over_capacity"]]

        candidate_teams = [
            tm for tm in teams
            if tm["team"] != team["team"] and tm["skill"] == team["skill"] and tm["remaining_capacity"] > 0
        ]
        for ex_t in excess_tickets:
            best = None
            for cand in candidate_teams:
                cand_tickets = [
                    ct for ct in team_tickets[cand["team"]]["tickets"]
                    if ct["lat"] is not None and ct["lon"] is not None
                ]
                if not cand_tickets:
                    continue
                nearest_dist = min(_haversine_km(ex_t["lat"], ex_t["lon"], ct["lat"], ct["lon"]) for ct in cand_tickets)
                if best is None or nearest_dist < best["distance_km"]:
                    best = {"team": cand["team"], "distance_km": round(nearest_dist, 1)}
            if best:
                recommendations.append({
                    "ticket_id": ex_t["ticket_id"], "subject": ex_t["subject"],
                    "province": ex_t["province"], "district": ex_t["district"],
                    "from_team": team["team"], "to_team": best["team"],
                    "distance_km": best["distance_km"], "skill": team["skill"],
                })
    recommendations.sort(key=lambda r: r["distance_km"])

    return {
        "reference_time": reference_dt.strftime("%Y-%m-%d %H:%M"),
        "severity_group": severity_group,
        "total_tickets": len(tickets),
        "region_summary": region_summary,
        "teams": teams,
        "unassigned_count": len(unassigned),
        "unassigned": unassigned,
        "recommendations": recommendations,
        "tickets": tickets,
    }
