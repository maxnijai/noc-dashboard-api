"""Smart Team Planning: recommends how to plan field-team assignments for
P0 tickets, ahead of time - "today" mode for morning planning (matches the
same P0 definition every other tab in this app uses), "tomorrow" mode for
night-time planning (one reference point further out, showing what WILL
be P0 by tomorrow night before it happens).

Combines three things:
1. Live ticket data (same REALTIME_SHEET_ID sheet every other tab already
   reads), classified against whichever reference point the chosen
   planning mode uses.
2. Team/Skill assignment, matched from the external GGS "Daily" sheet by
   TICKETID == Source Ticket ID or External TicketID - reuses
   mateline_status.fetch_ggs_daily_rows() directly (same sheet+tab that
   module already reads/caches for the Status Mateline feature, so this
   doesn't add a second read of the same data). Also used to total up
   each team's OVERALL current workload (every row assigned to them,
   regardless of severity/group), for context next to the P0 count.
3. Each ticket's own LATITUDE/LONGITUDE/PROVINCE/DISTRICT/SUBDISTRICT
   (already present on every ticket row in REALTIME_SHEET_ID - no separate
   site-coordinate master needed).
"""

import logging
from collections import Counter
from math import radians, sin, cos, sqrt, atan2

log = logging.getLogger(__name__)

# Normal capacity per skill - how many jobs a team can reasonably carry at
# once. Skills without an explicit number here fall back to DEFAULT_CAPACITY.
TEAM_CAPACITY = {"NODE": 3, "OFC": 2}
DEFAULT_CAPACITY = 3

# Same 4 groups Focus Priority 0 Ticket already uses everywhere else
# (pending_ticket.BOOKMARK_SORT_ORDER + the NSA3-4 catch-all), so this
# tab's grouping matches what the team already reads elsewhere on the page.
BOOKMARK_GROUP_ORDER = ["7.MB with SA1-4", "4.FBB with SA1-4", "3. All NW Incident NSA1-2", "NSA3-4"]

# "today": the SAME reference point (tomorrow 01:15) every other P0 view in
# this app uses - for morning planning, today's already-known P0 list.
# "tomorrow": one step further out (the day after tomorrow's 01:15) - for
# night-time planning, what WILL be P0 by tomorrow night before it happens.
PLANNING_MODE_DAYS_AHEAD = {"today": 1, "tomorrow": 2}


def build_team_assignment_lookup(gs_client):
    """Returns {ticket_id_upper: {"team", "skill"}}, keyed by both Source
    Ticket ID (col D) and External TicketID (col E) - whichever a NOC
    TICKETID matches. Team is col I, Skill is col H."""
    from mateline_status import fetch_ggs_daily_rows

    rows = fetch_ggs_daily_rows(gs_client)
    if not rows:
        return {}, {}
    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def get(row, name):
        i = col.get(name)
        if i is None or i >= len(row):
            return ""
        return row[i]

    lookup = {}
    workload_totals = Counter()
    for row in rows[1:]:
        if not row or not any(row):
            continue
        team = get(row, "Team").strip()
        if not team:
            continue
        workload_totals[team] += 1
        skill = get(row, "Skill").strip()
        entry = {"team": team, "skill": skill}
        for tid in (get(row, "Source Ticket ID").strip(), get(row, "External TicketID").strip()):
            if tid:
                lookup[tid.upper()] = entry
    return lookup, dict(workload_totals)


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



def build_team_plan(gs_client, bookmark_groups=None, planning_mode="tomorrow"):
    from pending_ticket import fetch_live_rows, PENDING_TICKET_REGIONS, _classify_priority_at, _exclusive_bookmark_label
    from pending_trend import bangkok_now
    from datetime import timedelta

    now_dt = bangkok_now()
    days_ahead = PLANNING_MODE_DAYS_AHEAD.get(planning_mode, 2)
    reference_dt = (now_dt + timedelta(days=days_ahead)).replace(hour=1, minute=15, second=0, microsecond=0)

    # None/empty = every group selected (the new default) - a real list
    # restricts the working ticket set (map/detail/region summary/load/
    # recommendations) to just those groups, but the per-team breakdown
    # by group (for the Capacity table's extra columns) always covers all
    # 4 regardless, so that context is visible even while focused on one.
    selected_groups = set(bookmark_groups) & set(BOOKMARK_GROUP_ORDER) if bookmark_groups else set(BOOKMARK_GROUP_ORDER)

    all_rows = fetch_live_rows(gs_client)

    try:
        team_lookup, team_workload_totals = build_team_assignment_lookup(gs_client)
    except Exception:
        log.exception("GGS Daily team lookup failed - continuing with no team assignments")
        team_lookup, team_workload_totals = {}, {}

    tickets_all_groups = []
    for r in all_rows:
        region = str(r.get("Region", "")).strip()
        if region not in PENDING_TICKET_REGIONS:
            continue
        group = _exclusive_bookmark_label(r.get("Bookmark"))
        if group not in BOOKMARK_GROUP_ORDER:
            continue
        priority = _classify_priority_at(r.get("TARGETFINISH"), reference_dt)
        if priority != "P0":
            continue

        ticket_id = str(r.get("TICKETID", "")).strip()
        assignment = team_lookup.get(ticket_id.upper())
        tickets_all_groups.append({
            "ticket_id": ticket_id,
            "subject": r.get("SUBJECT", ""),
            "severity": str(r.get("SEVERITY", "")).strip(),
            "bookmark_group": group,
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

    # Per-team, per-group P0 breakdown (Capacity table's extra columns) -
    # always computed across every group, independent of what's selected.
    team_group_breakdown = {}
    for t in tickets_all_groups:
        if not t["team"]:
            continue
        team_group_breakdown.setdefault(t["team"], {g: 0 for g in BOOKMARK_GROUP_ORDER})
        team_group_breakdown[t["team"]][t["bookmark_group"]] += 1

    # From here on, only the SELECTED groups' tickets are in play - this is
    # what drives region summary, the map, the detail table, team load vs
    # capacity, and recommendations.
    tickets = [t for t in tickets_all_groups if t["bookmark_group"] in selected_groups]

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

    # Team load vs capacity - plus region/province context per team (drawn
    # from this group's own tickets: the most common region/province among
    # them, with a "+N" note if the team's tickets span more than one).
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
        region_counts = Counter(t["region"] for t in info["tickets"])
        province_counts = Counter(t["province"] for t in info["tickets"])
        teams.append({
            "team": team_name, "skill": skill, "load": load, "capacity": capacity,
            "over_capacity": max(0, load - capacity),
            "remaining_capacity": max(0, capacity - load),
            "status": "overloaded" if load > capacity else ("full" if load == capacity else "available"),
            "region": region_counts.most_common(1)[0][0] if region_counts else "",
            "region_count": len(region_counts),
            "province": province_counts.most_common(1)[0][0] if province_counts else "",
            "province_count": len(province_counts),
            "total_workload": team_workload_totals.get(team_name, load),
            "group_breakdown": team_group_breakdown.get(team_name, {g: 0 for g in BOOKMARK_GROUP_ORDER}),
        })
    teams.sort(key=lambda tm: (-tm["over_capacity"], -tm["load"]))

    unassigned = [t for t in tickets if not t["team"]]

    # Reassignment recommendations: for each overloaded team, take its
    # excess tickets (the ones farthest from that team's own centroid -
    # i.e. the least "on the way" for them) and match each to whichever
    # same-skill team with spare capacity already has a ticket nearest to
    # it (a team already heading that direction is the natural pickup).
    # from_lat/from_lon/to_lat/to_lon are included so the frontend can draw
    # the suggested move directly on the map, not just list it as text.
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
                nearest = min(cand_tickets, key=lambda ct: _haversine_km(ex_t["lat"], ex_t["lon"], ct["lat"], ct["lon"]))
                nearest_dist = _haversine_km(ex_t["lat"], ex_t["lon"], nearest["lat"], nearest["lon"])
                if best is None or nearest_dist < best["distance_km"]:
                    best = {
                        "team": cand["team"], "distance_km": round(nearest_dist, 1),
                        "near_ticket_id": nearest["ticket_id"], "to_lat": nearest["lat"], "to_lon": nearest["lon"],
                        "to_district": nearest["district"], "to_province": nearest["province"],
                    }
            if best:
                recommendations.append({
                    "ticket_id": ex_t["ticket_id"], "subject": ex_t["subject"],
                    "province": ex_t["province"], "district": ex_t["district"],
                    "to_province": best["to_province"], "to_district": best["to_district"],
                    "from_team": team["team"], "to_team": best["team"],
                    "distance_km": best["distance_km"],
                    "near_ticket_id": best["near_ticket_id"],
                    "skill": team["skill"],
                    "from_lat": ex_t["lat"], "from_lon": ex_t["lon"],
                    "to_lat": best["to_lat"], "to_lon": best["to_lon"],
                    "reason": (
                        f"{team['team']} มีงานเกิน Capacity ({team['load']}/{team['capacity']}) - "
                        f"งานนี้ ({ex_t['district']}) อยู่ไกลจากงานอื่นๆ ของทีมตัวเองที่สุด ในขณะที่ {best['team']} "
                        f"มีงาน {best['near_ticket_id']} ({best['to_district']}) อยู่ห่างแค่ {round(best['distance_km'], 1)} กม."
                    ),
                })
    recommendations.sort(key=lambda r: r["distance_km"], reverse=True)

    return {
        "reference_time": reference_dt.strftime("%Y-%m-%d %H:%M"),
        "planning_mode": planning_mode,
        "selected_groups": sorted(selected_groups, key=BOOKMARK_GROUP_ORDER.index),
        "bookmark_groups": BOOKMARK_GROUP_ORDER,
        "total_tickets": len(tickets),
        "region_summary": region_summary,
        "teams": teams,
        "unassigned_count": len(unassigned),
        "unassigned": unassigned,
        "recommendations": recommendations,
        "tickets": tickets,
    }
