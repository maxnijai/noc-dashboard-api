"""
realtime_monitor.py
--------------------
NOR2026 / BBTEC NOC — "Realtime Monitoring" tab.

Unlike pending_trend.py (which works off nightly/hourly xlsx snapshots backed
up to Drive), this reads LIVE straight from the raw ticket Google Sheet on
every request - no caching layer, no scheduled job. That sheet
("Pending ticket raw data (SCCD+ITSM)") is updated by an external process that
stamps every row in a batch with the same `insert_time` value, so the
frontend can cheaply poll just that one value and only re-fetch everything
else when it changes.

Bookmark / severity views (mirrors the filter groups from pending_trend.py's
SEVERITY_GROUPS, but keyed differently since these are raw ticket-level rows,
not pre-aggregated buckets):
  FBB     -> Bookmark == "4.FBB with SA1-4"
  NW_NSA12-> Bookmark == "3. All NW Incident NSA1-2"
  MB      -> Bookmark == "7.MB with SA1-4"
  NSA34   -> SEVERITY in {"NSA3", "NSA4"}  (no bookmark filter for this one)

Priority (P0/P1/P2), per MAX's existing Telegram-bot convention:
  diff_hours = now - TARGETFINISH
  diff_hours > 24   -> P0 (red)    - over SLA by more than a day
  0 < diff_hours <=24-> P1 (amber) - over SLA, under a day
  diff_hours <= 0    -> P2 (green) - not yet due
"""

import logging
from datetime import datetime, timedelta

from pending_trend import (
    get_drive_and_sheets_clients,
    bangkok_now,
    ALLOWED_REGIONS,
    AGING_ORDER,
    AGING_COLORS,
)
from ticket_views import BOOKMARK_VIEWS, row_matches_view as _row_matches_view

log = logging.getLogger(__name__)

REALTIME_SHEET_ID = "1AEQSsiLUbr5p6HYh36WNGF9TkUDVeW2xN-vDvDkjy1k"
REALTIME_WORKSHEET_GID = 0  # "gid=0" tab from the shared URL

DETAIL_COLUMNS = [
    "Over_SLA_Day", "TICKETID", "AGING", "CREATIONDATE", "TARGETFINISH",
    "Aging_Flag_Group", "SEVERITY", "SUBJECT", "CINAME", "CATEGORIES",
    "Region", "TRUEOWNERGROUP", "DISTRICT", "Subimpact",
]

PRIORITY_COLORS = {"P0": "#E24B4A", "P1": "#EF9F27", "P2": "#639922"}


def _get_worksheet(gs_client):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    for ws in sh.worksheets():
        if ws.id == REALTIME_WORKSHEET_GID:
            return ws
    return sh.sheet1


def get_insert_time(gs_client=None):
    """Cheap poll target: just the insert_time value from the first data row,
    without pulling the rest of the sheet. Returns None if the sheet is empty."""
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()
    ws = _get_worksheet(gs_client)
    val = ws.cell(2, 1).value  # row 1 = header, row 2 = first data row, col A = insert_time
    return val


def _parse_dt(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def _classify_priority(target_finish_str, now_dt):
    """Priority reference point is tomorrow's 01:15:00 (Bangkok time) - NOT the
    literal current moment - to match the same convention used elsewhere
    (Telegram bot, daily snapshot jobs). diff_hours = reference - TARGETFINISH:
      > 24h  -> P0 (over SLA by more than a day)
      0-24h  -> P1 (over SLA, under a day)
      <= 0h  -> P2 (not yet due)"""
    tf = _parse_dt(target_finish_str)
    if tf is None:
        return None
    reference_dt = (now_dt + timedelta(days=1)).replace(hour=1, minute=15, second=0, microsecond=0)
    diff_hours = (reference_dt - tf).total_seconds() / 3600
    if diff_hours > 24:
        return "P0"
    elif diff_hours > 0:
        return "P1"
    else:
        return "P2"


def fetch_rows(gs_client=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()
    ws = _get_worksheet(gs_client)
    return ws.get_all_records()


def _row_matches_filters(row, region_filter, trueowner_filter, aging_filter, district_filter):
    if region_filter and str(row.get("Region", "")).strip() not in region_filter:
        return False
    if trueowner_filter and str(row.get("TRUEOWNERGROUP", "")).strip() != trueowner_filter:
        return False
    if aging_filter and str(row.get("Aging_Flag_Group", "")).strip() not in aging_filter:
        return False
    if district_filter and str(row.get("DISTRICT", "")).strip() != district_filter:
        return False
    return True


def _top_n_breakdown(rows, dim_key, n=25):
    """Returns {category: {aging_key: count, ...}} sorted by total desc, top n."""
    totals = {}
    breakdown = {}
    for r in rows:
        cat = str(r.get(dim_key, "")).strip() or "UNKNOWN"
        aging = str(r.get("Aging_Flag_Group", "")).strip()
        if aging not in AGING_ORDER:
            continue
        breakdown.setdefault(cat, {k: 0 for k in AGING_ORDER})
        breakdown[cat][aging] += 1
        totals[cat] = totals.get(cat, 0) + 1

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return [{"category": cat, "total": total, "aging_counts": breakdown[cat]} for cat, total in ranked]


def build_realtime_response(gs_client=None, view_key="FBB", region_filter=None,
                             trueowner_filter=None, aging_filter=None, district_filter=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()
    if view_key not in BOOKMARK_VIEWS:
        raise ValueError(f"Unknown view_key {view_key!r}")

    all_rows = fetch_rows(gs_client)
    now_dt = bangkok_now()

    matched = [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in ALLOWED_REGIONS
        and _row_matches_view(r, view_key)
        and _row_matches_filters(r, region_filter, trueowner_filter, aging_filter, district_filter)
    ]

    # Filter option lists (built from the view's full match set, before the
    # optional region/trueowner/aging/district filters narrow it further,
    # so dropdowns don't shrink themselves out as the user filters).
    view_only = [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in ALLOWED_REGIONS and _row_matches_view(r, view_key)
    ]
    filter_options = {
        "regions": sorted({str(r.get("Region", "")).strip() for r in view_only if r.get("Region")}),
        "trueowners": sorted({str(r.get("TRUEOWNERGROUP", "")).strip() for r in view_only if r.get("TRUEOWNERGROUP")}),
        "districts": sorted({str(r.get("DISTRICT", "")).strip() for r in view_only if r.get("DISTRICT")}),
    }

    map_points = []
    detail_rows = []
    for r in matched:
        priority = _classify_priority(r.get("TARGETFINISH"), now_dt)
        lat, lon = r.get("LATITUDE"), r.get("LONGITUDE")
        try:
            lat_f, lon_f = float(lat), float(lon)
            if lat_f and lon_f:
                map_points.append({
                    "lat": lat_f, "lon": lon_f, "priority": priority,
                    "ticketid": r.get("TICKETID"), "subject": r.get("SUBJECT"),
                    "trueowner": r.get("TRUEOWNERGROUP"), "district": r.get("DISTRICT"),
                })
        except (TypeError, ValueError):
            pass

        detail_rows.append({**{c: r.get(c) for c in DETAIL_COLUMNS}, "priority": priority})

    # Most urgent first: P0 > P1 > P2 > unknown, then longest-overdue within each tier.
    priority_rank = {"P0": 0, "P1": 1, "P2": 2}
    detail_rows.sort(key=lambda d: (
        priority_rank.get(d.get("priority"), 3),
        -(d.get("Over_SLA_Day") if isinstance(d.get("Over_SLA_Day"), (int, float)) else 0),
    ))
    DETAIL_ROW_LIMIT = 500
    truncated = len(detail_rows) > DETAIL_ROW_LIMIT
    detail_rows = detail_rows[:DETAIL_ROW_LIMIT]

    response = {
        "view": view_key,
        "view_label": BOOKMARK_VIEWS[view_key]["label"],
        "insert_time": all_rows[0].get("insert_time") if all_rows else None,
        "total": len(matched),
        "filter_options": filter_options,
        "aging_order": AGING_ORDER,
        "aging_colors": AGING_COLORS,
        "priority_colors": PRIORITY_COLORS,
        "breakdown_region": _top_n_breakdown(matched, "Region"),
        "breakdown_trueowner": _top_n_breakdown(matched, "TRUEOWNERGROUP"),
        "breakdown_district": _top_n_breakdown(matched, "DISTRICT"),
        "map_points": map_points,
        "detail_rows": detail_rows,
        "detail_truncated": truncated,
    }
    return response
