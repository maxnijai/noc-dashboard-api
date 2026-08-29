"""Flood NAN Monitoring: region-wide (NOR1/NOR2, every province) live ticket
map + summary tables. Historically Nan-only and driven by a separate site
master sheet; now driven entirely by the live ticket data itself
(REALTIME_SHEET_ID) - every "site" plotted is simply a distinct CINAME that
currently has at least one open ticket, using that ticket's own
LATITUDE/LONGITUDE/PROVINCE/DISTRICT/SUBDISTRICT fields directly. No site
master lookup is needed anymore, so nothing here depends on a fixed site
list existing or being kept up to date.

"DN" (important/generator-equipped site) status is likewise now derived
per-ticket instead of from a hand-maintained site list: a site counts as DN
if any of its tickets has CLASSIFICATION containing "SMALL EXCHANGE" or
CATEGORIES containing "Exchange Node".

Manual remark pins and DN site remarks are unrelated to any of the above -
both live in their own tabs inside REALTIME_SHEET_ID and are unaffected by
this module's scope change.
"""

import logging
import threading
import time
import uuid
from datetime import timedelta, datetime

from pending_ticket import ALLOWED_SEVERITIES, PENDING_TICKET_REGIONS, _exclusive_bookmark_label, fetch_live_rows
from pending_trend import get_drive_and_sheets_clients, bangkok_now, find_closest_file, download_xlsx_as_rows
from realtime_monitor import REALTIME_SHEET_ID

log = logging.getLogger(__name__)

# Severity -> marker color, per the requested classification.
SEVERITY_COLOR = {
    "SA1": "#E24B4A", "SA2": "#E24B4A", "SA3": "#E24B4A", "SA4": "#E24B4A",
    "NSA1": "#EF9F27", "NSA2": "#EF9F27",
    "NSA3": "#F5C518", "NSA4": "#F5C518",
}
# Worse-first order, used to pick ONE color when a site has multiple open
# tickets with different severities.
SEVERITY_RANK = {"SA1": 0, "SA2": 0, "SA3": 0, "SA4": 0, "NSA1": 1, "NSA2": 1, "NSA3": 2, "NSA4": 2}

# The 4 Bookmark buckets used throughout this module - same labels
# Exclusive Pending / P0 Only use (see pending_ticket.py's
# EXCLUSIVE_BOOKMARK_ORDER), just relabeled here per the request wording.
CLASSIFICATION_BOOKMARKS = [
    ("SA Mobile", "7.MB with SA1-4"),
    ("Online", "4.FBB with SA1-4"),
    ("NSA1-2", "3. All NW Incident NSA1-2"),
    ("NSA3-4", "NSA3-4"),
]
BOOKMARK_LABEL_LOOKUP = {raw: label for label, raw in CLASSIFICATION_BOOKMARKS}

# Extended 6-column breakdown used by Classification Summary, District, and
# Unique Sites tables specifically - same 4 buckets, but "SA Mobile" is
# split into SA1-2 / SA3 / SA4 for closer tracking (SA3/SA4 being the more
# severe tier within Mobile). The Province x Bookmark heat matrix keeps the
# plain 4-column CLASSIFICATION_BOOKMARKS set - this split wasn't requested
# there.
EXTENDED_BOOKMARK_LABELS = ["SA Mobile (SA1-2)", "SA Mobile (SA3)", "SA Mobile (SA4)", "Online", "NSA1-2", "NSA3-4"]


def _extended_bookmark_label(bookmark_label, severity):
    if bookmark_label != "SA Mobile":
        return bookmark_label
    if severity == "SA3":
        return "SA Mobile (SA3)"
    if severity == "SA4":
        return "SA Mobile (SA4)"
    return "SA Mobile (SA1-2)"

MANUAL_MARKERS_TAB = "FloodNanMarkers"
MANUAL_MARKERS_HEADER = ["id", "lat", "lon", "remark", "created_by", "created_at"]
SITE_REMARKS_TAB = "FloodNanSiteRemarks"
SITE_REMARKS_HEADER = ["location_id", "remark", "updated_by", "updated_at"]

# NOD/OFC workload calculator: real team counts per province (from the
# reference headcount table provided) - ticket counts get DIVIDED by these
# to give workload-per-team, not raw ticket totals. Keyed by full Thai
# province name to match PROVINCE straight off the ticket data.
PROVINCE_TEAM_COUNTS = {
    "เชียงใหม่": {"NOD": 9, "OFC": 34},
    "เชียงราย": {"NOD": 5, "OFC": 12},
    "กำแพงเพชร": {"NOD": 2, "OFC": 3},
    "ลำปาง": {"NOD": 3, "OFC": 7},
    "ลำพูน": {"NOD": 1, "OFC": 3},
    "แม่ฮ่องสอน": {"NOD": 3, "OFC": 10},
    "น่าน": {"NOD": 3, "OFC": 9},
    "เพชรบูรณ์": {"NOD": 3, "OFC": 6},
    "พิจิตร": {"NOD": 2, "OFC": 5},
    "แพร่": {"NOD": 2, "OFC": 3},
    "พิษณุโลก": {"NOD": 4, "OFC": 5},
    "พะเยา": {"NOD": 2, "OFC": 3},
    "สุโขทัย": {"NOD": 2, "OFC": 3},
    "ตาก": {"NOD": 3, "OFC": 6},
    "อุตรดิตถ์": {"NOD": 2, "OFC": 3},
}

# Only these 3 Bookmark groups count toward workload - NSA3-4 is excluded.
WORKLOAD_BOOKMARKS = {"SA Mobile", "Online", "NSA1-2"}
WORKLOAD_NOD_SEVERITIES = {"SA4"}
WORKLOAD_OFC_SEVERITIES = {"SA1", "SA2", "SA3", "NSA1", "NSA2"}


def _to_float(v):
    try:
        f = float(str(v).strip())
        return f if f else None
    except (TypeError, ValueError):
        return None


def _last_classification_segment(raw):
    """CLASSIFICATION values are backslash-delimited hierarchies, e.g.
    'NOC-NW-BROADBAND \\ FTTH ODN-SPLITTER L2 \\ SERVICE QUALITY DEGRADE' -
    the last segment is the specific, meaningful part; the earlier segments
    are just broad category scaffolding that repeats across many tickets."""
    raw = str(raw or "").strip()
    if not raw:
        return "(ไม่ระบุ)"
    parts = [p.strip() for p in raw.replace("\\\\", "\\").split("\\") if p.strip()]
    return parts[-1] if parts else raw


def _is_dn_ticket(classification, categories):
    c = str(classification or "").upper()
    cat = str(categories or "").upper()
    return "SMALL EXCHANGE" in c or "EXCHANGE NODE" in cat


def _ticket_province(r):
    return str(r.get("PROVINCE", "")).strip() or "(ไม่ระบุจังหวัด)"


def _scoped_live_rows(gs_client):
    """Every live ticket row in NOR1/NOR2 with a real severity - the base
    dataset this whole module (map, tables, and trends alike) is built
    from. No site-master lookup or matching needed anymore: PROVINCE,
    DISTRICT, SUBDISTRICT, LATITUDE, LONGITUDE all come straight off each
    ticket row."""
    all_rows = fetch_live_rows(gs_client)
    return [
        r for r in all_rows
        if str(r.get("Region", "")).strip() in PENDING_TICKET_REGIONS
        and str(r.get("SEVERITY", "")).strip() in ALLOWED_SEVERITIES
    ]


def build_flood_nan_response(gs_client=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()

    scoped = _scoped_live_rows(gs_client)
    site_remarks = get_site_remarks(gs_client)

    tickets = []
    for r in scoped:
        bm = _exclusive_bookmark_label(r.get("Bookmark"))
        classification = r.get("CLASSIFICATION", "")
        categories = r.get("CATEGORIES", "")
        over_sla_day = r.get("Over_SLA_Day")
        try:
            over_sla_day = float(over_sla_day)
        except (TypeError, ValueError):
            over_sla_day = 0
        tickets.append({
            "TICKETID": r.get("TICKETID", ""), "SEVERITY": str(r.get("SEVERITY", "")).strip(),
            "CREATIONDATE": r.get("CREATIONDATE", ""), "TARGETFINISH": r.get("TARGETFINISH", ""),
            "CINAME": str(r.get("CINAME", "")).strip(),
            "Region": str(r.get("Region", "")).strip(),
            "PROVINCE": _ticket_province(r), "DISTRICT": str(r.get("DISTRICT", "")).strip(),
            "SUBDISTRICT": str(r.get("SUBDISTRICT", "")).strip(), "SUBJECT": r.get("SUBJECT", ""),
            "Bookmark": bm, "CLASSIFICATION": classification, "CATEGORIES": categories,
            "Aging_Flag_Group": str(r.get("Aging_Flag_Group", "")).strip(), "over_sla_day": over_sla_day,
            "insert_time": r.get("insert_time", ""),
            "is_dn": _is_dn_ticket(classification, categories),
            "lat": _to_float(r.get("LATITUDE")), "lon": _to_float(r.get("LONGITUDE")),
        })

    # Group into map sites by CINAME, split into 3 independent groups by
    # bookmark scope (for the 3 separate maps) - a site can appear on more
    # than one map if it has tickets in more than one scope. Each group is
    # its own CINAME grouping (not a shared one filtered 3 ways) so a site
    # with tickets in 2 scopes gets its OWN color/ticket-list per map.
    def _build_site_group(ticket_subset, color_fn):
        by_ciname = {}
        for t in ticket_subset:
            ciname = t["CINAME"].upper()
            if not ciname:
                continue
            entry = by_ciname.setdefault(ciname, {
                "location_id": t["CINAME"], "province": t["PROVINCE"], "district_e": t["DISTRICT"],
                "subdistrict_e": t["SUBDISTRICT"], "lat": t["lat"], "lon": t["lon"],
                "is_dn": False, "tickets": [],
            })
            if entry["lat"] is None and t["lat"] is not None:
                entry["lat"], entry["lon"] = t["lat"], t["lon"]
            if t["is_dn"]:
                entry["is_dn"] = True
            entry["tickets"].append({
                "TICKETID": t["TICKETID"], "SEVERITY": t["SEVERITY"], "CREATIONDATE": t["CREATIONDATE"],
                "CINAME": t["CINAME"], "PROVINCE": t["PROVINCE"], "DISTRICT": t["DISTRICT"],
                "SUBDISTRICT": t["SUBDISTRICT"], "SUBJECT": t["SUBJECT"], "Bookmark": t["Bookmark"],
                "CLASSIFICATION": t["CLASSIFICATION"],
            })
        out = []
        for ciname, s in by_ciname.items():
            if s["lat"] is None or s["lon"] is None:
                continue
            s["color"] = color_fn(s["tickets"])
            s["remark"] = site_remarks.get(ciname)
            out.append(s)
        return out

    def _mobile_online_color(site_tickets):
        # Mobile (red) takes priority if a site has both, per the requested
        # ordering ("MAP SA1-4 Mobile สีแดง Online สีฟ้า").
        has_mobile = any(tk["Bookmark"] == "7.MB with SA1-4" for tk in site_tickets)
        return "#E24B4A" if has_mobile else "#1f6feb"

    tickets_mobile_online = [t for t in tickets if t["Bookmark"] in ("7.MB with SA1-4", "4.FBB with SA1-4")]
    tickets_nsa12 = [t for t in tickets if t["Bookmark"] == "3. All NW Incident NSA1-2"]
    tickets_nsa34 = [t for t in tickets if t["Bookmark"] == "NSA3-4"]

    sites_mobile_online = _build_site_group(tickets_mobile_online, _mobile_online_color)
    sites_nsa12 = _build_site_group(tickets_nsa12, lambda _tk: "#EF9F27")
    sites_nsa34 = _build_site_group(tickets_nsa34, lambda _tk: "#F5C518")

    # A combined "all sites" list is still needed for DN table / infographic
    # / unique-site counts, which look across every bookmark at once.
    site_markers = []
    seen_ciname = set()
    for group in (sites_mobile_online, sites_nsa12, sites_nsa34):
        for s in group:
            key = s["location_id"].upper()
            if key in seen_ciname:
                continue
            seen_ciname.add(key)
            site_markers.append(s)
    # Give the combined list a severity-based color too (used by DN table
    # icon logic / infographic, which key off worst severity, not bookmark).
    site_ticket_lookup = {}
    for t in tickets:
        ciname = t["CINAME"].upper()
        if ciname:
            site_ticket_lookup.setdefault(ciname, []).append(t)
    for s in site_markers:
        all_tks = site_ticket_lookup.get(s["location_id"].upper(), [])
        if all_tks:
            best = min(all_tks, key=lambda tk: SEVERITY_RANK.get(tk["SEVERITY"], 9))
            s["color"] = SEVERITY_COLOR.get(best["SEVERITY"])

    # Province x Bookmark matrix, ticket counts (drives both the table and
    # its heatmap shading on the frontend). Also tracks each province's
    # Region (NOR1/NOR2, taken from its tickets) for the region-grouped
    # rendering with subtotals.
    provinces = sorted({t["PROVINCE"] for t in tickets})
    province_region = {}
    for t in tickets:
        province_region.setdefault(t["PROVINCE"], t["Region"])
    matrix = {prov: {label: 0 for label, _ in CLASSIFICATION_BOOKMARKS} for prov in provinces}
    for t in tickets:
        label = BOOKMARK_LABEL_LOOKUP.get(t["Bookmark"])
        if label:
            matrix[t["PROVINCE"]][label] += 1

    # Classification (all provinces) x extended Bookmark (SA Mobile split
    # into SA1-2/SA3/SA4).
    class_bm_totals = {}
    for t in tickets:
        label = BOOKMARK_LABEL_LOOKUP.get(t["Bookmark"])
        if not label:
            continue
        label = _extended_bookmark_label(label, t["SEVERITY"])
        cat = _last_classification_segment(t["CLASSIFICATION"])
        class_bm_totals.setdefault(cat, {lbl: 0 for lbl in EXTENDED_BOOKMARK_LABELS})
        class_bm_totals[cat][label] += 1
    classification_summary = sorted(
        [{"category": cat, "counts": counts, "total": sum(counts.values())} for cat, counts in class_bm_totals.items()],
        key=lambda r: r["total"], reverse=True,
    )

    # District (all provinces) x extended Bookmark.
    district_bm_totals = {}
    for t in tickets:
        label = BOOKMARK_LABEL_LOOKUP.get(t["Bookmark"])
        if not label:
            continue
        label = _extended_bookmark_label(label, t["SEVERITY"])
        d = t["DISTRICT"] or "(ไม่ระบุ)"
        district_bm_totals.setdefault(d, {lbl: 0 for lbl in EXTENDED_BOOKMARK_LABELS})
        district_bm_totals[d][label] += 1
    district_summary = sorted(
        [{"district": d, "counts": counts, "total": sum(counts.values())} for d, counts in district_bm_totals.items()],
        key=lambda r: r["total"], reverse=True,
    )

    # DN sites with at least one ticket - a focused subset of site_markers.
    dn_sites_with_tickets = [s for s in site_markers if s["is_dn"] and s["tickets"]]

    # Unique SITE count (not ticket count) per Province x Bookmark - a
    # single physical site with several open tickets should only count
    # once here.
    unique_site_sets = {prov: {label: set() for label in EXTENDED_BOOKMARK_LABELS} for prov in provinces}
    for t in tickets:
        if not t["CINAME"]:
            continue
        label = BOOKMARK_LABEL_LOOKUP.get(t["Bookmark"])
        if label:
            label = _extended_bookmark_label(label, t["SEVERITY"])
            unique_site_sets[t["PROVINCE"]][label].add(t["CINAME"].upper())
    unique_sites_by_province = []
    for prov in provinces:
        counts = {label: len(unique_site_sets[prov][label]) for label in EXTENDED_BOOKMARK_LABELS}
        all_site_ids = set()
        for s in unique_site_sets[prov].values():
            all_site_ids |= s
        unique_sites_by_province.append({"province": prov, "counts": counts, "total": len(all_site_ids)})
    unique_sites_by_province.sort(key=lambda r: -r["total"])
    unique_sites_affected = len({t["CINAME"].upper() for t in tickets if t["CINAME"]})

    # NOD/OFC workload calculator: real team headcount per province (from
    # the reference table, PROVINCE_TEAM_COUNTS) - ticket counts (SA
    # Mobile/Online/NSA1-2 only; SA4 -> NOD's workload, SA1-3+NSA1-2 ->
    # OFC's) get DIVIDED by team count to give workload PER TEAM, not a
    # raw ticket total.
    ticket_counts_by_province = {}
    for t in tickets:
        label = BOOKMARK_LABEL_LOOKUP.get(t["Bookmark"])
        if label not in WORKLOAD_BOOKMARKS:
            continue
        prov = t["PROVINCE"]
        ticket_counts_by_province.setdefault(prov, {"NOD": 0, "OFC": 0})
        if t["SEVERITY"] in WORKLOAD_NOD_SEVERITIES:
            ticket_counts_by_province[prov]["NOD"] += 1
        elif t["SEVERITY"] in WORKLOAD_OFC_SEVERITIES:
            ticket_counts_by_province[prov]["OFC"] += 1

    workload_table = []
    all_workload_provinces = sorted(set(ticket_counts_by_province) | set(PROVINCE_TEAM_COUNTS))
    for prov in all_workload_provinces:
        tickets_n = ticket_counts_by_province.get(prov, {"NOD": 0, "OFC": 0})
        teams_n = PROVINCE_TEAM_COUNTS.get(prov, {"NOD": 0, "OFC": 0})
        nod_per_team = round(tickets_n["NOD"] / teams_n["NOD"], 2) if teams_n["NOD"] else None
        ofc_per_team = round(tickets_n["OFC"] / teams_n["OFC"], 2) if teams_n["OFC"] else None
        workload_table.append({
            "province": prov,
            "nod_tickets": tickets_n["NOD"], "nod_teams": teams_n["NOD"], "nod_per_team": nod_per_team,
            "ofc_tickets": tickets_n["OFC"], "ofc_teams": teams_n["OFC"], "ofc_per_team": ofc_per_team,
        })
    workload_table.sort(key=lambda r: -(r["nod_tickets"] + r["ofc_tickets"]))
    workload_grand_total = {
        "nod_tickets": sum(r["nod_tickets"] for r in workload_table),
        "nod_teams": sum(r["nod_teams"] for r in workload_table),
        "ofc_tickets": sum(r["ofc_tickets"] for r in workload_table),
        "ofc_teams": sum(r["ofc_teams"] for r in workload_table),
    }
    if workload_grand_total["nod_teams"]:
        workload_grand_total["nod_per_team"] = round(workload_grand_total["nod_tickets"] / workload_grand_total["nod_teams"], 2)
    else:
        workload_grand_total["nod_per_team"] = None
    if workload_grand_total["ofc_teams"]:
        workload_grand_total["ofc_per_team"] = round(workload_grand_total["ofc_tickets"] / workload_grand_total["ofc_teams"], 2)
    else:
        workload_grand_total["ofc_per_team"] = None

    insert_time = scoped[0].get("insert_time") if scoped else None

    return {
        "sites": site_markers,
        "sites_mobile_online": sites_mobile_online,
        "sites_nsa12": sites_nsa12,
        "sites_nsa34": sites_nsa34,
        "tickets": tickets,
        "provinces": provinces,
        "province_region": province_region,
        "classification": {
            "provinces": provinces,
            "bookmarks": [label for label, _ in CLASSIFICATION_BOOKMARKS],
            "matrix": matrix,
        },
        "extended_bookmark_labels": EXTENDED_BOOKMARK_LABELS,
        "classification_summary": classification_summary,
        "district_summary": district_summary,
        "dn_sites_with_tickets": dn_sites_with_tickets,
        "unique_sites_by_province": unique_sites_by_province,
        "unique_sites_affected": unique_sites_affected,
        "workload_table": workload_table,
        "workload_grand_total": workload_grand_total,
        "insert_time": insert_time,
        "total_sites": len(site_markers),
        "total_tickets": len(tickets),
    }


# ── Manual remark pins ──────────────────────────────────────────────────

def _ensure_markers_tab(spreadsheet):
    try:
        return spreadsheet.worksheet(MANUAL_MARKERS_TAB)
    except Exception:
        ws = spreadsheet.add_worksheet(title=MANUAL_MARKERS_TAB, rows=1000, cols=len(MANUAL_MARKERS_HEADER))
        ws.append_row(MANUAL_MARKERS_HEADER)
        return ws


_manual_markers_cache = {"data": None, "ts": 0}
_manual_markers_lock = threading.Lock()
MANUAL_MARKERS_CACHE_TTL_SECONDS = 45


def list_manual_markers(gs_client, use_cache=True):
    now = time.monotonic()
    if use_cache:
        with _manual_markers_lock:
            if _manual_markers_cache["data"] is not None and (now - _manual_markers_cache["ts"]) < MANUAL_MARKERS_CACHE_TTL_SECONDS:
                return _manual_markers_cache["data"]
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_markers_tab(sh)
    rows = ws.get_all_values()[1:]
    out = []
    for row in rows:
        if not row or not row[0]:
            continue
        padded = row + [""] * (len(MANUAL_MARKERS_HEADER) - len(row))
        lat = _to_float(padded[1])
        lon = _to_float(padded[2])
        if lat is None or lon is None:
            continue
        out.append({
            "id": padded[0], "lat": lat, "lon": lon, "remark": padded[3],
            "created_by": padded[4], "created_at": padded[5],
        })
    if use_cache:
        with _manual_markers_lock:
            _manual_markers_cache["data"] = out
            _manual_markers_cache["ts"] = now
    return out


def _invalidate_manual_markers_cache():
    with _manual_markers_lock:
        _manual_markers_cache["data"] = None
        _manual_markers_cache["ts"] = 0


def add_manual_marker(gs_client, lat, lon, remark, created_by):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_markers_tab(sh)
    marker_id = uuid.uuid4().hex[:12]
    created_at = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append_row([marker_id, lat, lon, remark, created_by, created_at])
    _invalidate_manual_markers_cache()
    return {"id": marker_id, "lat": lat, "lon": lon, "remark": remark, "created_by": created_by, "created_at": created_at}


def delete_manual_marker(gs_client, marker_id):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_markers_tab(sh)
    cell = ws.find(marker_id, in_column=1)
    if cell is None:
        return False
    ws.delete_rows(cell.row)
    _invalidate_manual_markers_cache()
    return True


# ── DN site remarks (current action/status per site) ───────────────────
# One remark per site - submitting a new one overwrites the old, since
# this tracks "what's the team doing right now", not a running log.

def _ensure_site_remarks_tab(spreadsheet):
    try:
        return spreadsheet.worksheet(SITE_REMARKS_TAB)
    except Exception:
        ws = spreadsheet.add_worksheet(title=SITE_REMARKS_TAB, rows=1000, cols=len(SITE_REMARKS_HEADER))
        ws.append_row(SITE_REMARKS_HEADER)
        return ws


_site_remarks_cache = {"data": None, "ts": 0}
_site_remarks_lock = threading.Lock()
SITE_REMARKS_CACHE_TTL_SECONDS = 45


def get_site_remarks(gs_client, use_cache=True):
    """Returns {LOCATION_ID (upper): {remark, updated_by, updated_at}}."""
    now = time.monotonic()
    if use_cache:
        with _site_remarks_lock:
            if _site_remarks_cache["data"] is not None and (now - _site_remarks_cache["ts"]) < SITE_REMARKS_CACHE_TTL_SECONDS:
                return _site_remarks_cache["data"]
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_site_remarks_tab(sh)
    rows = ws.get_all_values()[1:]
    out = {}
    for row in rows:
        if not row or not row[0]:
            continue
        padded = row + [""] * (len(SITE_REMARKS_HEADER) - len(row))
        out[padded[0].strip().upper()] = {
            "remark": padded[1], "updated_by": padded[2], "updated_at": padded[3],
        }
    if use_cache:
        with _site_remarks_lock:
            _site_remarks_cache["data"] = out
            _site_remarks_cache["ts"] = now
    return out


def _invalidate_site_remarks_cache():
    with _site_remarks_lock:
        _site_remarks_cache["data"] = None
        _site_remarks_cache["ts"] = 0


def set_site_remark(gs_client, location_id, remark, updated_by):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_site_remarks_tab(sh)
    location_id = location_id.strip().upper()
    updated_at = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    cell = ws.find(location_id, in_column=1)
    if cell is not None:
        ws.update(f"B{cell.row}:D{cell.row}", [[remark, updated_by, updated_at]])
    else:
        ws.append_row([location_id, remark, updated_by, updated_at])
    _invalidate_site_remarks_cache()
    return {"location_id": location_id, "remark": remark, "updated_by": updated_by, "updated_at": updated_at}


def delete_site_remark(gs_client, location_id):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_site_remarks_tab(sh)
    location_id = location_id.strip().upper()
    cell = ws.find(location_id, in_column=1)
    if cell is None:
        return False
    ws.delete_rows(cell.row)
    _invalidate_site_remarks_cache()
    return True


# ── Trend charts: hourly, from backups, region-wide ─────────────────────
# Each hour's classification result is cached forever once computed (a
# finished hour's backup never changes) - only the CURRENT hour is ever
# freshly fetched from the live sheet. First load over a 3-day cold cache
# means downloading up to 72 backup files sequentially (slow); every load
# after that is fast since almost every hour is already cached.

_nan_trend_hour_cache = {}
_nan_trend_cache_lock = threading.Lock()

TREND_CATEGORIES = ["SA Mobile", "SA Online", "NSA1-2"]


def _classify_tickets_for_trend(rows):
    """rows: raw ticket row dicts (from fetch_live_rows or a downloaded
    backup snapshot - same shape either way). Returns (category_counts,
    sa_mobile_by_district, sa_mobile_by_province, sa_mobile_by_classification)
    for every NOR1/NOR2 ticket - no site matching needed, every row already
    carries its own province/district."""
    counts = {k: 0 for k in TREND_CATEGORIES}
    sa_mobile_by_district = {}
    sa_mobile_by_province = {}
    sa_mobile_by_classification = {}
    for r in rows:
        region = str(r.get("Region", "")).strip()
        if region not in PENDING_TICKET_REGIONS:
            continue
        sev = str(r.get("SEVERITY", "")).strip()
        if sev not in ALLOWED_SEVERITIES:
            continue
        bm = _exclusive_bookmark_label(r.get("Bookmark"))
        if bm == "7.MB with SA1-4":
            counts["SA Mobile"] += 1
            district = str(r.get("DISTRICT", "")).strip() or "(ไม่ระบุ)"
            sa_mobile_by_district[district] = sa_mobile_by_district.get(district, 0) + 1
            province = _ticket_province(r)
            sa_mobile_by_province[province] = sa_mobile_by_province.get(province, 0) + 1
            cat = _last_classification_segment(r.get("CLASSIFICATION"))
            sa_mobile_by_classification[cat] = sa_mobile_by_classification.get(cat, 0) + 1
        elif bm == "4.FBB with SA1-4":
            counts["SA Online"] += 1
        if sev in ("NSA1", "NSA2"):
            counts["NSA1-2"] += 1
    return counts, sa_mobile_by_district, sa_mobile_by_province, sa_mobile_by_classification


def _get_hour_classification(gs_client, drive_service, hour_dt, current_hour):
    """Cached per-hour lookup - the current hour always refetched live,
    every past hour cached forever once computed. Backup files land near
    :29 past each hour (same pattern run_hourly_job already relies on), so
    search a window around hour_dt + 29 min."""
    hour_key = hour_dt.strftime("%Y-%m-%dT%H:00")
    if hour_dt == current_hour:
        rows = fetch_live_rows(gs_client)
        return _classify_tickets_for_trend(rows)

    with _nan_trend_cache_lock:
        cached = _nan_trend_hour_cache.get(hour_key)
    if cached is not None:
        return cached

    search_target = hour_dt.replace(minute=29)
    file_info = find_closest_file(drive_service, search_target, window_minutes=25)
    if file_info is None:
        result = ({k: None for k in TREND_CATEGORIES}, {}, {}, {})
    else:
        file_id, _matched_dt, _filename = file_info
        backup_rows = download_xlsx_as_rows(drive_service, file_id)
        result = _classify_tickets_for_trend(backup_rows)

    with _nan_trend_cache_lock:
        _nan_trend_hour_cache[hour_key] = result
    return result


def _top_n_series(by_hour, labels, top_n):
    """Picks the top-N keys by total volume across the whole window,
    folding everything else into 'Other' - an unbounded categorical field
    would otherwise produce an unreadable number of lines."""
    totals = {}
    for hour_key in labels:
        for k, n in by_hour[hour_key].items():
            totals[k] = totals.get(k, 0) + (n or 0)
    top_keys = [k for k, _ in sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:top_n]]
    has_other = len(totals) > len(top_keys)
    series = {k: [by_hour[hour_key].get(k, 0) for hour_key in labels] for k in top_keys}
    if has_other:
        other_keys = set(totals) - set(top_keys)
        series["Other"] = [sum(by_hour[hour_key].get(k, 0) for k in other_keys) for hour_key in labels]
    return series


def build_nan_trends(gs_client, drive_service, hours=72, top_classifications=4, top_districts=10, top_provinces=10):
    """Returns four trend charts in one pass (they need the same per-hour
    downloads, so computing them together avoids fetching each backup file
    4x): the 5-category hourly ticket count trend, the SA-Mobile-only
    per-district hourly trend (top N), the SA-Mobile-only per-province
    hourly trend, and the SA-Mobile-only per-Classification hourly trend
    (top N), all over the last `hours` hours, region-wide."""
    current_hour = bangkok_now().replace(minute=0, second=0, microsecond=0)
    all_hours = [current_hour - timedelta(hours=i) for i in range(hours - 1, -1, -1)]  # oldest -> newest

    labels = []
    category_series = {k: [] for k in TREND_CATEGORIES}
    district_by_hour = {}
    province_by_hour = {}
    classification_by_hour = {}
    for hour_dt in all_hours:
        hour_key = hour_dt.strftime("%Y-%m-%dT%H:00")
        labels.append(hour_key)
        counts, dist_counts, prov_counts, class_counts = _get_hour_classification(gs_client, drive_service, hour_dt, current_hour)
        for k in TREND_CATEGORIES:
            category_series[k].append(counts.get(k))
        district_by_hour[hour_key] = dist_counts
        province_by_hour[hour_key] = prov_counts
        classification_by_hour[hour_key] = class_counts

    district_series = _top_n_series(district_by_hour, labels, top_districts)
    province_series = _top_n_series(province_by_hour, labels, top_provinces)  # top 10 provinces, rest folded into "Other"
    classification_series = _top_n_series(classification_by_hour, labels, top_classifications)

    return {
        "ticket_trend": {"dates": labels, "series": category_series},
        "district_trend_sa_mobile": {"dates": labels, "districts": list(district_series.keys()), "series": district_series},
        "province_trend_sa_mobile": {"dates": labels, "provinces": list(province_series.keys()), "series": province_series},
        "classification_trend_sa_mobile": {"dates": labels, "categories": list(classification_series.keys()), "series": classification_series},
    }
