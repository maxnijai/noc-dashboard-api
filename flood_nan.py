"""Flood NAN Monitoring: plots every known site in Nan province on a map,
color-coded by the worst open ticket severity found for that site (matched
by LOCATION ID == CINAME, or LOCATION ID appearing inside SUBJECT, against
the live Pending Ticket data), with a distinct marker for "DN"
(important/generator-equipped) sites, plus manually-placed remark pins.

Site master data comes from a SEPARATE Google Sheet (not the main
REALTIME_SHEET_ID) - a manually maintained inventory of every physical
site, one sheet ("ชีต1") with three stacked tables:
  1. Site master (LOCATION ID, Type Site, ..., LATITUDE, LONGITUDE) - the
     main table this feature reads from.
  2. A generator/DN equipment inventory (SITE, Lat, Lon, ...) covering many
     provinces, not just Nan - not used here directly.
  3. A short reference list at the bottom (blank col A, site code col B,
     literal "DN" col C) enumerating which Nan sites are flagged important.
Both table 1's own "Type Site" == "DN" rows AND table 3's list count as
DN sites - the two overlap for some codes but not entirely.

Manual remark pins are stored in their own tab ("FloodNanMarkers") inside
REALTIME_SHEET_ID, next to TicketWorkLog - not on the foreign site-master
sheet, which this feature only ever reads from, never writes to.
"""

import logging
import threading
import time
import uuid

from pending_ticket import _fetch_full_ticket_entries, ALLOWED_SEVERITIES, _exclusive_bookmark_label
from pending_trend import get_drive_and_sheets_clients, bangkok_now
from realtime_monitor import REALTIME_SHEET_ID

log = logging.getLogger(__name__)

NAN_SITE_SHEET_ID = "18BOiJCBQ-42QgURqnZBZSQ8hLGaxQXNuLePdMNVt3Os"
NAN_SITE_TAB_NAME = "ชีต1"

# Severity -> marker color, per the requested classification.
SEVERITY_COLOR = {
    "SA1": "#E24B4A", "SA2": "#E24B4A", "SA3": "#E24B4A", "SA4": "#E24B4A",
    "NSA1": "#EF9F27", "NSA2": "#EF9F27",
    "NSA3": "#F5C518", "NSA4": "#F5C518",
}
# Worse-first order, used to pick ONE color when a site has multiple open
# tickets with different severities.
SEVERITY_RANK = {"SA1": 0, "SA2": 0, "SA3": 0, "SA4": 0, "NSA1": 1, "NSA2": 1, "NSA3": 2, "NSA4": 2}

# The 4 Bookmark buckets used for the severity x bookmark matrix - same
# labels Exclusive Pending / P0 Only use (see pending_ticket.py's
# EXCLUSIVE_BOOKMARK_ORDER), just relabeled here per the request wording.
CLASSIFICATION_BOOKMARKS = [
    ("SA Mobile", "7.MB with SA1-4"),
    ("Online", "4.FBB with SA1-4"),
    ("NSA1-2", "3. All NW Incident NSA1-2"),
    ("NSA3-4", "NSA3-4"),
]
CLASSIFICATION_SEVERITIES = ["SA1", "SA2", "SA3", "SA4", "NSA1", "NSA2", "NSA3", "NSA4"]

_site_cache = {"data": None, "ts": 0}
_cache_lock = threading.Lock()
SITE_CACHE_TTL_SECONDS = 600  # site master rarely changes - safe to cache for a while

MANUAL_MARKERS_TAB = "FloodNanMarkers"
MANUAL_MARKERS_HEADER = ["id", "lat", "lon", "remark", "created_by", "created_at"]


def _to_float(v):
    try:
        f = float(str(v).strip())
        return f if f else None
    except (TypeError, ValueError):
        return None


def fetch_nan_sites(gs_client, use_cache=True):
    """Returns [{location_id, name_en, name_th, lat, lon, district_e,
    district_t, subdistrict_e, subdistrict_t, is_dn}, ...] for every Nan
    site with valid coordinates."""
    now = time.monotonic()
    if use_cache:
        with _cache_lock:
            if _site_cache["data"] is not None and (now - _site_cache["ts"]) < SITE_CACHE_TTL_SECONDS:
                return _site_cache["data"]

    sh = gs_client.open_by_key(NAN_SITE_SHEET_ID)
    ws = sh.worksheet(NAN_SITE_TAB_NAME)
    rows = ws.get_all_values()

    # --- Table 1: site master (header row has "LOCATION ID" in col A) ---
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip() == "LOCATION ID":
            header_idx = i
            break

    sites = []
    dn_codes_from_table1 = set()
    end_idx = len(rows)
    if header_idx is not None:
        for i in range(header_idx + 1, len(rows)):
            row = rows[i]
            location_id = (row[0] if len(row) > 0 else "").strip()
            if not location_id:
                end_idx = i
                break
            type_site = (row[1] if len(row) > 1 else "").strip()
            if type_site.upper() == "DN":
                dn_codes_from_table1.add(location_id.upper())
            name_en = row[7] if len(row) > 7 else ""
            name_th = row[8] if len(row) > 8 else ""
            district_e = row[24] if len(row) > 24 else ""
            # Ticket data's own DISTRICT field never has the "อำเภอ" prefix
            # (e.g. "แม่จริม"), but this site sheet's DISTRICT_T column always
            # does (e.g. "อำเภอแม่จริม") - strip it here so site<->ticket
            # district matching (and the click-to-filter feature) works with
            # simple string equality instead of silently never matching.
            district_t = (row[25] if len(row) > 25 else "").strip()
            if district_t.startswith("อำเภอ"):
                district_t = district_t[len("อำเภอ"):].strip()
            subdistrict_e = row[26] if len(row) > 26 else ""
            subdistrict_t = row[27] if len(row) > 27 else ""
            lat = _to_float(row[28]) if len(row) > 28 else None
            lon = _to_float(row[29]) if len(row) > 29 else None
            if lat is None or lon is None:
                continue
            sites.append({
                "location_id": location_id, "name_en": name_en, "name_th": name_th,
                "lat": lat, "lon": lon, "district_e": district_e, "district_t": district_t,
                "subdistrict_e": subdistrict_e, "subdistrict_t": subdistrict_t,
            })

    # --- Reference list further down: blank col A, site code col B, "DN" col C ---
    dn_codes_from_reflist = set()
    for row in rows[end_idx:]:
        col_a = (row[0] if len(row) > 0 else "").strip()
        col_b = (row[1] if len(row) > 1 else "").strip()
        col_c = (row[2] if len(row) > 2 else "").strip()
        if not col_a and col_b and col_c.upper() == "DN":
            dn_codes_from_reflist.add(col_b.upper())

    dn_codes = dn_codes_from_table1 | dn_codes_from_reflist
    for s in sites:
        s["is_dn"] = s["location_id"].upper() in dn_codes

    if use_cache:
        with _cache_lock:
            _site_cache["data"] = sites
            _site_cache["ts"] = now
    return sites


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


def build_flood_nan_response(gs_client=None):
    if gs_client is None:
        _, gs_client = get_drive_and_sheets_clients()

    sites = fetch_nan_sites(gs_client)
    all_entries = _fetch_full_ticket_entries(gs_client)

    # Only tickets with a real severity are relevant to plot/count.
    live_entries = [t for t in all_entries if str(t.get("SEVERITY", "")).strip() in ALLOWED_SEVERITIES]
    # Relabel Bookmark the same way Exclusive Pending / P0 Only do (raw
    # values only ever cover the 3 named categories - anything else,
    # including every NSA3/NSA4 ticket, needs this to land in the "NSA3-4"
    # catch-all instead of never matching anything downstream).
    for t in live_entries:
        t["Bookmark"] = _exclusive_bookmark_label(t.get("Bookmark"))
    tickets_by_ciname = {}
    for t in live_entries:
        ciname = str(t.get("CINAME", "")).strip().upper()
        if ciname:
            tickets_by_ciname.setdefault(ciname, []).append(t)

    site_markers = []
    matched_ticket_ids = set()
    nan_tickets = []
    for s in sites:
        loc_upper = s["location_id"].upper()
        # 1) exact CINAME match, 2) LOCATION ID appearing inside SUBJECT text
        # (tickets often embed the site code in the subject line even when
        # CINAME itself doesn't match, e.g. "[U05][NW] CMI0233 : ...").
        by_ciname = tickets_by_ciname.get(loc_upper, [])
        by_ciname_ids = {m.get("TICKETID") for m in by_ciname}
        by_subject = [
            t for t in live_entries
            if loc_upper in str(t.get("SUBJECT", "")).upper() and t.get("TICKETID") not in by_ciname_ids
        ]
        matches = by_ciname + by_subject
        color = None
        if matches:
            best = min(matches, key=lambda t: SEVERITY_RANK.get(str(t.get("SEVERITY", "")).strip(), 9))
            color = SEVERITY_COLOR.get(str(best.get("SEVERITY", "")).strip())
        site_markers.append({
            "location_id": s["location_id"], "name_en": s["name_en"], "name_th": s["name_th"],
            "lat": s["lat"], "lon": s["lon"], "is_dn": s["is_dn"], "color": color,
            "district_e": s["district_e"], "district_t": s["district_t"], "subdistrict_e": s["subdistrict_e"],
            "tickets": [{
                "TICKETID": t.get("TICKETID", ""), "SEVERITY": t.get("SEVERITY", ""),
                "CREATIONDATE": t.get("CREATIONDATE", ""), "CINAME": t.get("CINAME", ""),
                "DISTRICT": t.get("DISTRICT", ""), "SUBDISTRICT": t.get("SUBDISTRICT", ""),
                "SUBJECT": t.get("SUBJECT", ""), "Bookmark": t.get("Bookmark", ""),
                "CLASSIFICATION": t.get("CLASSIFICATION", ""),
            } for t in matches],
        })
        for t in matches:
            tid = t.get("TICKETID")
            if tid not in matched_ticket_ids:
                matched_ticket_ids.add(tid)
                nan_tickets.append(t)

    # Severity x Bookmark matrix (SA1-4 rows x SA Mobile/Online/NSA1-2/NSA3-4).
    matrix = {sev: {label: 0 for label, _ in CLASSIFICATION_BOOKMARKS} for sev in CLASSIFICATION_SEVERITIES}
    bookmark_lookup = {raw: label for label, raw in CLASSIFICATION_BOOKMARKS}
    for t in nan_tickets:
        sev = str(t.get("SEVERITY", "")).strip()
        if sev not in CLASSIFICATION_SEVERITIES:
            continue
        label = bookmark_lookup.get(str(t.get("Bookmark", "")).strip())
        if label:
            matrix[sev][label] += 1

    # CLASSIFICATION summary - last (most specific) segment, counted.
    classification_totals = {}
    for t in nan_tickets:
        cat = _last_classification_segment(t.get("CLASSIFICATION"))
        classification_totals[cat] = classification_totals.get(cat, 0) + 1
    classification_summary = sorted(
        [{"category": cat, "total": total} for cat, total in classification_totals.items()],
        key=lambda r: r["total"], reverse=True,
    )

    # District summary - ticket count per DISTRICT.
    district_totals = {}
    for t in nan_tickets:
        d = str(t.get("DISTRICT", "")).strip() or "(ไม่ระบุ)"
        district_totals[d] = district_totals.get(d, 0) + 1
    district_summary = sorted(
        [{"district": d, "total": total} for d, total in district_totals.items()],
        key=lambda r: r["total"], reverse=True,
    )

    # DN sites with at least one matched ticket - a focused subset of
    # site_markers, kept separate since these are the highest-priority
    # sites to check first (important/generator-equipped AND affected).
    dn_sites_with_tickets = [s for s in site_markers if s["is_dn"] and s["tickets"]]

    return {
        "sites": site_markers,
        "tickets": nan_tickets,
        "classification": {
            "severities": CLASSIFICATION_SEVERITIES,
            "bookmarks": [label for label, _ in CLASSIFICATION_BOOKMARKS],
            "matrix": matrix,
        },
        "classification_summary": classification_summary,
        "district_summary": district_summary,
        "dn_sites_with_tickets": dn_sites_with_tickets,
        "total_sites": len(site_markers),
        "total_tickets": len(nan_tickets),
    }


# ── Manual remark pins ──────────────────────────────────────────────────

def _ensure_markers_tab(spreadsheet):
    try:
        return spreadsheet.worksheet(MANUAL_MARKERS_TAB)
    except Exception:
        ws = spreadsheet.add_worksheet(title=MANUAL_MARKERS_TAB, rows=1000, cols=len(MANUAL_MARKERS_HEADER))
        ws.append_row(MANUAL_MARKERS_HEADER)
        return ws


def list_manual_markers(gs_client):
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
    return out


def add_manual_marker(gs_client, lat, lon, remark, created_by):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_markers_tab(sh)
    marker_id = uuid.uuid4().hex[:12]
    created_at = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    ws.append_row([marker_id, lat, lon, remark, created_by, created_at])
    return {"id": marker_id, "lat": lat, "lon": lon, "remark": remark, "created_by": created_by, "created_at": created_at}


def delete_manual_marker(gs_client, marker_id):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _ensure_markers_tab(sh)
    cell = ws.find(marker_id, in_column=1)
    if cell is None:
        return False
    ws.delete_rows(cell.row)
    return True
