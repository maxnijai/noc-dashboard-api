"""OFC Monitor - Executive Operational Dashboard for OFC Work Orders
(explicit spec, section-numbered comments below refer to that spec).

Reuses mateline_status.fetch_ggs_daily_rows() for the "Daily" sheet
(same GGS_DAILY_SHEET_ID, already cached + timeout-protected there - see
that module for why the timeout matters) rather than duplicating the
fetch, per spec section 19/21 ("ห้าม Fetch Google Sheet ซ้ำ", "ห้าม
Duplicate Google Sheet API logic"). The second sheet ("data", the
Bookmark/DISTRICT mapping source) gets its own fetch here, built with
the SAME bounded-timeout + daemon-thread pattern mateline_status.py
uses, applied from the start this time rather than after a production
incident.

Architecture (spec section 19): fetch both sheets once, build the
ticket->mapping lookup once, return ONE scoped-to-default entry list
plus the mapping-coverage/duplicate/missing stats needed for the KPI
cards. All further filtering/aggregation (by team, province, P0/P1,
etc.) happens client-side in the browser from this one dataset - the
backend is never re-hit just to change a filter.
"""

import logging
import threading
import time
from datetime import datetime

import mateline_status

log = logging.getLogger(__name__)

MAPPING_SHEET_ID = "1AEQSsiLUbr5p6HYh36WNGF9TkUDVeW2xN-vDvDkjy1k"
MAPPING_TAB = "data"
MAPPING_DISTRICT_COL_INDEX = 38  # column AM (0-indexed: A=0 ... AM=38) - fallback if the header row doesn't literally say "DISTRICT"

_mapping_cache = {"data": None, "ts": 0}
_mapping_lock = threading.Lock()
MAPPING_CACHE_TTL_SECONDS = 300
MAPPING_FETCH_TIMEOUT_SECONDS = 15

# Config (spec section 8: "ต้องทำเป็น Config ที่แก้ไขได้ง่ายใน Code ห้าม
# Hardcode กระจายหลายจุด") - the ONLY place these thresholds are defined.
HIGH_LOAD_THRESHOLD = 8
CRITICAL_LOAD_THRESHOLD = 12

# Default scope (spec section 3) - applied server-side once, before the
# entries are handed to the frontend, so the person never has to set
# these up themselves.
DEFAULT_SKILL = "OFC"
DEFAULT_REGIONS = {"NOR1", "NOR2"}
DEFAULT_BOOKMARK = "4.FBB with SA1-4"

NA_LABEL = "N/A"


def _fetch_mapping_rows_raw(gs_client):
    sh = gs_client.open_by_key(MAPPING_SHEET_ID)
    ws = sh.worksheet(MAPPING_TAB)
    return ws.get_all_values()


def _fetch_mapping_rows_bounded(gs_client, timeout_seconds):
    """Same daemon-thread + Event bounded-wait pattern as
    mateline_status._fetch_ggs_daily_rows_bounded - see that function's
    docstring for the full rationale (a plain daemon Thread rather than
    ThreadPoolExecutor, specifically so a genuine hang can never block
    clean process shutdown)."""
    result, error, done = {}, {}, threading.Event()

    def _run():
        try:
            result["rows"] = _fetch_mapping_rows_raw(gs_client)
        except Exception as e:
            error["exc"] = e
        finally:
            done.set()

    threading.Thread(target=_run, daemon=True, name="ofc-mapping-fetch").start()
    if not done.wait(timeout=timeout_seconds):
        raise TimeoutError(f"OFC mapping sheet fetch timed out after {timeout_seconds}s")
    if "exc" in error:
        raise error["exc"]
    return result["rows"]


def fetch_mapping_rows(gs_client, use_cache=True):
    now = time.monotonic()
    if not use_cache:
        return _fetch_mapping_rows_raw(gs_client)
    with _mapping_lock:
        if _mapping_cache["data"] is not None and (now - _mapping_cache["ts"]) < MAPPING_CACHE_TTL_SECONDS:
            return _mapping_cache["data"]
        try:
            rows = _fetch_mapping_rows_bounded(gs_client, MAPPING_FETCH_TIMEOUT_SECONDS)
        except TimeoutError:
            log.error("OFC mapping sheet fetch timed out after %ss - giving up on this attempt", MAPPING_FETCH_TIMEOUT_SECONDS)
            raise
        _mapping_cache["data"] = rows
        _mapping_cache["ts"] = time.monotonic()
        return rows


def _safe_str(v):
    """Same NaN/None-safe stringify as pending_ticket._safe_str - kept
    as its own copy here rather than importing across modules for a one
    line helper, but fixes the identical class of bug (a raw NaN value
    reaching json.dumps breaks the whole response for the browser)."""
    if v is None:
        return ""
    if isinstance(v, float) and v != v:
        return ""
    return str(v).strip()


def build_ticket_mapping(gs_client):
    """Returns {ticket_id_upper: {"bookmark", "district"}} from the
    "data" sheet, keyed by TICKETID (spec section 2's Daily[Source
    Ticket ID] -> data[TICKETID] match). DISTRICT is read by header name
    first; if no column is literally named "DISTRICT", falls back to
    the explicit column AM position given in the spec - covers either a
    differently-worded header or a column that was never labeled."""
    rows = fetch_mapping_rows(gs_client)
    if not rows:
        return {}
    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}
    district_idx = col.get("DISTRICT", MAPPING_DISTRICT_COL_INDEX)
    ticket_idx = col.get("TICKETID")
    bookmark_idx = col.get("Bookmark")

    def get(row, idx):
        if idx is None or idx >= len(row):
            return ""
        return row[idx]

    mapping = {}
    for row in rows[1:]:
        if not row or not any(row):
            continue
        tid = _safe_str(get(row, ticket_idx))
        if not tid:
            continue
        mapping[tid.upper()] = {
            "bookmark": _safe_str(get(row, bookmark_idx)),
            "district": _safe_str(get(row, district_idx)),
        }
    return mapping


def build_ofc_monitor_response(gs_client):
    daily_rows = mateline_status.fetch_ggs_daily_rows(gs_client)
    mapping = build_ticket_mapping(gs_client)

    if not daily_rows:
        return {"entries": [], "integrity": {}, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    header = daily_rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def get(row, name):
        i = col.get(name)
        if i is None or i >= len(row):
            return ""
        return row[i]

    seen_ticket_ids = set()
    duplicate_ticket_ids = set()
    missing_ticket_id_count = 0
    entries = []

    for row in daily_rows[1:]:
        if not row or not any(row):
            continue
        skill = _safe_str(get(row, "Skill"))
        region = _safe_str(get(row, "Region"))
        # Default scope (spec section 3) applied here, server-side, once.
        if skill != DEFAULT_SKILL or region not in DEFAULT_REGIONS:
            continue

        source_tid = _safe_str(get(row, "Source Ticket ID"))
        if not source_tid:
            missing_ticket_id_count += 1
            continue
        tid_upper = source_tid.upper()
        if tid_upper in seen_ticket_ids:
            duplicate_ticket_ids.add(tid_upper)
        seen_ticket_ids.add(tid_upper)

        m = mapping.get(tid_upper)
        bookmark = (m or {}).get("bookmark") or ""
        district = (m or {}).get("district") or ""
        is_mapped = bool(m)
        if not bookmark:
            bookmark = NA_LABEL
        if not district:
            district = NA_LABEL

        # Default scope also filters on Bookmark - unmapped rows (no
        # Bookmark at all) are EXCLUDED from the default-scoped entries
        # list here (they wouldn't match DEFAULT_BOOKMARK), but they
        # still count toward the UNMAPPED OWS KPI via unmapped_count
        # below, computed over the Skill+Region-scoped set before this
        # Bookmark filter - so the KPI reflects everyone still missing a
        # mapping, not just the ones already matching the default view.
        entries.append({
            "ticket_id": source_tid,
            "owner": _safe_str(get(row, "Owner")),
            "team": _safe_str(get(row, "Team")) or NA_LABEL,
            "severity": _safe_str(get(row, "Severity")) or NA_LABEL,
            "site_id": _safe_str(get(row, "Site ID")),
            "subject": _safe_str(get(row, "Subject")),
            "departed": _safe_str(get(row, "Departed")),
            "arrived": _safe_str(get(row, "Arrived")),
            "completed": _safe_str(get(row, "Completed")),
            "closed": _safe_str(get(row, "Closed")),
            "region": region,
            "skill": skill,
            "bookmark": bookmark,
            "district": district,
            "is_mapped": is_mapped,
        })

    unmapped_count = sum(1 for e in entries if not e["is_mapped"])
    default_scope_entries = [e for e in entries if e["bookmark"] == DEFAULT_BOOKMARK]

    integrity = {
        "duplicate_ticket_ids": sorted(duplicate_ticket_ids),
        "duplicate_count": len(duplicate_ticket_ids),
        "missing_ticket_id_count": missing_ticket_id_count,
        "total_ofc_nor_rows": len(entries),
        "unmapped_count": unmapped_count,
    }

    return {
        "entries": default_scope_entries,
        "all_skill_region_entries_count": len(entries),
        "integrity": integrity,
        "config": {
            "high_load_threshold": HIGH_LOAD_THRESHOLD,
            "critical_load_threshold": CRITICAL_LOAD_THRESHOLD,
            "default_skill": DEFAULT_SKILL,
            "default_regions": sorted(DEFAULT_REGIONS),
            "default_bookmark": DEFAULT_BOOKMARK,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
