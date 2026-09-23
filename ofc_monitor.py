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
import re
import threading
import time
from datetime import datetime

import mateline_status

log = logging.getLogger(__name__)

# Reused verbatim from sla_improvement._extract_region_province (explicit
# request: same TRUEOWNERGROUP-style parsing, applied here to the Daily
# sheet's "Owner" column instead - e.g. "TRUE-TH-BBT-NOR1-CMI1-NOP" ->
# province "CMI". Kept as its own copy rather than importing across
# modules for one small function, but the pattern/merge table must stay
# identical to that module's - if it's ever updated there, update here
# too.
_TOG_PATTERN = re.compile(r"TRUE-TH-BBT-(NOR[12])-([A-Z0-9]+)-NOP")
_PROVINCE_CODE_MERGE = {"CMI1": "CMI", "CMI2": "CMI"}


def _extract_province_from_owner(owner_value):
    m = _TOG_PATTERN.match(str(owner_value or "").strip())
    if not m:
        return None
    code = m.group(2)
    return _PROVINCE_CODE_MERGE.get(code, code)

MAPPING_SHEET_ID = "1AEQSsiLUbr5p6HYh36WNGF9TkUDVeW2xN-vDvDkjy1k"
MAPPING_TAB = "data"
MAPPING_DISTRICT_COL_INDEX = 38  # column AM (0-indexed: A=0 ... AM=38) - fallback if the header row doesn't literally say "DISTRICT"
MAPPING_OWNER_COL_INDEX = 49  # column AX (0-indexed: A=0 ... AX=49) - fallback if the header row doesn't literally say "Owner"

_mapping_cache = {"data": None, "ts": 0}
_mapping_lock = threading.Lock()
MAPPING_CACHE_TTL_SECONDS = 300
MAPPING_FETCH_TIMEOUT_SECONDS = 15

# Remark/note storage (explicit request: a per-ticket free-text note,
# something no source sheet has - needs its own place to persist).
# Same spreadsheet the Pending Ticket mirror export already writes to
# (EXPORT_SHEET_ID in pending_ticket.py), but its own tab.
NOTE_SHEET_ID = "10Y3Pyp6-MqlrcxiDXdAjTNWWZLgDWp94dZvU51lOxWU"
NOTE_WORKSHEET_GID = 804374063
NOTE_HEADER = ["TICKETID", "Remark", "updated_by", "updated_at"]

_note_cache = {"data": None, "ts": 0}
_note_lock = threading.Lock()
NOTE_CACHE_TTL_SECONDS = 60  # short TTL - remarks are meant to feel responsive after saving, unlike the 5-minute source-data caches above


def _get_note_worksheet(gs_client):
    sh = gs_client.open_by_key(NOTE_SHEET_ID)
    for ws in sh.worksheets():
        if ws.id == NOTE_WORKSHEET_GID:
            return ws
    # Falls back to creating the tab if the gid given doesn't (yet) exist
    # - keeps this working even before anyone manually sets the tab up,
    # rather than throwing on every remark save.
    return sh.add_worksheet(title="Note Ticket", rows=2000, cols=len(NOTE_HEADER))


def _fetch_note_rows_raw(gs_client):
    ws = _get_note_worksheet(gs_client)
    rows = ws.get_all_values()
    if not rows:
        ws.append_row(NOTE_HEADER)
        return [NOTE_HEADER]
    return rows


def load_ofc_remarks(gs_client, use_cache=True):
    """Returns {ticket_id_upper: remark_text}. Cached briefly (60s, much
    shorter than the source-data caches above) - a remark someone just
    typed and saved should show back up on refresh quickly, not sit
    behind the same 5-minute window as Daily/mapping data that changes
    far less often."""
    now = time.monotonic()
    if not use_cache:
        rows = _fetch_note_rows_raw(gs_client)
    else:
        with _note_lock:
            if _note_cache["data"] is not None and (now - _note_cache["ts"]) < NOTE_CACHE_TTL_SECONDS:
                rows = _note_cache["data"]
            else:
                rows = _fetch_note_rows_raw(gs_client)
                _note_cache["data"] = rows
                _note_cache["ts"] = time.monotonic()
    if not rows:
        return {}
    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}
    tid_idx = col.get("TICKETID", 0)
    remark_idx = col.get("Remark", 1)
    remarks = {}
    for row in rows[1:]:
        if not row or not any(row):
            continue
        tid = row[tid_idx].strip() if tid_idx < len(row) else ""
        if not tid:
            continue
        remarks[tid.upper()] = row[remark_idx] if remark_idx < len(row) else ""
    return remarks


def save_ofc_remark(gs_client, ticket_id, remark, updated_by=None):
    ticket_id = (ticket_id or "").strip()
    if not ticket_id:
        raise ValueError("ticket_id is required")
    ws = _get_note_worksheet(gs_client)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cell = None
    try:
        cell = ws.find(ticket_id)
    except Exception:
        cell = None  # gspread raises if not found rather than returning None, depending on version - either way, "not found" means append below
    if cell is not None:
        ws.update(f"A{cell.row}", [[ticket_id, remark, updated_by or "", now_str]])
    else:
        ws.append_row([ticket_id, remark, updated_by or "", now_str])
    with _note_lock:
        _note_cache["data"] = None  # invalidate - next read gets the fresh value instead of waiting out the TTL

# Config (spec section 8: "ต้องทำเป็น Config ที่แก้ไขได้ง่ายใน Code ห้าม
# Hardcode กระจายหลายจุด") - the ONLY place these thresholds are defined.
HIGH_LOAD_THRESHOLD = 8
CRITICAL_LOAD_THRESHOLD = 12

# Default scope (spec section 3) - applied server-side once, before the
# entries are handed to the frontend, so the person never has to set
# these up themselves.
DEFAULT_SKILL = "OFC"
DEFAULT_REGIONS = {"NOR"}  # confirmed against real data: the sheet stores the combined NOR1+NOR2 scope as the single value "NOR" (no digit suffix) - "NOR1"/"NOR2" never actually appear
DEFAULT_BOOKMARK = "4.FBB with SA1-4"

NA_LABEL = "N/A"

# Statuses that mean "not actually active work anymore" (explicit
# request: "ผมต้องการดูงานที่มีในมือจริงๆ") - excluded from the entries
# list itself, but NOT from all_teams below, so a team whose every
# ticket happens to be in one of these statuses still shows up as an
# idle team with 0 active tickets, instead of silently disappearing.
EXCLUDED_STATUSES = {"Canceled", "Closed (Auto)", "Closed"}


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


def build_ticket_mapping(gs_client, use_cache=True):
    """Returns (mapping, header) where mapping is
    {ticket_id_upper: {"bookmark", "district", "filter_province"}} from
    the "data" sheet, keyed by TICKETID (spec section 2's Daily[Source
    Ticket ID] -> data[TICKETID] match), and header is the sheet's
    actual header row (returned for diagnostics - if "TICKETID" or
    "Bookmark" isn't found verbatim in it, every row is silently skipped
    and mapping comes back empty, which otherwise looks identical to
    "the sheet has no data").
    DISTRICT is read by header name first; if no column is literally
    named "DISTRICT", falls back to the explicit column AM position
    given in the spec.
    filter_province (explicit request: "Filter Province ให้เอาคอลัม (AX)
    Owner มาแทน") is this sheet's OWN "Owner" column (also TRUEOWNERGROUP-
    style, same as Daily's Owner but a separate value on this separate
    sheet) - read by header name first, falling back to column AX. Used
    ONLY to drive the Province filter dropdown; DISTRICT above still
    drives OWS Mapping Coverage, and Daily's own Owner (elsewhere in
    this module) still drives the Work Order by Province grouping - all
    three are deliberately kept independent, not merged into one."""
    rows = fetch_mapping_rows(gs_client, use_cache=use_cache)
    if not rows:
        return {}, []
    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}
    district_idx = col.get("DISTRICT", MAPPING_DISTRICT_COL_INDEX)
    ticket_idx = col.get("TICKETID")
    bookmark_idx = col.get("Bookmark")
    # "Owner" isn't a header this sheet actually has (per its known
    # column list) - TRUEOWNERGROUP is the field that holds this
    # TRUEOWNERGROUP-style data here, so that's checked first. Column AX
    # is only a last-resort position fallback, and on this sheet's real
    # layout lands on Tech_timestamp, not an Owner-style field - kept
    # only in case the header row is ever reshuffled without warning.
    owner_idx = col.get("TRUEOWNERGROUP", col.get("Owner", MAPPING_OWNER_COL_INDEX))

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
            "filter_province": _extract_province_from_owner(get(row, owner_idx)) or "",
        }
    return mapping, header


def build_ofc_monitor_response(gs_client, force_refresh=False):
    # force_refresh (explicit request: "ปุ่ม Refresh ขอให้เช็คเพื่อให้ใช้
    # งานได้จริงครับ") bypasses both sheets' 5-minute server-side caches -
    # without this, clicking Refresh within the same 5-minute window
    # would silently re-serve the same cached data, making the button
    # look broken even though a real HTTP round-trip did happen.
    daily_rows = mateline_status.fetch_ggs_daily_rows(gs_client, use_cache=not force_refresh)
    mapping, mapping_header = build_ticket_mapping(gs_client, use_cache=not force_refresh)
    try:
        remarks = load_ofc_remarks(gs_client, use_cache=not force_refresh)
    except Exception:
        # A failure here shouldn't take down the whole page - every
        # entry just falls back to "no remark yet" if the Note Ticket
        # sheet can't be reached this time.
        log.exception("load_ofc_remarks failed - remarks will show blank for this response")
        remarks = {}

    if not daily_rows:
        return {"entries": [], "integrity": {}, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    header = daily_rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def get(row, name):
        i = col.get(name)
        if i is None or i >= len(row):
            return ""
        return row[i]

    # Data batch/snapshot time (explicit request: show clearly on the
    # page header what time round this data is from) - "insert_time" is
    # column A, written fresh on every sync of this sheet, so it's the
    # same value across every row in one batch; just read it off the
    # first data row rather than re-deriving it per entry.
    data_insert_time = ""
    for row in daily_rows[1:]:
        if row and any(row):
            data_insert_time = _safe_str(get(row, "insert_time") if "insert_time" in col else (row[0] if row else ""))
            break

    seen_ticket_ids = set()
    duplicate_ticket_ids = set()
    missing_ticket_id_count = 0
    skipped_no_province = 0
    entries = []

    # Diagnostic counters (explicit request: pinpoint where the funnel
    # narrows to zero without needing live sheet access) - cheap to keep
    # always on, not gated behind a debug flag.
    diag_skill_values = {}
    diag_region_values = {}
    diag_bookmark_values = {}
    diag_severity_values = {}
    diag_total_daily_data_rows = 0
    diag_after_skill_region = 0
    # Pinpointing "some active tickets show no SLA remaining" (explicit
    # report) - counts Require Finish Time presence, split by Status, so
    # the pattern is visible without live sheet access: is it blank for
    # a specific Status only, or scattered regardless of Status?
    diag_finish_time_by_status = {}

    for row in daily_rows[1:]:
        if not row or not any(row):
            continue
        diag_total_daily_data_rows += 1
        skill = _safe_str(get(row, "Skill"))
        region = _safe_str(get(row, "Region"))
        diag_skill_values[skill] = diag_skill_values.get(skill, 0) + 1
        diag_region_values[region] = diag_region_values.get(region, 0) + 1
        # Default scope (spec section 3) applied here, server-side, once.
        if skill != DEFAULT_SKILL or region not in DEFAULT_REGIONS:
            continue
        diag_after_skill_region += 1

        source_tid = _safe_str(get(row, "Source Ticket ID"))
        if not source_tid:
            missing_ticket_id_count += 1
            continue
        province = _extract_province_from_owner(get(row, "Owner"))
        if province is None:
            # Explicit request: an Owner value that isn't a recognized
            # "-NOP" NOR province (a "-CORP" suffix, for example) is not
            # relevant work - excluded entirely, same rule as every
            # other tab now, not shown with a placeholder province.
            skipped_no_province += 1
            continue
        tid_upper = source_tid.upper()
        if tid_upper in seen_ticket_ids:
            duplicate_ticket_ids.add(tid_upper)
        seen_ticket_ids.add(tid_upper)

        m = mapping.get(tid_upper)
        bookmark = (m or {}).get("bookmark") or ""
        district = (m or {}).get("district") or ""
        filter_province = (m or {}).get("filter_province") or ""
        is_mapped = bool(m)
        if not bookmark:
            bookmark = NA_LABEL
        if not district:
            district = NA_LABEL
        if not filter_province:
            filter_province = NA_LABEL
        diag_bookmark_values[bookmark] = diag_bookmark_values.get(bookmark, 0) + 1
        severity = _safe_str(get(row, "Severity")) or NA_LABEL
        diag_severity_values[severity] = diag_severity_values.get(severity, 0) + 1
        status = _safe_str(get(row, "Status")) or NA_LABEL
        require_finish_time = _safe_str(get(row, "Require Finish Time"))
        finish_key = f"{status} (has Require Finish Time)" if require_finish_time else f"{status} (BLANK Require Finish Time)"
        diag_finish_time_by_status[finish_key] = diag_finish_time_by_status.get(finish_key, 0) + 1

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
            "severity": severity,
            "site_id": _safe_str(get(row, "Site ID")),
            "subject": _safe_str(get(row, "Subject")),
            "alarm_description": _safe_str(get(row, "Alarm Description")),
            "pause_resume_time": _safe_str(get(row, "Pause/Resume Time")),
            "remark": remarks.get(tid_upper, ""),
            "departed": _safe_str(get(row, "Departed")),
            "arrived": _safe_str(get(row, "Arrived")),
            "completed": _safe_str(get(row, "Completed")),
            "closed": _safe_str(get(row, "Closed")),
            "require_finish_time": require_finish_time,
            "status": status,
            "region": region,
            "province": province,  # already validated non-None above (TRUEOWNERGROUP-style, extracted from Daily's Owner)
            "filter_province": filter_province,  # from the mapping sheet's OWN Owner (column AX) - explicit request: drives the Province filter dropdown specifically
            "skill": skill,
            "bookmark": bookmark,
            "district": district,
            "is_mapped": is_mapped,
        })

    # unmapped_count, all_teams, and mapping_coverage below are all
    # data-completeness concerns ("is this province/team's OWS mapping
    # done at all"), not "active work in hand" concerns - so all three
    # are deliberately computed from the FULL Skill+Region scope, BEFORE
    # the Status filter a few lines down. Explicit bug this fixes: a
    # province or team whose every ticket happened to already be
    # Canceled/Closed (Auto)/Closed was vanishing entirely from both
    # Team Workload and OWS Mapping Coverage, instead of showing up
    # with 0 active tickets (idle team / fully-resolved province).
    unmapped_count = sum(1 for e in entries if not e["is_mapped"])
    all_teams = sorted({e["team"] for e in entries})
    coverage_by_province = {}
    for e in entries:
        d = coverage_by_province.setdefault(e["district"], {"total": 0, "mapped": 0})
        d["total"] += 1
        if e["is_mapped"]:
            d["mapped"] += 1
    mapping_coverage = [
        {"province": prov, "total": d["total"], "mapped": d["mapped"], "unmapped": d["total"] - d["mapped"],
         "pct": round(d["mapped"] / d["total"] * 100, 1) if d["total"] else 0}
        for prov, d in coverage_by_province.items()
    ]
    mapping_coverage.sort(key=lambda x: -x["total"])

    # Explicit request: exclude Canceled/Closed (Auto)/Closed entirely -
    # "ผมต้องการดูงานที่มีในมือจริงๆ" (only genuinely active work). This
    # is the actual `entries` list returned below; everything above
    # (unmapped_count, all_teams, mapping_coverage) already captured the
    # full picture before this filter runs.
    entries = [e for e in entries if e["status"] not in EXCLUDED_STATUSES]

    # Bookmark is no longer a hard server-side filter (explicit request:
    # "ขอปลด Filter ไม่กรองเฉพาะ FBB Online อย่างเดียว ขอเอาทุก Bookmark
    # เลยครับ") - `entries` below is now the FULL Skill+Region scope,
    # every Bookmark included. Each entry still carries its own
    # `bookmark` value so the frontend can filter/group by it if wanted.
    default_scope_entries = entries

    integrity = {
        "duplicate_ticket_ids": sorted(duplicate_ticket_ids),
        "duplicate_count": len(duplicate_ticket_ids),
        "missing_ticket_id_count": missing_ticket_id_count,
        "skipped_no_province": skipped_no_province,
        "total_ofc_nor_rows": len(entries),
        "unmapped_count": unmapped_count,
    }

    diagnostics = {
        "daily_sheet_headers_found": header,
        "daily_data_row_count": diag_total_daily_data_rows,
        "skill_value_counts": diag_skill_values,
        "region_value_counts": diag_region_values,
        "rows_after_skill_region_filter": diag_after_skill_region,
        "bookmark_value_counts_after_mapping": diag_bookmark_values,
        "severity_value_counts": diag_severity_values,
        "require_finish_time_presence_by_status": diag_finish_time_by_status,
        "mapping_sheet_headers_found": mapping_header,
        "mapping_sheet_entry_count": len(mapping),
        "mapping_sheet_sample": dict(list(mapping.items())[:3]),
        "mapping_filter_province_success_count": sum(1 for m in mapping.values() if m.get("filter_province")),
        "mapping_filter_province_blank_count": sum(1 for m in mapping.values() if not m.get("filter_province")),
        "looking_for_bookmark": DEFAULT_BOOKMARK,
        "looking_for_skill": DEFAULT_SKILL,
        "looking_for_regions": sorted(DEFAULT_REGIONS),
    }

    return {
        "entries": default_scope_entries,
        "all_skill_region_entries_count": len(entries),
        "all_teams": all_teams,
        "unmapped_total": unmapped_count,
        "mapping_coverage": mapping_coverage,
        "integrity": integrity,
        "diagnostics": diagnostics,
        "config": {
            "high_load_threshold": HIGH_LOAD_THRESHOLD,
            "critical_load_threshold": CRITICAL_LOAD_THRESHOLD,
            "default_skill": DEFAULT_SKILL,
            "default_regions": sorted(DEFAULT_REGIONS),
            "default_bookmark": DEFAULT_BOOKMARK,
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_insert_time": data_insert_time,
    }
