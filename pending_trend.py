"""
pending_trend.py
-----------------
NOR2026 / BBTEC NOC — "Pending Ticket Trend" feature (Tap #1)

Nightly pipeline:
  1. Find the Google Drive backup file closest to 01:15 local time for a given date
     (files are named "Pending ticket_YYYY-MM-DD_HH-MM-SS.xlsx" and are produced hourly
     at :29-30 past the hour by an external process, in folder "Sheet_Backups").
  2. Download + parse it.
  3. Filter to Region in ALLOWED_REGIONS.
  4. Compute aggregates for 5 (severity_group, bookmark_filter) combinations x
     (aging breakdown, trueownergroup breakdown).
  5. Persist one compact JSON blob per date to a Google Sheet tab "PendingTrendDaily"
     (keeps the same "everything lives in Sheets" pattern as the rest of the stack,
     and is far cheaper than re-downloading/re-parsing 5MB xlsx files on every page view).

The API layer (app.py) only ever reads from PendingTrendDaily — it never touches
the raw Drive backups directly, so page loads stay fast regardless of history depth.

Integration notes for app.py:
  - Import: from pending_trend import run_nightly_job, load_trend_range, PENDING_TREND_SHEET
  - Add an APScheduler job (see bottom of this file for the trigger snippet).
  - Add route:  @app.route('/api/pending-trend') -> pending_trend.build_api_response(...)
  - Requires a Drive-scoped credential in addition to the existing Sheets-only one.
    Extend get_client()'s scopes to include 'https://www.googleapis.com/auth/drive.readonly'
    (the folder must be shared with the same service account used for GOOGLE_CREDENTIALS_JSON).
"""

import os
import io
import json
import time
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import openpyxl

from ticket_views import BOOKMARK_VIEWS, row_matches_view

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BANGKOK_TZ = ZoneInfo("Asia/Bangkok")

def bangkok_now():
    """Naive datetime representing the current moment in Thai local time.
    Railway's container clock runs in UTC, but the Drive backup filenames and
    all the 'today'/'this hour' reasoning in this module are inherently in
    Thai wall-clock time - using plain datetime.now() here would be off by
    the UTC offset (7 hours) and silently misidentify which file/day/hour is
    'now'. Returned as a naive datetime (tzinfo stripped) so it compares
    cleanly against the naive datetimes parsed from filenames."""
    return datetime.now(BANGKOK_TZ).replace(tzinfo=None)


DRIVE_FOLDER_ID = "188bv1FhdU2A64wjJjkntkKFIGZG18nMJ"   # "Sheet_Backups"
FILE_PREFIX = "Pending ticket_"
TARGET_TIME = dtime(1, 15)          # nightly snapshot target: ~01:15 local
SNAPSHOT_SEARCH_WINDOW_HOURS = 4    # look within +/- this window if 01:xx file is missing

PENDING_TREND_SHEET = "PendingTrendDaily"   # tab name in the main spreadsheet

ALLOWED_REGIONS = {"NOR1", "NOR2"}

SEVERITY_GROUPS = {
    "SA":     {"label": "SA1-SA4",   "values": {"SA1", "SA2", "SA3", "SA4"}},
    "NSA12":  {"label": "NSA1-NSA2", "values": {"NSA1", "NSA2"}},
    "NSA34":  {"label": "NSA3-NSA4", "values": {"NSA3", "NSA4"}},
}

# Only SA1-4 gets the extra Bookmark-filtered views (charts 4-9)
BOOKMARK_FILTERS = {
    "ALL": {"label": "All", "bookmark": None},
    "FBB": {"label": "Ticket Online (4.FBB with SA1-4)", "bookmark": "4.FBB with SA1-4"},
    "MB":  {"label": "Ticket Mobile (7.MB with SA1-4)",  "bookmark": "7.MB with SA1-4"},
}
GROUPS_WITH_BOOKMARK_SPLIT = {"SA"}   # NSA12 / NSA34 only need "ALL"

# Aging_Flag_Group canonical order + pastel colors.
# Stack draw order (bottom -> top) must be 1,2,3,4,5,6 so that Chart.js paints
# 6) Within SLA on top and 1) >30 days at the very bottom, matching the reference image.
AGING_ORDER = [
    "1) OverSLA : > 30 days",
    "2) OverSLA : < 30 days",
    "3) OverSLA : < 7 days",
    "4) OverSLA : < 3 days",
    "5) OverSLA : < 1 day",
    "6) Within SLA",
]
AGING_COLORS = {
    "1) OverSLA : > 30 days": "#8B2E2E",   # blood-red pastel
    "2) OverSLA : < 30 days": "#F2A6A6",   # light red pastel
    "3) OverSLA : < 7 days":  "#F7C99E",   # orange pastel
    "4) OverSLA : < 3 days":  "#F5E6A3",   # yellow pastel
    "5) OverSLA : < 1 day":   "#BFE3BF",   # light green pastel
    "6) Within SLA":          "#6FAE6F",   # dark green pastel
}
# "Over 24hr" = everything except 5) and 6)
OVER_24H_AGING_KEYS = [
    "1) OverSLA : > 30 days",
    "2) OverSLA : < 30 days",
    "3) OverSLA : < 7 days",
    "4) OverSLA : < 3 days",
]

REQUIRED_COLUMNS = ["Region", "SEVERITY", "TRUEOWNERGROUP", "Aging_Flag_Group", "Bookmark", "TICKETID", "TARGETFINISH"]


# ---------------------------------------------------------------------------
# Auth / Drive helpers
# ---------------------------------------------------------------------------

def get_drive_and_sheets_clients():
    """Single service-account credential, scoped for Sheets (read+write) and
    Drive (read+write). Was drive.readonly - just needed to list/download
    files from the Sheet_Backups folder - but creating a brand new Google
    Sheet (the on-demand Pending Ticket export button) goes through the
    Drive API too and needs write access to make a new file, so this is
    the full 'drive' scope now, not just 'drive.readonly'."""
    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gs_client = gspread.authorize(creds)
    drive_service = build("drive", "v3", credentials=creds)
    return drive_service, gs_client


def _parse_ts_from_filename(name):
    # "Pending ticket_2026-07-13_01-29-44.xlsx" -> datetime(2026,7,13,1,29,44)
    try:
        stem = name.replace(".xlsx", "")
        date_part, time_part = stem.replace(FILE_PREFIX, "").split("_")
        h, m, s = time_part.split("-")
        y, mo, d = date_part.split("-")
        return datetime(int(y), int(mo), int(d), int(h), int(m), int(s))
    except Exception:
        return None


def find_closest_file(drive_service, target_dt, window_minutes):
    """Return (file_id, matched_datetime, filename) for the backup file whose
    filename timestamp is closest to target_dt, searching +/- window_minutes.
    Returns None if nothing is found within the window."""
    window_start = target_dt - timedelta(minutes=window_minutes)
    window_end = target_dt + timedelta(minutes=window_minutes)

    query = f"'{DRIVE_FOLDER_ID}' in parents and name contains '{FILE_PREFIX}'"
    resp = drive_service.files().list(
        q=query,
        fields="files(id, name, createdTime)",
        pageSize=100,
        orderBy="createdTime desc",
    ).execute()

    best = None
    best_delta = None
    for f in resp.get("files", []):
        ts = _parse_ts_from_filename(f["name"])
        if ts is None or not (window_start <= ts <= window_end):
            continue
        delta = abs((ts - target_dt).total_seconds())
        if best_delta is None or delta < best_delta:
            best, best_delta = f, delta

    if best is None:
        log.warning("No Pending ticket snapshot found near %s (+/- %s min)", target_dt, window_minutes)
        return None
    return best["id"], _parse_ts_from_filename(best["name"]), best["name"]


def find_nightly_file(drive_service, target_date):
    """Return the backup file closest to TARGET_TIME (01:15) on target_date."""
    target_dt = datetime.combine(target_date, TARGET_TIME)
    return find_closest_file(drive_service, target_dt, window_minutes=SNAPSHOT_SEARCH_WINDOW_HOURS * 60)


def download_xlsx_as_rows(drive_service, file_id):
    """Download an xlsx from Drive and return a list of dict rows (header-keyed),
    using openpyxl in read-only/streaming mode to keep memory reasonable for ~5MB files.
    Periodically sleeps for a beat during the download and row-parse loops so a single
    gunicorn worker doesn't hold the GIL continuously for tens of seconds straight -
    without this, health checks / other requests can starve badly enough that the
    platform decides the process is unresponsive and restarts it mid-job."""
    request = drive_service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
        time.sleep(0.01)  # yield between chunks
    buf.seek(0)

    wb = openpyxl.load_workbook(buf, read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    header = [str(h).strip() if h is not None else "" for h in next(rows_iter)]

    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ValueError(f"Snapshot file missing expected columns: {missing}")

    idx = {name: header.index(name) for name in header}
    rows = []
    for r in rows_iter:
        if r is None or all(v is None for v in r):
            continue
        rows.append({name: r[i] if i < len(r) else None for name, i in idx.items()})
        if len(rows) % 4000 == 0:
            time.sleep(0.02)  # yield periodically during the row-parse loop
    wb.close()
    return rows


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _empty_bucket():
    return {
        "aging_counts": {k: 0 for k in AGING_ORDER},
        "trueowner_counts": {},     # {TRUEOWNERGROUP: total_count}
        "trueowner_region": {},     # {TRUEOWNERGROUP: region} - lets the frontend filter chart 2 by region
        "region_breakdown": {},     # {region: {aging_counts, total, actual_over_sla}} - lets charts 1/3 be filtered by region
        "total": 0,
        "actual_over_sla": 0,     # sum of aging groups 1-4
    }


def compute_repeat_ticket_counts(rows):
    """Per BOOKMARK_VIEWS group, collect one row per distinct TICKETID seen for
    each CINAME in this single snapshot (region-filtered, deduped within the
    snapshot). Returned as a FLAT list rather than a nested dict, because a
    single day's worth of tickets across all 4 views turned out to be far
    more than fits in one 50,000-character Sheets cell as JSON - many small
    rows in a dedicated sheet has no such limit. Returns:
    [{"view", "ciname", "ticket_id", "trueowner", "subject", "creationdate", "severity"}, ...]
    """
    filtered = [r for r in rows if str(r.get("Region", "")).strip() in ALLOWED_REGIONS]
    out = []
    seen_ticket_ids = {vk: set() for vk in BOOKMARK_VIEWS}

    for row_idx, r in enumerate(filtered):
        if row_idx % 8000 == 0:
            time.sleep(0.02)
        cin = str(r.get("CINAME", "")).strip()
        if not cin:
            continue
        ticket_id = str(r.get("TICKETID", "")).strip()
        owner = str(r.get("TRUEOWNERGROUP", "")).strip()
        subject = str(r.get("SUBJECT", "")).strip()
        creationdate = str(r.get("CREATIONDATE", "")).strip()
        severity = str(r.get("SEVERITY", "")).strip()
        for vk in BOOKMARK_VIEWS:
            if row_matches_view(r, vk):
                if ticket_id:
                    if ticket_id in seen_ticket_ids[vk]:
                        continue  # already recorded this ticket for this snapshot
                    seen_ticket_ids[vk].add(ticket_id)
                out.append({
                    "view": vk, "ciname": cin, "ticket_id": ticket_id, "trueowner": owner,
                    "subject": subject, "creationdate": creationdate, "severity": severity,
                })

    return out


def compute_daily_aggregate(rows):
    """rows: list of dict rows from download_xlsx_as_rows (already includes ALL regions).
    Returns nested dict: {group_key: {filter_key: bucket}}"""

    # Pre-filter to allowed regions once.
    filtered = [r for r in rows if str(r.get("Region", "")).strip() in ALLOWED_REGIONS]

    result = {}
    for group_key, group_def in SEVERITY_GROUPS.items():
        sev_values = group_def["values"]
        filter_keys = ["ALL"] + (["FBB", "MB"] if group_key in GROUPS_WITH_BOOKMARK_SPLIT else [])
        result[group_key] = {fk: _empty_bucket() for fk in filter_keys}

        for row_idx, r in enumerate(filtered):
            sev = str(r.get("SEVERITY", "")).strip()
            if sev not in sev_values:
                continue
            aging = str(r.get("Aging_Flag_Group", "")).strip()
            if aging not in AGING_ORDER:
                continue
            owner = str(r.get("TRUEOWNERGROUP", "")).strip() or "UNKNOWN"
            bookmark = str(r.get("Bookmark", "")).strip()
            region = str(r.get("Region", "")).strip()

            if row_idx % 8000 == 0:
                time.sleep(0.02)  # yield periodically - this loop runs once per severity group

            targets = ["ALL"]
            if group_key in GROUPS_WITH_BOOKMARK_SPLIT:
                if bookmark == BOOKMARK_FILTERS["FBB"]["bookmark"]:
                    targets.append("FBB")
                elif bookmark == BOOKMARK_FILTERS["MB"]["bookmark"]:
                    targets.append("MB")

            for fk in targets:
                bucket = result[group_key][fk]
                bucket["aging_counts"][aging] += 1
                bucket["trueowner_counts"][owner] = bucket["trueowner_counts"].get(owner, 0) + 1
                bucket["trueowner_region"][owner] = region
                bucket["total"] += 1
                if aging in OVER_24H_AGING_KEYS:
                    bucket["actual_over_sla"] += 1

                rb = bucket["region_breakdown"].setdefault(
                    region, {"aging_counts": {k: 0 for k in AGING_ORDER}, "total": 0, "actual_over_sla": 0}
                )
                rb["aging_counts"][aging] += 1
                rb["total"] += 1
                if aging in OVER_24H_AGING_KEYS:
                    rb["actual_over_sla"] += 1

    return result


# ---------------------------------------------------------------------------
# Persistence (Google Sheet: one row per key [date or hour], one JSON blob column)
# ---------------------------------------------------------------------------

HOURLY_TREND_SHEET = "PendingTrendHourly"

def _ensure_sheet_tab_named(spreadsheet, sheet_name):
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=3)
        ws.append_row(["key", "source_file", "data_json"])
        return ws


def save_aggregate(gs_client, spreadsheet_id, key_str, source_file, aggregate, sheet_name):
    sh = gs_client.open_by_key(spreadsheet_id)
    ws = _ensure_sheet_tab_named(sh, sheet_name)

    existing = ws.col_values(1)  # key column
    payload = json.dumps(aggregate, ensure_ascii=False, separators=(",", ":"))

    if len(payload) > 49000:
        log.warning(
            "save_aggregate: %s payload for %s is %d chars - close to or over "
            "the Sheets 50,000-char cell limit and may fail to save",
            sheet_name, key_str, len(payload),
        )

    if key_str in existing:
        row_idx = existing.index(key_str) + 1
        ws.update(f"A{row_idx}:C{row_idx}", [[key_str, source_file, payload]])
        log.info("Updated %s row for %s (%d chars)", sheet_name, key_str, len(payload))
    else:
        ws.append_row([key_str, source_file, payload])
        log.info("Appended %s row for %s (%d chars)", sheet_name, key_str, len(payload))


def load_range_named(gs_client, spreadsheet_id, sheet_name, n):
    """Return list of {date, data} dicts for the most recent `n` keys that have
    a saved aggregate, oldest first. Keeps the 'date' field name for backward
    compatibility with build_api_response even when the key is actually an hour."""
    sh = gs_client.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return []

    records = ws.get_all_values()[1:]  # skip header
    parsed = []
    for row in records:
        if len(row) < 3 or not row[0]:
            continue
        try:
            parsed.append({"date": row[0], "data": json.loads(row[2])})
        except (json.JSONDecodeError, IndexError):
            continue

    parsed.sort(key=lambda x: x["date"])
    return parsed[-n:]


REPEAT_TICKET_DETAIL_SHEET = "PendingTrendRepeatDetail"
REPEAT_TICKET_DETAIL_HOURLY_SHEET = "PendingTrendRepeatDetailHourly"
REPEAT_DETAIL_HEADER = ["key", "view", "ciname", "ticket_id", "trueowner", "subject", "creationdate", "severity"]


def _ensure_repeat_detail_tab(spreadsheet, sheet_name):
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(REPEAT_DETAIL_HEADER))
        ws.append_row(REPEAT_DETAIL_HEADER)
        return ws


def save_repeat_ticket_detail(gs_client, spreadsheet_id, key_str, detail_rows, sheet_name=REPEAT_TICKET_DETAIL_SHEET):
    """detail_rows: flat list from compute_repeat_ticket_counts(). key_str is a
    date ('YYYY-MM-DD') for the daily sheet or an hour ('YYYY-MM-DDTHH:00') for
    the hourly one. Appends one small row per ticket - no JSON blob, no
    per-cell size limit to worry about. Always appends (doesn't overwrite
    same-key rows) - re-running backfill for a key that was already saved may
    create duplicate rows, but that's harmless since the response builders
    dedupe by TICKETID at read time anyway."""
    sh = gs_client.open_by_key(spreadsheet_id)
    ws = _ensure_repeat_detail_tab(sh, sheet_name)
    if not detail_rows:
        return
    rows = [
        [key_str, d["view"], d["ciname"], d["ticket_id"], d["trueowner"],
         d.get("subject", ""), d.get("creationdate", ""), d.get("severity", "")]
        for d in detail_rows
    ]
    ws.append_rows(rows, value_input_option="RAW")
    log.info("Appended %d repeat-ticket detail rows to %s for %s", len(rows), sheet_name, key_str)


def _load_repeat_ticket_rows(gs_client, spreadsheet_id, sheet_name, view_key, since_key_str):
    """Shared reader: filters the flat detail sheet to view_key with
    key >= since_key_str (ISO date/hour strings sort correctly as plain
    strings). Returns the raw matching rows as dicts."""
    sh = gs_client.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return []

    records = ws.get_all_values()[1:]  # skip header
    out = []
    for row in records:
        if len(row) < 5:
            continue
        key_str, view, cin, ticket_id, owner = row[0], row[1], row[2], row[3], row[4]
        subject = row[5] if len(row) > 5 else ""
        creationdate = row[6] if len(row) > 6 else ""
        severity = row[7] if len(row) > 7 else ""
        if not key_str or key_str < since_key_str or view != view_key:
            continue
        out.append({
            "key": key_str, "ciname": cin, "ticket_id": ticket_id, "trueowner": owner,
            "subject": subject, "creationdate": creationdate, "severity": severity,
        })
    return out


def load_repeat_ticket_detail(gs_client, spreadsheet_id, view_key, since_date_str):
    """Returns {"ticket_sets": {cin: set(ticket_id,...)}, "trueowner_of": {cin: owner},
    "dates_seen": set(date_str,...)}."""
    rows = _load_repeat_ticket_rows(gs_client, spreadsheet_id, REPEAT_TICKET_DETAIL_SHEET, view_key, since_date_str)
    ticket_sets = {}
    trueowner_of = {}
    dates_seen = set()
    for r in rows:
        dates_seen.add(r["key"])
        if r["ticket_id"]:
            ticket_sets.setdefault(r["ciname"], set()).add(r["ticket_id"])
        if r["trueowner"]:
            trueowner_of[r["ciname"]] = r["trueowner"]
    return {"ticket_sets": ticket_sets, "trueowner_of": trueowner_of, "dates_seen": dates_seen}


def load_repeat_ticket_tickets_for_ciname(gs_client, spreadsheet_id, view_key, ciname, since_date_str):
    """For the CINAME detail popup: distinct tickets (with subject/creationdate/
    severity) seen for one CINAME within the date range, newest first."""
    rows = _load_repeat_ticket_rows(gs_client, spreadsheet_id, REPEAT_TICKET_DETAIL_SHEET, view_key, since_date_str)
    seen = {}
    for r in rows:
        if r["ciname"] != ciname or not r["ticket_id"]:
            continue
        seen[r["ticket_id"]] = {
            "ticket_id": r["ticket_id"], "subject": r["subject"],
            "creationdate": r["creationdate"], "severity": r["severity"],
        }
    tickets = list(seen.values())
    tickets.sort(key=lambda t: t["creationdate"], reverse=True)
    return tickets


def build_repeat_ticket_hourly_trend(gs_client, spreadsheet_id, view_key, hours=168):
    """For the given view, walks the hourly repeat-detail sheet chronologically
    and counts, per hour, how many TICKETIDs are appearing for the FIRST TIME
    within the loaded window - i.e. genuinely new tickets that hour (as opposed
    to the same ticket still being pending from an earlier hour). Returns a
    per-hour series plus a total, so the frontend can chart "new tickets/hour"
    over the last N hours (default 7 days = 168 hours)."""
    if view_key not in BOOKMARK_VIEWS:
        raise ValueError(f"Unknown view_key {view_key!r}")

    since_hour_str = (bangkok_now() - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:00")
    rows = _load_repeat_ticket_rows(
        gs_client, spreadsheet_id, REPEAT_TICKET_DETAIL_HOURLY_SHEET, view_key, since_hour_str
    )
    rows.sort(key=lambda r: r["key"])  # chronological

    seen_ever = set()
    per_hour_new = {}   # {hour_key: count}
    per_hour_cin_new = {}  # {hour_key: {ciname: count}} - for drill-down if needed later
    for r in rows:
        hk, tid, cin = r["key"], r["ticket_id"], r["ciname"]
        if not tid or tid in seen_ever:
            continue
        seen_ever.add(tid)
        per_hour_new[hk] = per_hour_new.get(hk, 0) + 1
        per_hour_cin_new.setdefault(hk, {})
        per_hour_cin_new[hk][cin] = per_hour_cin_new[hk].get(cin, 0) + 1

    hour_keys = sorted(per_hour_new.keys())
    return {
        "view": view_key,
        "view_label": BOOKMARK_VIEWS[view_key]["label"],
        "hours_requested": hours,
        "hours_covered": len(hour_keys),
        "hours": hour_keys,
        "new_ticket_counts": [per_hour_new[h] for h in hour_keys],
        "total_new": sum(per_hour_new.values()),
    }


def save_daily_aggregate(gs_client, spreadsheet_id, date_str, source_file, aggregate):
    save_aggregate(gs_client, spreadsheet_id, date_str, source_file, aggregate, PENDING_TREND_SHEET)


def load_trend_range(gs_client, spreadsheet_id, days=14):
    return load_range_named(gs_client, spreadsheet_id, PENDING_TREND_SHEET, days)


# ---------------------------------------------------------------------------
# Nightly (daily) + hourly job entrypoints (wire into APScheduler in app.py)
# ---------------------------------------------------------------------------

def run_nightly_job(spreadsheet_id, for_date=None):
    """for_date defaults to 'today' in whatever timezone the scheduler is running in.
    Call this once per night, e.g. via APScheduler cron trigger at 01:35."""
    for_date = for_date or bangkok_now().date()
    drive_service, gs_client = get_drive_and_sheets_clients()

    found = find_nightly_file(drive_service, for_date)
    if found is None:
        log.error("run_nightly_job: no snapshot available for %s, skipping", for_date)
        return False

    file_id, matched_dt, filename = found
    log.info("Using snapshot %s (matched %s) for date %s", filename, matched_dt, for_date)

    rows = download_xlsx_as_rows(drive_service, file_id)
    aggregate = compute_daily_aggregate(rows)
    save_daily_aggregate(gs_client, spreadsheet_id, for_date.isoformat(), filename, aggregate)

    # NOTE: repeat-ticket tracking (Repeat Ticket tab) was retired - it read
    # the whole PendingTrendRepeatDetail/Hourly sheets on every page load,
    # which only grew (never pruned) and got slower every day. Left the
    # compute/save functions below in place but no longer call them here.

    return True


def run_hourly_job(spreadsheet_id, for_hour=None):
    """for_hour defaults to the current hour (truncated to :00) and identifies which
    hourly BUCKET to store the result under. The actual Drive search target is
    for_hour + 29 min, because backup files are created near :29 past each hour
    (same pattern as the nightly job) - searching around :00 would miss them
    entirely. Stores the aggregate keyed by 'YYYY-MM-DDTHH:00' in PendingTrendHourly.
    Call this once per hour via APScheduler, or repeatedly for backfill."""
    for_hour = for_hour or bangkok_now().replace(minute=0, second=0, microsecond=0)
    search_target = for_hour.replace(minute=29)
    drive_service, gs_client = get_drive_and_sheets_clients()

    found = find_closest_file(drive_service, search_target, window_minutes=25)
    if found is None:
        log.error("run_hourly_job: no snapshot available near %s, skipping", search_target)
        return False

    file_id, matched_dt, filename = found
    log.info("Using snapshot %s (matched %s) for hour bucket %s", filename, matched_dt, for_hour)

    rows = download_xlsx_as_rows(drive_service, file_id)
    aggregate = compute_daily_aggregate(rows)  # same generic aggregator works for any snapshot
    key_str = for_hour.strftime("%Y-%m-%dT%H:00")
    save_aggregate(gs_client, spreadsheet_id, key_str, filename, aggregate, HOURLY_TREND_SHEET)

    # NOTE: repeat-ticket hourly tracking (Repeat Ticket tab) was retired -
    # see the matching note in run_nightly_job.

    return True


# ---------------------------------------------------------------------------
# API response shaping (consumed directly by the frontend Chart.js code)
# ---------------------------------------------------------------------------

PERIOD_DAYS = {"7d": 7, "14d": 14, "21d": 21, "1m": 30}


def _build_response_from_rows(rows, label_key="dates", extra_meta=None, trueowner_filter=None, region_filter=None):
    """Core response shaper - works identically for daily rows (label = date string)
    and hourly rows (label = 'YYYY-MM-DDTHH:00' string); the frontend just formats
    the label differently depending on which endpoint it called."""
    labels = [r["date"] for r in rows]
    response = {label_key: labels, "regions": sorted(ALLOWED_REGIONS), "groups": {}}
    if extra_meta:
        response.update(extra_meta)

    for group_key, group_def in SEVERITY_GROUPS.items():
        filter_keys = ["ALL"] + (["FBB", "MB"] if group_key in GROUPS_WITH_BOOKMARK_SPLIT else [])
        response["groups"][group_key] = {"label": group_def["label"], "filters": {}}

        for fk in filter_keys:
            aging_series = {k: [] for k in AGING_ORDER}
            total_series = []
            over_sla_series = []
            trueowner_series = {}   # {owner: [count per label, aligned to `labels`]}

            for idx, r in enumerate(rows):
                bucket = r["data"].get(group_key, {}).get(fk)
                if bucket is None:
                    for k in AGING_ORDER:
                        aging_series[k].append(0)
                    total_series.append(0)
                    over_sla_series.append(0)
                    continue

                if region_filter:
                    row_aging = {k: 0 for k in AGING_ORDER}
                    row_total = 0
                    row_over_sla = 0
                    region_breakdown = bucket.get("region_breakdown", {})
                    for reg in region_filter:
                        rb = region_breakdown.get(reg)
                        if not rb:
                            continue
                        for k in AGING_ORDER:
                            row_aging[k] += rb["aging_counts"].get(k, 0)
                        row_total += rb["total"]
                        row_over_sla += rb["actual_over_sla"]
                    for k in AGING_ORDER:
                        aging_series[k].append(row_aging[k])
                    total_series.append(row_total)
                    over_sla_series.append(row_over_sla)
                else:
                    for k in AGING_ORDER:
                        aging_series[k].append(bucket["aging_counts"].get(k, 0))
                    total_series.append(bucket["total"])
                    over_sla_series.append(bucket["actual_over_sla"])

                owner_region = bucket.get("trueowner_region", {})
                for owner, cnt in bucket["trueowner_counts"].items():
                    if trueowner_filter and owner != trueowner_filter:
                        continue
                    if region_filter and owner_region.get(owner) not in region_filter:
                        continue
                    trueowner_series.setdefault(owner, [0] * len(labels))
                    trueowner_series[owner][idx] = cnt

            response["groups"][group_key]["filters"][fk] = {
                "label": BOOKMARK_FILTERS[fk]["label"],
                "aging_series": aging_series,          # stack chart + summary table
                "aging_order": AGING_ORDER,
                "aging_colors": AGING_COLORS,
                "total_series": total_series,          # blue "Total Pending /day" line
                "over_sla_series": over_sla_series,    # gold "Actual Pending Over SLA" line
                "trueowner_series": trueowner_series,  # chart 2: multi-line by province
            }

    return response


def build_api_response(gs_client, spreadsheet_id, period="14d", trueowner_filter=None, region_filter=None):
    """
    trueowner_filter: optional single TRUEOWNERGROUP string - narrows chart 2's
    per-province line series to that one province.
    region_filter: optional list of region strings (subset of ALLOWED_REGIONS).
    When provided, charts 1/3 (stack + over-SLA) and the summary table are
    recomputed from the stored per-region breakdown instead of the full
    4-region total, and chart 2's province lines are limited to provinces
    that belong to one of the selected regions. None/empty = all 4 regions
    combined (original behaviour).
    """
    days = PERIOD_DAYS.get(period, 14)
    rows = load_trend_range(gs_client, spreadsheet_id, days=days)
    return _build_response_from_rows(
        rows, label_key="dates", extra_meta={"period": period},
        trueowner_filter=trueowner_filter, region_filter=region_filter,
    )


def build_hourly_api_response(gs_client, spreadsheet_id, hours=24, trueowner_filter=None, region_filter=None):
    """Same shape as build_api_response, but each entry in `dates` is an
    'YYYY-MM-DDTHH:00' hour key instead of a day, sourced from PendingTrendHourly."""
    rows = load_range_named(gs_client, spreadsheet_id, HOURLY_TREND_SHEET, hours)
    return _build_response_from_rows(
        rows, label_key="dates", extra_meta={"hours": hours},
        trueowner_filter=trueowner_filter, region_filter=region_filter,
    )


def build_repeat_ticket_response(gs_client, spreadsheet_id, view_key, days=30, top_n=50):
    """For the given BOOKMARK_VIEWS key, unions the distinct TICKETIDs seen per
    CINAME across the last `days` days (reading the flat PendingTrendRepeatDetail
    sheet), then ranks CINAMEs by that distinct count (top_n). A ticket pending
    across many days only counts once per CINAME - this is a global, cross-day
    dedup, not a per-day one. Days with no stored rows are silently absent from
    days_covered (e.g. before the feature was deployed, or a missed backfill)."""
    if view_key not in BOOKMARK_VIEWS:
        raise ValueError(f"Unknown view_key {view_key!r}")

    since_date_str = (bangkok_now().date() - timedelta(days=days)).isoformat()
    loaded = load_repeat_ticket_detail(gs_client, spreadsheet_id, view_key, since_date_str)
    ticket_sets = loaded["ticket_sets"]
    trueowner_of = loaded["trueowner_of"]
    dates_seen = loaded["dates_seen"]

    totals = {cin: len(ids) for cin, ids in ticket_sets.items()}
    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    sorted_dates = sorted(dates_seen)
    return {
        "view": view_key,
        "view_label": BOOKMARK_VIEWS[view_key]["label"],
        "days_requested": days,
        "days_covered": len(dates_seen),
        "dates_range": {"from": sorted_dates[0] if sorted_dates else None, "to": sorted_dates[-1] if sorted_dates else None},
        "ranking": [
            {"rank": i + 1, "ciname": cin, "count": cnt, "trueowner": trueowner_of.get(cin)}
            for i, (cin, cnt) in enumerate(ranked)
        ],
    }


# Example APScheduler wiring for app.py:
#
#   from pending_trend import run_nightly_job, run_hourly_job
#   scheduler.add_job(lambda: run_nightly_job(SHEET_ID), 'cron', hour=1, minute=35, id='pending_trend_nightly')
#   scheduler.add_job(lambda: run_hourly_job(SHEET_ID), 'cron', minute=35, id='pending_trend_hourly')
