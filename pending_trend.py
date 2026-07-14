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
import logging
from datetime import datetime, timedelta, time as dtime

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import openpyxl

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DRIVE_FOLDER_ID = "188bv1FhdU2A64wjJjkntkKFIGZG18nMJ"   # "Sheet_Backups"
FILE_PREFIX = "Pending ticket_"
TARGET_TIME = dtime(1, 15)          # nightly snapshot target: ~01:15 local
SNAPSHOT_SEARCH_WINDOW_HOURS = 4    # look within +/- this window if 01:xx file is missing

PENDING_TREND_SHEET = "PendingTrendDaily"   # tab name in the main spreadsheet

ALLOWED_REGIONS = {"NOE1", "NOE", "NOR1", "NOR2"}

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

REQUIRED_COLUMNS = ["Region", "SEVERITY", "TRUEOWNERGROUP", "Aging_Flag_Group", "Bookmark", "TICKETID"]


# ---------------------------------------------------------------------------
# Auth / Drive helpers
# ---------------------------------------------------------------------------

def get_drive_and_sheets_clients():
    """Single service-account credential, scoped for both Sheets (read) and Drive (read).
    NOTE: the existing get_client() in app.py only requests the spreadsheets.readonly
    scope - extend it (or use this standalone function) so the same service account
    can also list/download files from the Sheet_Backups folder."""
    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
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


def find_nightly_file(drive_service, target_date):
    """Return (file_id, matched_datetime, filename) for the backup file whose
    filename timestamp is closest to TARGET_TIME on target_date. Returns None
    if nothing is found within the search window."""
    target_dt = datetime.combine(target_date, TARGET_TIME)
    window_start = target_dt - timedelta(hours=SNAPSHOT_SEARCH_WINDOW_HOURS)
    window_end = target_dt + timedelta(hours=SNAPSHOT_SEARCH_WINDOW_HOURS)

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
        log.warning("No Pending ticket snapshot found near %s for %s", TARGET_TIME, target_date)
        return None
    return best["id"], _parse_ts_from_filename(best["name"]), best["name"]


def download_xlsx_as_rows(drive_service, file_id):
    """Download an xlsx from Drive and return a list of dict rows (header-keyed),
    using openpyxl in read-only/streaming mode to keep memory reasonable for ~5MB files."""
    request = drive_service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
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

        for r in filtered:
            sev = str(r.get("SEVERITY", "")).strip()
            if sev not in sev_values:
                continue
            aging = str(r.get("Aging_Flag_Group", "")).strip()
            if aging not in AGING_ORDER:
                continue
            owner = str(r.get("TRUEOWNERGROUP", "")).strip() or "UNKNOWN"
            bookmark = str(r.get("Bookmark", "")).strip()
            region = str(r.get("Region", "")).strip()

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
# Persistence (Google Sheet: one row per date, one JSON blob column)
# ---------------------------------------------------------------------------

def _ensure_sheet_tab(spreadsheet):
    try:
        return spreadsheet.worksheet(PENDING_TREND_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=PENDING_TREND_SHEET, rows=1000, cols=3)
        ws.append_row(["date", "source_file", "data_json"])
        return ws


def save_daily_aggregate(gs_client, spreadsheet_id, date_str, source_file, aggregate):
    sh = gs_client.open_by_key(spreadsheet_id)
    ws = _ensure_sheet_tab(sh)

    existing = ws.col_values(1)  # date column
    payload = json.dumps(aggregate, ensure_ascii=False, separators=(",", ":"))

    if date_str in existing:
        row_idx = existing.index(date_str) + 1
        ws.update(f"A{row_idx}:C{row_idx}", [[date_str, source_file, payload]])
        log.info("Updated PendingTrendDaily row for %s", date_str)
    else:
        ws.append_row([date_str, source_file, payload])
        log.info("Appended PendingTrendDaily row for %s", date_str)


def load_trend_range(gs_client, spreadsheet_id, days=14):
    """Return list of {date, data} dicts for the most recent `days` calendar dates
    that have a saved aggregate, oldest first."""
    sh = gs_client.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(PENDING_TREND_SHEET)
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
    return parsed[-days:]


# ---------------------------------------------------------------------------
# Nightly job entrypoint (wire this into APScheduler in app.py)
# ---------------------------------------------------------------------------

def run_nightly_job(spreadsheet_id, for_date=None):
    """for_date defaults to 'today' in whatever timezone the scheduler is running in.
    Call this once per night, e.g. via APScheduler cron trigger at 01:35."""
    for_date = for_date or datetime.now().date()
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
    return True


# ---------------------------------------------------------------------------
# API response shaping (consumed directly by the frontend Chart.js code)
# ---------------------------------------------------------------------------

PERIOD_DAYS = {"7d": 7, "14d": 14, "21d": 21, "1m": 30}


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

    dates = [r["date"] for r in rows]
    response = {"dates": dates, "period": period, "regions": sorted(ALLOWED_REGIONS), "groups": {}}

    for group_key, group_def in SEVERITY_GROUPS.items():
        filter_keys = ["ALL"] + (["FBB", "MB"] if group_key in GROUPS_WITH_BOOKMARK_SPLIT else [])
        response["groups"][group_key] = {"label": group_def["label"], "filters": {}}

        for fk in filter_keys:
            aging_series = {k: [] for k in AGING_ORDER}
            total_series = []
            over_sla_series = []
            trueowner_series = {}   # {owner: [count per day, aligned to `dates`]}

            for day_idx, r in enumerate(rows):
                bucket = r["data"].get(group_key, {}).get(fk)
                if bucket is None:
                    for k in AGING_ORDER:
                        aging_series[k].append(0)
                    total_series.append(0)
                    over_sla_series.append(0)
                    continue

                if region_filter:
                    day_aging = {k: 0 for k in AGING_ORDER}
                    day_total = 0
                    day_over_sla = 0
                    region_breakdown = bucket.get("region_breakdown", {})
                    for reg in region_filter:
                        rb = region_breakdown.get(reg)
                        if not rb:
                            continue
                        for k in AGING_ORDER:
                            day_aging[k] += rb["aging_counts"].get(k, 0)
                        day_total += rb["total"]
                        day_over_sla += rb["actual_over_sla"]
                    for k in AGING_ORDER:
                        aging_series[k].append(day_aging[k])
                    total_series.append(day_total)
                    over_sla_series.append(day_over_sla)
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
                    trueowner_series.setdefault(owner, [0] * len(dates))
                    trueowner_series[owner][day_idx] = cnt

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


# Example APScheduler wiring for app.py:
#
#   from pending_trend import run_nightly_job
#   scheduler.add_job(
#       lambda: run_nightly_job(SHEET_ID),
#       'cron', hour=1, minute=35, id='pending_trend_nightly'
#   )
