"""Oncall Team tab - Northern-region field-team roster with a clickable
Day Off / Oncall grid per date, one column per day. Persisted in its own
"OncallSchedule" tab in the same spreadsheet the rest of the app already
uses (REALTIME_SHEET_ID), so no new spreadsheet/credentials to manage.

Storage shape (one row per team member, one column per date):
    TeamType | Region | Province | ProvinceTH | Type | TeamID | Remark |
    No | TeamID1 | Name | Tel | FMOffice | TypeCM | <date1> | <date2> | ...

Cell values in the date columns: "off" (Day Off), "on" (Oncall - the
default), or "always" (7*24 - always oncall, not meant to be toggled;
carried over from the seed data as-is).
"""

import threading
import time

from realtime_monitor import REALTIME_SHEET_ID

ONCALL_SHEET = "OncallSchedule"
IDENTITY_HEADER = [
    "TeamType", "Region", "Province", "ProvinceTH", "Type", "TeamID",
    "Remark", "No", "TeamID1", "Name", "Tel", "FMOffice", "TypeCM",
]
IDENTITY_COLS = len(IDENTITY_HEADER)  # date columns start right after this

_CACHE_TTL_SECONDS = 20
_cache = {"data": None, "ts": 0}
_cache_lock = threading.Lock()


def _get_spreadsheet(gs_client):
    return gs_client.open_by_key(REALTIME_SHEET_ID)


def _get_oncall_ws(spreadsheet):
    try:
        return spreadsheet.worksheet(ONCALL_SHEET)
    except Exception:
        return None


def _invalidate_cache():
    with _cache_lock:
        _cache["data"] = None
        _cache["ts"] = 0


def seed_oncall_schedule(gs_client, dates, rows):
    """One-time bootstrap: creates the OncallSchedule tab (or replaces its
    content if it already exists) from `dates` (list of "YYYY-MM-DD"
    strings, one per column) and `rows` (list of dicts with the identity
    fields plus a "days" dict of {date: "on"/"off"/"always"}). Writes the
    header + every row in ONE batched call."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_oncall_ws(sh)
    header = IDENTITY_HEADER + dates
    if ws is None:
        ws = sh.add_worksheet(title=ONCALL_SHEET, rows=len(rows) + 10, cols=len(header) + 5)
    else:
        ws.clear()

    body = []
    for r in rows:
        line = [
            r.get("team_type", ""), r.get("region", ""), r.get("province", ""),
            r.get("province_th", ""), r.get("type", ""), r.get("team_id", ""),
            r.get("remark", ""), r.get("no", ""), r.get("team_id1", ""),
            r.get("name", ""), r.get("tel", ""), r.get("fm_office", ""), r.get("type_cm", ""),
        ]
        days = r.get("days", {})
        for d in dates:
            line.append(days.get(d, "on"))
        body.append(line)

    ws.update("A1", [header] + body, value_input_option="RAW")
    _invalidate_cache()
    return len(body)


def load_oncall_schedule(gs_client, use_cache=True):
    """Returns {"dates": [...], "rows": [ {...identity, "days": {date: status}} ]}
    or None if the tab hasn't been seeded yet."""
    now = time.monotonic()
    if use_cache:
        with _cache_lock:
            if _cache["data"] is not None and (now - _cache["ts"]) < _CACHE_TTL_SECONDS:
                return _cache["data"]

    sh = _get_spreadsheet(gs_client)
    ws = _get_oncall_ws(sh)
    if ws is None:
        return None
    values = ws.get_all_values()
    if not values:
        return None
    header = values[0]
    dates = header[IDENTITY_COLS:]

    rows = []
    for line in values[1:]:
        if not line or not line[0]:
            continue
        padded = line + [""] * (len(header) - len(line))
        days = {dates[i]: (padded[IDENTITY_COLS + i] or "on") for i in range(len(dates))}
        rows.append({
            "team_type": padded[0], "region": padded[1], "province": padded[2],
            "province_th": padded[3], "type": padded[4], "team_id": padded[5],
            "remark": padded[6], "no": padded[7], "team_id1": padded[8],
            "name": padded[9], "tel": padded[10], "fm_office": padded[11], "type_cm": padded[12],
            "days": days,
        })

    result = {"dates": dates, "rows": rows}
    if use_cache:
        with _cache_lock:
            _cache["data"] = result
            _cache["ts"] = now
    return result


def toggle_oncall_cell(gs_client, team_id1, date_str, new_status):
    """Sets one person's status for one date. new_status must be "on" or
    "off" (the two clickable states - "always"/7*24 rows are left alone by
    the UI, but nothing stops setting them here if ever needed)."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_oncall_ws(sh)
    if ws is None:
        raise ValueError("OncallSchedule tab has not been seeded yet")

    values = ws.get_all_values()
    header = values[0]
    dates = header[IDENTITY_COLS:]
    if date_str not in dates:
        raise ValueError(f"date {date_str} is not a column in OncallSchedule")
    col_idx = IDENTITY_COLS + dates.index(date_str) + 1  # 1-based

    row_idx = None
    for i, line in enumerate(values[1:], start=2):
        if line and line[8] == team_id1:  # column I = TeamID1 (0-based index 8)
            row_idx = i
            break
    if row_idx is None:
        raise ValueError(f"team_id1 {team_id1} not found in OncallSchedule")

    ws.update_cell(row_idx, col_idx, new_status)
    _invalidate_cache()
    return {"team_id1": team_id1, "date": date_str, "status": new_status}


def add_month_columns(gs_client, year_month):
    """Appends one column per day of `year_month` ("YYYY-MM") to the end of
    the sheet, defaulting every existing person's status to "on" for those
    new dates (they can then click to plan Day Offs ahead of time). Dates
    that already exist as columns are skipped. Returns how many columns
    were actually added."""
    import calendar
    from gspread.utils import rowcol_to_a1

    year, month = (int(x) for x in year_month.split("-"))
    _, days_in_month = calendar.monthrange(year, month)
    month_dates = [f"{year:04d}-{month:02d}-{d:02d}" for d in range(1, days_in_month + 1)]

    sh = _get_spreadsheet(gs_client)
    ws = _get_oncall_ws(sh)
    if ws is None:
        raise ValueError("OncallSchedule tab has not been seeded yet")

    values = ws.get_all_values()
    header = values[0]
    existing_dates = set(header[IDENTITY_COLS:])
    dates_to_add = [d for d in month_dates if d not in existing_dates]
    if not dates_to_add:
        return 0

    start_col = len(header) + 1  # 1-based, right after the current last column
    end_col = start_col + len(dates_to_add) - 1

    header_range = f"{rowcol_to_a1(1, start_col)}:{rowcol_to_a1(1, end_col)}"
    ws.update(header_range, [dates_to_add], value_input_option="RAW")

    n_data_rows = len(values) - 1
    if n_data_rows > 0:
        body_range = f"{rowcol_to_a1(2, start_col)}:{rowcol_to_a1(1 + n_data_rows, end_col)}"
        fill = [["on"] * len(dates_to_add) for _ in range(n_data_rows)]
        ws.update(body_range, fill, value_input_option="RAW")

    _invalidate_cache()
    return len(dates_to_add)
