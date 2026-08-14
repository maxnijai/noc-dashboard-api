"""Oncall Team tab - Northern-region field-team roster with a clickable
Day Off / Oncall grid per date, one column per day. Persisted in its own
"OncallSchedule" tab in the same spreadsheet the rest of the app already
uses (REALTIME_SHEET_ID), so no new spreadsheet/credentials to manage.

Storage shape (one row per team member, one column per date):
    TeamType | Region | Province | ProvinceTH | Type | TeamID | Remark |
    No | TeamID1 | Name | Tel | FMOffice | TypeCM | LastUpdatedBy |
    LastUpdatedAt | <date1> | <date2> | ...

LastUpdatedBy/LastUpdatedAt track the most recent toggle on ANY date for
that person (not per-cell - one row's most recent edit), stamped from the
session the same way Pending Ticket's work log does. Dates always start
right after these two fixed columns and new months are appended at the
true end of the sheet, so the tracking columns never need to move.

Cell values in the date columns: "" or "blank" (unset - nobody has picked
this person as Oncall for that day; the default, not counted anywhere),
"on" (explicitly marked Oncall - counted), "off" (Day Off - not counted),
or "always" (7*24 - always oncall, not meant to be toggled; carried over
from the seed data as-is, counted).
"""

import threading
import time

from realtime_monitor import REALTIME_SHEET_ID
from pending_trend import bangkok_now

ONCALL_SHEET = "OncallSchedule"
IDENTITY_HEADER = [
    "TeamType", "Region", "Province", "ProvinceTH", "Type", "TeamID",
    "Remark", "No", "TeamID1", "Name", "Tel", "FMOffice", "TypeCM",
    "LastUpdatedBy", "LastUpdatedAt",
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
            "", "",  # LastUpdatedBy, LastUpdatedAt - blank until first toggle
        ]
        days = r.get("days", {})
        for d in dates:
            line.append(days.get(d, "blank"))
        body.append(line)

    ws.update("A1", [header] + body, value_input_option="RAW")
    _invalidate_cache()
    _invalidate_layout_cache()
    return len(body)


def load_oncall_schedule(gs_client, use_cache=True):
    """Returns {"dates": [...], "rows": [ {...identity, "last_updated_by",
    "last_updated_at", "days": {date: status}} ]} or None if the tab hasn't
    been seeded yet."""
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
        days = {dates[i]: (padded[IDENTITY_COLS + i] or "blank") for i in range(len(dates))}
        rows.append({
            "team_type": padded[0], "region": padded[1], "province": padded[2],
            "province_th": padded[3], "type": padded[4], "team_id": padded[5],
            "remark": padded[6], "no": padded[7], "team_id1": padded[8],
            "name": padded[9], "tel": padded[10], "fm_office": padded[11], "type_cm": padded[12],
            "last_updated_by": padded[13], "last_updated_at": padded[14],
            "days": days,
        })

    result = {"dates": dates, "rows": rows}
    if use_cache:
        with _cache_lock:
            _cache["data"] = result
            _cache["ts"] = now
    return result


# Layout cache (header/dates + team_id1 -> row index) - separate from and
# much longer-lived than the data cache above, since the SHAPE of the
# sheet (who's in it, which columns exist) changes only on seed/add-month,
# not on every toggle. Without this, every single click was doing a full
# ws.get_all_values() read (1 API call) plus two separate writes (2 more) -
# fine occasionally, but once Oncall became something people click for
# every person on every day instead of a pre-filled default, dozens of
# rapid clicks were blowing through the Sheets API per-minute quota and
# the UI would stall waiting on 429s. Caching the layout cuts a toggle
# down to a single batched write call after the first hit.
_LAYOUT_CACHE_TTL_SECONDS = 300
_layout_cache = {"dates": None, "team_row": None, "ts": 0}


def _invalidate_layout_cache():
    with _cache_lock:
        _layout_cache["dates"] = None
        _layout_cache["team_row"] = None
        _layout_cache["ts"] = 0


def _get_layout(ws):
    now = time.monotonic()
    with _cache_lock:
        if _layout_cache["dates"] is not None and (now - _layout_cache["ts"]) < _LAYOUT_CACHE_TTL_SECONDS:
            return _layout_cache["dates"], _layout_cache["team_row"]

    values = ws.get_all_values()
    header = values[0]
    dates = header[IDENTITY_COLS:]
    team_row = {}
    for i, line in enumerate(values[1:], start=2):
        if line and len(line) > 8 and line[8]:
            team_row[line[8]] = i  # column I = TeamID1 (0-based index 8)

    with _cache_lock:
        _layout_cache["dates"] = dates
        _layout_cache["team_row"] = team_row
        _layout_cache["ts"] = now
    return dates, team_row


def toggle_oncall_cell(gs_client, team_id1, date_str, new_status, updated_by=None):
    """Sets one person's status for one date, and stamps that row's
    LastUpdatedBy/LastUpdatedAt (whoever last touched ANY of that person's
    dates, not per-cell). new_status must be "on", "off", or "blank" (the
    three clickable states - "blank" means nobody's picked this person as
    Oncall for that day, "off" means Day Off. Legacy "always"/7*24 rows
    from old data are treated like any other cell now)."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_oncall_ws(sh)
    if ws is None:
        raise ValueError("OncallSchedule tab has not been seeded yet")

    dates, team_row = _get_layout(ws)
    if date_str not in dates:
        raise ValueError(f"date {date_str} is not a column in OncallSchedule")
    col_idx = IDENTITY_COLS + dates.index(date_str) + 1  # 1-based

    row_idx = team_row.get(team_id1)
    if row_idx is None:
        # Cache might be stale (e.g. a row was added after it was built) -
        # refresh once before giving up.
        _invalidate_layout_cache()
        dates, team_row = _get_layout(ws)
        row_idx = team_row.get(team_id1)
    if row_idx is None:
        raise ValueError(f"team_id1 {team_id1} not found in OncallSchedule")

    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    from gspread.utils import rowcol_to_a1
    cell_a1 = rowcol_to_a1(row_idx, col_idx)
    ws.batch_update([
        {"range": cell_a1, "values": [[new_status]]},
        {"range": f"N{row_idx}:O{row_idx}", "values": [[updated_by or "unknown", now_str]]},
    ], value_input_option="RAW")
    _invalidate_cache()
    return {"team_id1": team_id1, "date": date_str, "status": new_status, "updated_by": updated_by or "unknown", "updated_at": now_str}


def add_month_columns(gs_client, year_month):
    """Appends one column per day of `year_month` ("YYYY-MM") to the end of
    the sheet. Each new date is defaulted from that person's existing
    schedule on the SAME DAY OF WEEK (Mon-Sun) - e.g. if someone is
    consistently off on Fridays in the existing data, every Friday in the
    new month defaults to off too - using the most common status seen for
    that weekday (so one-off exceptions don't skew the pattern). People can
    still click to adjust individual days after. Dates that already exist
    as columns are skipped. Returns how many columns were actually added."""
    import calendar
    from collections import Counter
    from datetime import datetime
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
    existing_dates = header[IDENTITY_COLS:]
    existing_dates_set = set(existing_dates)
    dates_to_add = [d for d in month_dates if d not in existing_dates_set]
    if not dates_to_add:
        return 0

    existing_weekdays = [datetime.strptime(d, "%Y-%m-%d").weekday() for d in existing_dates]  # Mon=0..Sun=6
    new_weekdays = [datetime.strptime(d, "%Y-%m-%d").weekday() for d in dates_to_add]

    start_col = len(header) + 1  # 1-based, right after the current last column
    end_col = start_col + len(dates_to_add) - 1

    header_range = f"{rowcol_to_a1(1, start_col)}:{rowcol_to_a1(1, end_col)}"
    ws.update(header_range, [dates_to_add], value_input_option="RAW")

    n_data_rows = len(values) - 1
    if n_data_rows > 0:
        fill_rows = []
        for line in values[1:1 + n_data_rows]:
            padded = line + [""] * (len(header) - len(line))
            day_statuses = padded[IDENTITY_COLS:]
            weekday_pattern = {}
            for wd in range(7):
                statuses = [
                    (day_statuses[i] or "blank") for i, w in enumerate(existing_weekdays)
                    if w == wd and i < len(day_statuses)
                ]
                weekday_pattern[wd] = Counter(statuses).most_common(1)[0][0] if statuses else "blank"
            fill_rows.append([weekday_pattern.get(wd, "blank") for wd in new_weekdays])
        body_range = f"{rowcol_to_a1(2, start_col)}:{rowcol_to_a1(1 + n_data_rows, end_col)}"
        ws.update(body_range, fill_rows, value_input_option="RAW")

    _invalidate_cache()
    _invalidate_layout_cache()
    return len(dates_to_add)


def reset_default_on_to_blank(gs_client):
    """One-time cleanup: the original Excel seed marked every non-day-off
    cell as "on" (and a couple of fixed roles as "always"/7*24), which made
    the today-summary count people as Oncall by default - not useful once
    Oncall is meant to be an explicit pick per person per day, with no
    locked/fixed roles either. This finds every date cell still holding
    "on" or "always" and clears it to "blank" in ONE batched write. Returns
    how many cells were cleared."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_oncall_ws(sh)
    if ws is None:
        raise ValueError("OncallSchedule tab has not been seeded yet")

    values = ws.get_all_values()
    header = values[0]
    n_dates = len(header) - IDENTITY_COLS
    if n_dates <= 0 or len(values) <= 1:
        return 0

    n_data_rows = len(values) - 1
    new_grid = []
    cleared = 0
    for line in values[1:1 + n_data_rows]:
        padded = line + [""] * (len(header) - len(line))
        day_statuses = padded[IDENTITY_COLS:IDENTITY_COLS + n_dates]
        new_row = []
        for s in day_statuses:
            if s in ("on", "always"):
                new_row.append("blank")
                cleared += 1
            else:
                new_row.append(s)
        new_grid.append(new_row)

    if cleared:
        from gspread.utils import rowcol_to_a1
        start_col = IDENTITY_COLS + 1
        end_col = IDENTITY_COLS + n_dates
        body_range = f"{rowcol_to_a1(2, start_col)}:{rowcol_to_a1(1 + n_data_rows, end_col)}"
        ws.update(body_range, new_grid, value_input_option="RAW")
        _invalidate_cache()

    return cleared
