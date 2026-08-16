"""Oncall Sup/Eng Zone tab - a per-province escalation contact directory
WITH the same clickable per-date Oncall/Day Off grid as the Oncall Team
tab. If a field team doesn't answer, this is who to call next: Supervisor
(and Supervisor MTE) first, then Eng Zone as the final escalation level.

Stored in its own "OncallEscalation" tab in the same spreadsheet the rest
of the Oncall feature uses (REALTIME_SHEET_ID via oncall.py's helpers), so
no new spreadsheet/credentials to manage.

Storage shape (one row per contact, one column per date) - mirrors
oncall.py's OncallSchedule:
    Province | Position | Name | Tel | Type | LastUpdatedBy |
    LastUpdatedAt | <date1> | <date2> | ...

Cell values: "" or "blank" (unset - default, nobody's picked this person
as available that day), "on" (explicitly marked Oncall), "off" (Day Off).
Click cycle: blank -> on -> off -> blank, same as OncallSchedule.
"""

import threading
import time

from oncall import _get_spreadsheet
from pending_trend import bangkok_now

ESCALATION_SHEET = "OncallEscalation"
IDENTITY_HEADER = ["Province", "Position", "Name", "Tel", "Type", "LastUpdatedBy", "LastUpdatedAt", "Note"]
IDENTITY_COLS = len(IDENTITY_HEADER)
_LAST_UPDATED_BY_COL = IDENTITY_HEADER.index("LastUpdatedBy") + 1  # 1-based
_LAST_UPDATED_AT_COL = IDENTITY_HEADER.index("LastUpdatedAt") + 1  # 1-based

# Escalation call order within a province: Supervisor first, Supervisor MTE
# next, Eng Zone last (the final level if nobody below answers).
POSITION_RANK = {"Supervisor": 0, "Supervisor MTE": 1, "Eng Zone": 2}

_CACHE_TTL_SECONDS = 7200  # 2h; explicit invalidation on every write (see toggle/seed below)
_cache = {"data": None, "ts": 0}
_cache_lock = threading.Lock()

_LAYOUT_CACHE_TTL_SECONDS = 300
_layout_cache = {"dates": None, "row_index": None, "ts": 0}


def _get_escalation_ws(spreadsheet):
    try:
        return spreadsheet.worksheet(ESCALATION_SHEET)
    except Exception:
        return None


def _invalidate_cache():
    with _cache_lock:
        _cache["data"] = None
        _cache["ts"] = 0


def _invalidate_layout_cache():
    with _cache_lock:
        _layout_cache["dates"] = None
        _layout_cache["row_index"] = None
        _layout_cache["ts"] = 0


def _row_key(province, position, name):
    return f"{province}||{position}||{name}"


def seed_escalation_contacts(gs_client, rows, dates):
    """Replaces the whole OncallEscalation tab with `rows` (list of dicts:
    province, position, name, tel, type) and one column per date in
    `dates` ("YYYY-MM-DD" strings), all defaulted to blank (nobody's
    Oncall until picked). Safe to re-run - always a full replace."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    header = IDENTITY_HEADER + dates
    if ws is None:
        ws = sh.add_worksheet(title=ESCALATION_SHEET, rows=len(rows) + 10, cols=len(header) + 5)
    else:
        ws.clear()

    body = []
    for r in rows:
        line = [
            r.get("province", ""), r.get("position", ""), r.get("name", ""),
            r.get("tel", ""), r.get("type", ""), "", "", "",  # LastUpdatedBy/At/Note blank
        ]
        line.extend(["blank"] * len(dates))
        body.append(line)

    ws.update("A1", [header] + body, value_input_option="RAW")
    _invalidate_cache()
    _invalidate_layout_cache()
    return len(body)


def load_escalation_contacts(gs_client, use_cache=True):
    """Returns {"dates": [...], "provinces": [{"province": ..., "contacts":
    [{position, name, tel, type, row_key, last_updated_by,
    last_updated_at, days: {date: status}}, ...]}]} sorted by province name,
    contacts within each province sorted by escalation order. None if the
    tab hasn't been seeded yet."""
    now = time.monotonic()
    if use_cache:
        with _cache_lock:
            if _cache["data"] is not None and (now - _cache["ts"]) < _CACHE_TTL_SECONDS:
                return _cache["data"]

    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        return None
    values = ws.get_all_values()
    if not values:
        return None
    header = values[0]
    dates = header[IDENTITY_COLS:]

    by_province = {}
    for row in values[1:]:
        if not row or not row[0]:
            continue
        padded = row + [""] * (len(header) - len(row))
        province, position, name, tel, typ, last_by, last_at, note = padded[:IDENTITY_COLS]
        days = {dates[i]: (padded[IDENTITY_COLS + i] or "blank") for i in range(len(dates))}
        by_province.setdefault(province, []).append({
            "position": position, "name": name, "tel": tel, "type": typ,
            "row_key": _row_key(province, position, name),
            "last_updated_by": last_by, "last_updated_at": last_at, "note": note,
            "days": days,
        })

    provinces = []
    for province in sorted(by_province.keys(), key=lambda p: p):
        contacts = sorted(by_province[province], key=lambda c: POSITION_RANK.get(c["position"], 99))
        provinces.append({"province": province, "contacts": contacts})

    result = {"dates": dates, "provinces": provinces}
    if use_cache:
        with _cache_lock:
            _cache["data"] = result
            _cache["ts"] = now
    return result


def _get_layout(ws):
    now = time.monotonic()
    with _cache_lock:
        if _layout_cache["dates"] is not None and (now - _layout_cache["ts"]) < _LAYOUT_CACHE_TTL_SECONDS:
            return _layout_cache["dates"], _layout_cache["row_index"]

    values = ws.get_all_values()
    header = values[0]
    dates = header[IDENTITY_COLS:]
    row_index = {}
    for i, line in enumerate(values[1:], start=2):
        if len(line) >= 3 and line[0]:
            row_index[_row_key(line[0], line[1], line[2])] = i

    with _cache_lock:
        _layout_cache["dates"] = dates
        _layout_cache["row_index"] = row_index
        _layout_cache["ts"] = now
    return dates, row_index


def toggle_escalation_cell(gs_client, row_key, date_str, new_status, updated_by=None):
    """Sets one contact's status for one date. new_status must be "on",
    "off", or "blank" - same 3-way cycle as OncallSchedule."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    dates, row_index = _get_layout(ws)
    if date_str not in dates:
        raise ValueError(f"date {date_str} is not a column in OncallEscalation")
    col_idx = IDENTITY_COLS + dates.index(date_str) + 1  # 1-based

    row_idx = row_index.get(row_key)
    if row_idx is None:
        _invalidate_layout_cache()
        dates, row_index = _get_layout(ws)
        row_idx = row_index.get(row_key)
    if row_idx is None:
        raise ValueError(f"contact {row_key} not found in OncallEscalation")

    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    from gspread.utils import rowcol_to_a1
    cell_a1 = rowcol_to_a1(row_idx, col_idx)
    tracking_range = f"{rowcol_to_a1(row_idx, _LAST_UPDATED_BY_COL)}:{rowcol_to_a1(row_idx, _LAST_UPDATED_AT_COL)}"
    ws.batch_update([
        {"range": cell_a1, "values": [[new_status]]},
        {"range": tracking_range, "values": [[updated_by or "unknown", now_str]]},
    ], value_input_option="RAW")
    _invalidate_cache()
    return {"row_key": row_key, "date": date_str, "status": new_status, "updated_by": updated_by or "unknown", "updated_at": now_str}


def add_month_columns(gs_client, year_month):
    """Appends one column per day of `year_month` ("YYYY-MM") to the end of
    the sheet, all defaulted to blank. Skips dates that already exist.
    Returns how many columns were added."""
    import calendar
    from gspread.utils import rowcol_to_a1

    year, month = (int(x) for x in year_month.split("-"))
    _, days_in_month = calendar.monthrange(year, month)
    month_dates = [f"{year:04d}-{month:02d}-{d:02d}" for d in range(1, days_in_month + 1)]

    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    values = ws.get_all_values()
    header = values[0]
    existing_dates = set(header[IDENTITY_COLS:])
    dates_to_add = [d for d in month_dates if d not in existing_dates]
    if not dates_to_add:
        return 0

    start_col = len(header) + 1
    end_col = start_col + len(dates_to_add) - 1
    header_range = f"{rowcol_to_a1(1, start_col)}:{rowcol_to_a1(1, end_col)}"
    ws.update(header_range, [dates_to_add], value_input_option="RAW")

    n_data_rows = len(values) - 1
    if n_data_rows > 0:
        body_range = f"{rowcol_to_a1(2, start_col)}:{rowcol_to_a1(1 + n_data_rows, end_col)}"
        fill = [["blank"] * len(dates_to_add) for _ in range(n_data_rows)]
        ws.update(body_range, fill, value_input_option="RAW")

    _invalidate_cache()
    _invalidate_layout_cache()
    return len(dates_to_add)


def add_note_column(gs_client):
    """One-time migration: appends the "Note" identity column right after
    LastUpdatedAt on the LIVE sheet, WITHOUT touching any date-cell data -
    every existing Oncall/Day Off pick is read back unchanged and
    rewritten as-is. New rows get an empty Note."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    values = ws.get_all_values()
    if not values:
        raise ValueError("OncallEscalation tab is empty")
    old_header = values[0]

    if "Note" in old_header:
        raise ValueError("Note column already exists - nothing to migrate")

    if "LastUpdatedAt" in old_header:
        insert_at = old_header.index("LastUpdatedAt") + 1
    else:
        insert_at = min(IDENTITY_COLS - 1, len(old_header))

    new_header = old_header[:insert_at] + ["Note"] + old_header[insert_at:]
    new_rows = []
    for line in values[1:]:
        if not line or not line[0]:
            continue
        padded = line + [""] * (len(old_header) - len(line))
        new_row = padded[:insert_at] + [""] + padded[insert_at:]
        new_rows.append(new_row)

    ws.update("A1", [new_header] + new_rows, value_input_option="RAW")
    _invalidate_cache()
    _invalidate_layout_cache()
    return len(new_rows)


def update_note(gs_client, row_key, note_text, updated_by=None):
    """Sets the freeform Note for one escalation contact, and stamps
    LastUpdatedBy/LastUpdatedAt the same way a date-cell toggle does."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    dates, row_index = _get_layout(ws)
    row_idx = row_index.get(row_key)
    if row_idx is None:
        _invalidate_layout_cache()
        dates, row_index = _get_layout(ws)
        row_idx = row_index.get(row_key)
    if row_idx is None:
        raise ValueError(f"contact {row_key} not found in OncallEscalation")

    note_col = IDENTITY_HEADER.index("Note") + 1  # 1-based
    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    from gspread.utils import rowcol_to_a1
    ws.batch_update([
        {"range": rowcol_to_a1(row_idx, note_col), "values": [[note_text]]},
        {"range": f"{rowcol_to_a1(row_idx, _LAST_UPDATED_BY_COL)}:{rowcol_to_a1(row_idx, _LAST_UPDATED_AT_COL)}",
         "values": [[updated_by or "unknown", now_str]]},
    ], value_input_option="RAW")
    _invalidate_cache()
    return {"row_key": row_key, "note": note_text, "updated_by": updated_by or "unknown", "updated_at": now_str}
