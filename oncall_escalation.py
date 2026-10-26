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

from oncall import _get_spreadsheet, ensure_grid, clean_tel
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


def duplicate_province_contacts(gs_client, source_province, target_province):
    """Adds a copy of every Sup/Eng Zone contact from `source_province`
    under `target_province` (same position/name/tel/type - e.g. a shared
    Eng Zone that covers two provinces) - useful when one Supervisor/Eng
    Zone actually covers multiple provinces and should show up under both.
    New rows start with a blank date grid (nothing pre-clicked) and no
    note, appended to the sheet's existing date columns. Skips any contact
    that would exact-duplicate a row already under target_province.
    Returns how many rows were added."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    values = ws.get_all_values()
    if not values:
        raise ValueError("OncallEscalation tab is empty")
    header = values[0]
    dates = header[IDENTITY_COLS:]

    prov_idx = IDENTITY_HEADER.index("Province")
    pos_idx = IDENTITY_HEADER.index("Position")
    name_idx = IDENTITY_HEADER.index("Name")
    tel_idx = IDENTITY_HEADER.index("Tel")
    type_idx = IDENTITY_HEADER.index("Type")

    existing_target_keys = set()
    source_rows = []
    for line in values[1:]:
        if not line or not line[0]:
            continue
        padded = line + [""] * (len(header) - len(line))
        if padded[prov_idx] == target_province:
            existing_target_keys.add((padded[pos_idx], padded[name_idx]))
        if padded[prov_idx] == source_province:
            source_rows.append(padded)

    new_rows = []
    for padded in source_rows:
        key = (padded[pos_idx], padded[name_idx])
        if key in existing_target_keys:
            continue  # already there, don't duplicate
        line = [""] * len(header)
        line[prov_idx] = target_province
        line[pos_idx] = padded[pos_idx]
        line[name_idx] = padded[name_idx]
        line[tel_idx] = padded[tel_idx]
        line[type_idx] = padded[type_idx]
        for i in range(len(dates)):
            line[IDENTITY_COLS + i] = "blank"
        new_rows.append(line)

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")
        _invalidate_cache()
        _invalidate_layout_cache()
    return len(new_rows)


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

    ensure_grid(ws, len(body) + 1, len(header))
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
            "province": province, "position": position, "name": name, "tel": tel, "type": typ,
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


_month_lock = threading.Lock()


def _ensure_month_columns(gs_client, date_str):
    """Adds the missing day columns of date_str's month. Idempotent (add_month_columns skips
    dates that exist) and serialised, so simultaneous clicks cannot add the same columns twice."""
    import re
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(date_str)) or not (2024 <= int(date_str[:4]) <= 2100) or not (1 <= int(date_str[5:7]) <= 12):
        raise ValueError(f"invalid date {date_str}")
    with _month_lock:
        add_month_columns(gs_client, date_str[:7])


def toggle_escalation_cell(gs_client, row_key, date_str, new_status, updated_by=None):
    """Sets one contact's status for one date. new_status must be "on",
    "off", or "blank" - same 3-way cycle as OncallSchedule."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    dates, row_index = _get_layout(ws)
    if date_str not in dates:
        # The month was never added to this tab (the "+ วางแผนเดือนถัดไป" button used to extend
        # only OncallSchedule). Add that month's missing day columns now (blank) and carry on,
        # instead of failing every click on those days.
        _ensure_month_columns(gs_client, date_str)
        ws = _get_escalation_ws(sh)
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
    ensure_grid(ws, len(values), end_col)  # the tab may already be at its column limit
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

    ensure_grid(ws, len(new_rows) + 1, len(new_header))
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


def update_tel(gs_client, row_key, tel, updated_by=None):
    """Sets one escalation contact's Tel (that one row only) and stamps LastUpdatedBy/At.
    Written as RAW text so a leading 0 is kept. row_key does not contain the Tel, so it stays valid."""
    tel = clean_tel(tel)
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

    tel_col = IDENTITY_HEADER.index("Tel") + 1  # 1-based
    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    from gspread.utils import rowcol_to_a1
    ws.batch_update([
        {"range": rowcol_to_a1(row_idx, tel_col), "values": [[tel]]},
        {"range": f"{rowcol_to_a1(row_idx, _LAST_UPDATED_BY_COL)}:{rowcol_to_a1(row_idx, _LAST_UPDATED_AT_COL)}",
         "values": [[updated_by or "unknown", now_str]]},
    ], value_input_option="RAW")
    _invalidate_cache()
    return {"row_key": row_key, "tel": tel, "updated_by": updated_by or "unknown", "updated_at": now_str}


def repair_column_alignment(gs_client):
    """Self-healing repair: rebuilds every row of OncallEscalation by
    matching values to their column NAME in the current header instead of
    trusting positional offsets. Date columns are found by pattern
    (YYYY-MM-DD). Safe to run repeatedly. Returns the number of data rows
    rewritten."""
    import re
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    values = ws.get_all_values()
    if not values:
        raise ValueError("OncallEscalation tab is empty")
    old_header = values[0]

    identity_pos = {name: (old_header.index(name) if name in old_header else None) for name in IDENTITY_HEADER}
    date_cols = sorted({h for h in old_header if date_re.match(h)})
    date_pos = {d: old_header.index(d) for d in date_cols}

    new_header = list(IDENTITY_HEADER) + date_cols
    new_rows = []
    for line in values[1:]:
        if not line or not line[0]:
            continue
        padded = line + [""] * (max(0, len(old_header) - len(line)))
        new_row = []
        for name in IDENTITY_HEADER:
            idx = identity_pos.get(name)
            new_row.append(padded[idx] if idx is not None and idx < len(padded) else "")
        for d in date_cols:
            idx = date_pos[d]
            val = padded[idx] if idx < len(padded) else "blank"
            new_row.append(val if val == "on" or val == "off" else "blank")
        new_rows.append(new_row)

    ensure_grid(ws, len(new_rows) + 1, len(new_header))
    ws.update("A1", [new_header] + new_rows, value_input_option="RAW")
    _invalidate_cache()
    _invalidate_layout_cache()
    return len(new_rows)


def _base_province(name):
    """"เชียงใหม่1"/"เชียงใหม่2" -> "เชียงใหม่" (the Thai province name the
    team rows use); every other province is returned unchanged."""
    n = (name or "").strip()
    return n[:-1] if n[-1:] in ("1", "2") else n


_TITLES = ("นางสาว", "น.ส.", "นส.", "นาง", "นาย", "คุณ")


def _strip_title(s):
    """Name without a leading title ("นายจตุพล จันทร์ทอง" -> "จตุพล จันทร์ทอง")."""
    n = " ".join(str(s or "").split())
    for t in _TITLES:
        if n.startswith(t):
            return n[len(t):].strip()
    return n


def _norm_name(s):
    """Whitespace-normalised name with any leading title removed - used for matching."""
    return _strip_title(s)


def import_roster(gs_client, roster, apply=False, updated_by=None, remove_names=()):
    """Matches `roster` (list of {province, position, name, dates:[YYYY-MM-DD]})
    against the live OncallEscalation tab and returns a plan; with
    apply=True it also writes it.

    Per roster entry:
      keep    - same province+position+name already exists (its Tel is kept)
      rename  - the person exists under the un-numbered province (e.g.
                "เชียงใหม่") and is moved to the numbered one ("เชียงใหม่1")
      copy    - person already exists under another province -> a new row
                is added with that row's Tel
      new     - nobody by that name+position -> new row, Tel left blank
    A roster entry's "tel" (when given) is authoritative: it replaces the
    stored Tel on keep/rename rows and fills copy/new rows.
    `remove_names` (position-less, title-insensitive) lists people who have
    left: every row of theirs is deleted.
    Other existing rows that are not in the roster are never touched.
    Day cells: only blank cells are set to "on"; an existing "on"/"off" is kept.
    """
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")

    all_dates = sorted({d for e in roster for d in e.get("dates", [])})
    values = ws.get_all_values()
    header = values[0] if values else []
    missing_months = sorted({d[:7] for d in all_dates if d not in set(header[IDENTITY_COLS:])})
    if apply:
        for ym in missing_months:
            with _month_lock:
                add_month_columns(gs_client, ym)
        if missing_months:
            values = ws.get_all_values()
            header = values[0]
    date_col = {d: IDENTITY_COLS + i for i, d in enumerate(header[IDENTITY_COLS:])}  # 0-based
    missing_dates = [d for d in all_dates if d not in date_col]

    # existing rows
    existing = []   # dicts with row (1-based), province, position, name, tel
    for i, line in enumerate(values[1:], start=2):
        if not line or not line[0]:
            continue
        p = line + [""] * (len(header) - len(line))
        existing.append({"row": i, "province": p[0], "position": p[1], "name": _norm_name(p[2]), "raw_name": p[2],
                         "tel": p[3], "type": p[4], "cells": p})
    by_exact = {}
    for e in existing:   # a title-less row wins over a "นาย..." twin
        k = (e["province"], e["position"], e["name"])
        if k not in by_exact or _strip_title(e["raw_name"]) == " ".join(e["raw_name"].split()):
            by_exact[k] = e
    claimed = set()
    plan = []
    for ent in roster:
        prov, pos, name = ent["province"], ent["position"], _norm_name(ent["name"])
        dates = [d for d in ent.get("dates", []) if d in date_col]
        item = {"province": prov, "position": pos, "name": name, "days_on": len(dates), "dates": dates}
        new_tel = ent.get("tel") or ""
        ex = by_exact.get((prov, pos, name))
        if ex:
            item.update(action="keep", tel=new_tel or ex["tel"], row=ex["row"], tel_update=bool(new_tel and new_tel != ex["tel"]))
            claimed.add(ex["row"])
        else:
            base = _base_province(prov)
            renamable = next((e for e in existing if e["position"] == pos and e["name"] == name
                              and e["row"] not in claimed and e["province"] == base and base != prov), None)
            if renamable:
                item.update(action="rename", from_province=renamable["province"], tel=new_tel or renamable["tel"], row=renamable["row"],
                            tel_update=bool(new_tel and new_tel != renamable["tel"]))
                claimed.add(renamable["row"])
            else:
                src = next((e for e in existing if e["position"] == pos and e["name"] == name and e["tel"]), None) \
                    or next((e for e in existing if e["position"] == pos and e["name"] == name), None)
                if src:
                    item.update(action="copy", from_province=src["province"], tel=new_tel or src["tel"], type=src["type"])
                else:
                    item.update(action="new", tel=new_tel)
        plan.append(item)

    gone = {_norm_name(n) for n in remove_names}
    to_remove = [{"row": e["row"], "province": e["province"], "position": e["position"], "name": e["name"]}
                 for e in existing if _norm_name(e["name"]) in gone]
    removed_rows = {x["row"] for x in to_remove}
    untouched = [{"province": e["province"], "position": e["position"], "name": e["name"]}
                 for e in existing if e["row"] not in claimed and e["row"] not in removed_rows]
    summary = {a: sum(1 for x in plan if x["action"] == a) for a in ("keep", "rename", "copy", "new")}
    summary["tel_updated"] = sum(1 for x in plan if x.get("tel_update"))
    summary["removed"] = len(to_remove)
    result = {"applied": False, "summary": summary, "missing_months": missing_months,
              "missing_dates": missing_dates, "plan": plan, "existing_not_in_roster": untouched,
              "to_remove": to_remove}
    if not apply:
        return result

    from gspread.utils import rowcol_to_a1
    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    updates, new_rows = [], []
    for item in plan:
        if item["action"] in ("keep", "rename"):
            r = item["row"]
            cells = next(e["cells"] for e in existing if e["row"] == r)
            if item["action"] == "rename":
                updates.append({"range": rowcol_to_a1(r, 1), "values": [[item["province"]]]})
            changed = False
            if item.get("tel_update"):
                updates.append({"range": rowcol_to_a1(r, 4), "values": [[item["tel"]]]})
                changed = True
            for d in item["dates"]:
                c = date_col[d]
                if (cells[c] or "blank") == "blank":
                    updates.append({"range": rowcol_to_a1(r, c + 1), "values": [["on"]]})
                    changed = True
            if changed or item["action"] == "rename":
                updates.append({"range": f"{rowcol_to_a1(r, _LAST_UPDATED_BY_COL)}:{rowcol_to_a1(r, _LAST_UPDATED_AT_COL)}",
                                "values": [[updated_by or "import", now_str]]})
        else:
            line = [""] * len(header)
            line[0], line[1], line[2] = item["province"], item["position"], item["name"]
            line[3] = item.get("tel", "")
            line[4] = item.get("type", "")
            line[IDENTITY_HEADER.index("LastUpdatedBy")] = updated_by or "import"
            line[IDENTITY_HEADER.index("LastUpdatedAt")] = now_str
            for c in range(IDENTITY_COLS, len(header)):
                line[c] = "blank"
            for d in item["dates"]:
                line[date_col[d]] = "on"
            new_rows.append(line)
    for k in range(0, len(updates), 400):
        ws.batch_update(updates[k:k + 400], value_input_option="RAW")
    if new_rows:
        ensure_grid(ws, len(values) + len(new_rows) + 1, len(header))
        ws.append_rows(new_rows, value_input_option="RAW")
    _invalidate_cache()
    _invalidate_layout_cache()
    for r in sorted(removed_rows, reverse=True):   # bottom first so row numbers stay valid
        ws.delete_rows(r)
    _invalidate_cache()
    _invalidate_layout_cache()
    result["applied"] = True
    result["written"] = {"cell_updates": len(updates), "rows_added": len(new_rows), "rows_removed": len(removed_rows)}
    return result


def dedupe_titled_names(gs_client, apply=False, updated_by=None):
    """Removes Sup/Eng Zone rows whose name only differs by a leading title
    ("นายจตุพล ..." vs "จตุพล ...") when the title-less duplicate exists under
    the same position and province (numbered CMI1/CMI2 count as the same
    province). A titled row with no title-less twin is left alone.

    Before a titled row is deleted, whatever it holds that the surviving rows
    lack is carried over: Tel / Type / Note (only where blank) and day cells
    (only where blank, and only when the survivor is in the same province or
    is the single survivor - otherwise they are reported as dropped).
    """
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        raise ValueError("OncallEscalation tab has not been seeded yet")
    values = ws.get_all_values()
    if not values:
        raise ValueError("OncallEscalation tab is empty")
    header = values[0]
    rows = []
    for i, line in enumerate(values[1:], start=2):
        if not line or not line[0]:
            continue
        p = line + [""] * (len(header) - len(line))
        rows.append({"row": i, "cells": p, "province": p[0], "position": p[1], "name": p[2],
                     "clean": _strip_title(p[2]), "titled": _strip_title(p[2]) != " ".join(p[2].split())})
    groups = {}
    for r in rows:
        groups.setdefault((_base_province(r["province"]), r["position"], r["clean"]), []).append(r)

    tel_i, type_i, note_i = 3, 4, IDENTITY_HEADER.index("Note")
    plan, updates, delete_rows = [], [], []
    for (_, pos, clean), grp in groups.items():
        keepers = [r for r in grp if not r["titled"]]
        titled = [r for r in grp if r["titled"]]
        if not keepers or not titled:
            continue
        for t in titled:
            same = [k for k in keepers if k["province"] == t["province"]]
            targets_for_cells = same or (keepers if len(keepers) == 1 else [])
            carried, dropped = [], 0
            for k in keepers:
                for col, label in ((tel_i, "tel"), (type_i, "type"), (note_i, "note")):
                    if not k["cells"][col] and t["cells"][col]:
                        k["cells"][col] = t["cells"][col]
                        updates.append((k["row"], col + 1, t["cells"][col]))
                        carried.append(f"{label}->{k['province']}")
            for c in range(IDENTITY_COLS, len(header)):
                v = t["cells"][c]
                if v in ("", "blank"):
                    continue
                placed = False
                for k in targets_for_cells:
                    if k["cells"][c] in ("", "blank"):
                        k["cells"][c] = v
                        updates.append((k["row"], c + 1, v))
                        placed = True
                if not placed and not any(k["cells"][c] == v for k in targets_for_cells):
                    dropped += 1
            plan.append({"delete_row": t["row"], "province": t["province"], "position": pos, "name": t["name"],
                         "kept_as": sorted({f"{k['province']} | {k['name']}" for k in keepers}),
                         "carried": carried, "day_cells_dropped": dropped})
            delete_rows.append(t["row"])
    result = {"applied": False, "to_delete": len(plan), "plan": plan}
    if not apply or not plan:
        return result

    from gspread.utils import rowcol_to_a1
    now_str = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    batch = [{"range": rowcol_to_a1(r, c), "values": [[v]]} for r, c, v in updates]
    for r in sorted({r for r, _, _ in updates}):
        batch.append({"range": f"{rowcol_to_a1(r, _LAST_UPDATED_BY_COL)}:{rowcol_to_a1(r, _LAST_UPDATED_AT_COL)}",
                      "values": [[updated_by or "dedupe", now_str]]})
    for k in range(0, len(batch), 400):
        ws.batch_update(batch[k:k + 400], value_input_option="RAW")
    for r in sorted(delete_rows, reverse=True):   # bottom first so row numbers stay valid
        ws.delete_rows(r)
    _invalidate_cache()
    _invalidate_layout_cache()
    result["applied"] = True
    return result
