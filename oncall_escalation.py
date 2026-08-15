"""Oncall Sup/Eng Zone tab - a per-province escalation contact directory.
If a field team doesn't answer, this is who to call next: Supervisor (and
Supervisor MTE) first, then Eng Zone as the final escalation level. Stored
in its own "OncallEscalation" tab in the same spreadsheet the rest of the
Oncall feature uses (REALTIME_SHEET_ID via oncall.py's helpers), so no new
spreadsheet/credentials to manage.

This is reference data, not something people click to toggle day-to-day -
load/seed only, no per-cell state like OncallSchedule has.
"""

import threading
import time

from oncall import _get_spreadsheet

ESCALATION_SHEET = "OncallEscalation"
ESCALATION_HEADER = ["Province", "Position", "Name", "Tel", "Type"]

# Escalation call order within a province: Supervisor first, Supervisor MTE
# next, Eng Zone last (the final level if nobody below answers).
POSITION_RANK = {"Supervisor": 0, "Supervisor MTE": 1, "Eng Zone": 2}

_CACHE_TTL_SECONDS = 7200  # 2h; only ever changes via seed_escalation_contacts()
_cache = {"data": None, "ts": 0}
_cache_lock = threading.Lock()


def _get_escalation_ws(spreadsheet):
    try:
        return spreadsheet.worksheet(ESCALATION_SHEET)
    except Exception:
        return None


def _invalidate_cache():
    with _cache_lock:
        _cache["data"] = None
        _cache["ts"] = 0


def seed_escalation_contacts(gs_client, rows):
    """Replaces the whole OncallEscalation tab with `rows` (list of dicts:
    province, position, name, tel, type). Safe to re-run - always a full
    replace, not additive, since this is a small manually-curated list."""
    sh = _get_spreadsheet(gs_client)
    ws = _get_escalation_ws(sh)
    if ws is None:
        ws = sh.add_worksheet(title=ESCALATION_SHEET, rows=len(rows) + 10, cols=len(ESCALATION_HEADER))
    else:
        ws.clear()

    body = [[r.get("province", ""), r.get("position", ""), r.get("name", ""),
             r.get("tel", ""), r.get("type", "")] for r in rows]
    ws.update("A1", [ESCALATION_HEADER] + body, value_input_option="RAW")
    _invalidate_cache()
    return len(body)


def load_escalation_contacts(gs_client, use_cache=True):
    """Returns {"provinces": [{"province": ..., "contacts": [{position,
    name, tel, type}, ...]}]} sorted by province name, with contacts within
    each province sorted by escalation order (Supervisor -> Eng Zone). None
    if the tab hasn't been seeded yet."""
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

    by_province = {}
    for row in values[1:]:
        if not row or not row[0]:
            continue
        padded = row + [""] * (5 - len(row))
        province, position, name, tel, typ = padded[:5]
        by_province.setdefault(province, []).append({
            "position": position, "name": name, "tel": tel, "type": typ,
        })

    provinces = []
    for province in sorted(by_province.keys(), key=lambda p: p):
        contacts = sorted(by_province[province], key=lambda c: POSITION_RANK.get(c["position"], 99))
        provinces.append({"province": province, "contacts": contacts})

    result = {"provinces": provinces}
    if use_cache:
        with _cache_lock:
            _cache["data"] = result
            _cache["ts"] = now
    return result
