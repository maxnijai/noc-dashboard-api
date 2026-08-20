"""Nan Team Tracker: plots field teams currently working in Nan province on
a map, using the latest "Update พิกัด" (coordinate update) each team has
logged in the shared field-tracking Google Sheet.

Source data comes from a separate Google Sheet (not REALTIME_SHEET_ID or
the Nan site-master sheet) - a manually-updated field log where each row
is one ticket a team is/was working, with periodic coordinate check-ins
as they travel to and work the site.
"""

import logging
import threading
import time

log = logging.getLogger(__name__)

TEAM_SHEET_ID = "1t8DErfQLBRXkoaorDFMdAxtLZf-RA_hzQ-xFQI6EgBo"
TEAM_SHEET_TAB = "ชีต1"

_team_cache = {"data": None, "ts": 0}
_team_cache_lock = threading.Lock()
TEAM_CACHE_TTL_SECONDS = 30


def _to_latlon(raw):
    """'Update พิกัด' cells look like '18.839556, 99.067483' (lat, lon)."""
    raw = str(raw or "").strip()
    if not raw or "," not in raw:
        return None, None
    try:
        lat_str, lon_str = raw.split(",", 1)
        return float(lat_str.strip()), float(lon_str.strip())
    except (TypeError, ValueError):
        return None, None


def fetch_nan_team_rows(gs_client, use_cache=True):
    """Returns every Nan-site ticket row from the field-tracking sheet,
    each with parsed lat/lon (None if that row has no coordinate yet)."""
    now = time.monotonic()
    if use_cache:
        with _team_cache_lock:
            if _team_cache["data"] is not None and (now - _team_cache["ts"]) < TEAM_CACHE_TTL_SECONDS:
                return _team_cache["data"]

    sh = gs_client.open_by_key(TEAM_SHEET_ID)
    ws = sh.worksheet(TEAM_SHEET_TAB)
    raw_rows = ws.get_all_values()

    header_idx = None
    for i, row in enumerate(raw_rows):
        if row and row[0].strip() == "Ticket" and len(row) > 19 and row[19].strip() == "Update พิกัด":
            header_idx = i
            break
    if header_idx is None:
        for i, row in enumerate(raw_rows):
            if row and row[0].strip() == "Ticket":
                header_idx = i
                break
    if header_idx is None:
        return []

    header = raw_rows[header_idx]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def get(row, name, default=""):
        i = col.get(name)
        if i is None or i >= len(row):
            return default
        return row[i]

    out = []
    for row in raw_rows[header_idx + 1:]:
        if not row or not row[0].strip():
            continue
        site = get(row, "Site").strip()
        if not site.upper().startswith("NAN"):
            continue
        lat, lon = _to_latlon(get(row, "Update พิกัด"))
        out.append({
            "ticket": get(row, "Ticket").strip(),
            "sla": get(row, "SLA").strip(),
            "subject": get(row, "Subject").strip(),
            "site": site,
            "district": get(row, "อำเภอ").strip(),
            "subdistrict": get(row, "ตำบล").strip(),
            "team_id": get(row, "Team ID").strip(),
            "type_team": get(row, "Type Team").strip(),
            "status_team": get(row, "Status Team").strip(),
            "travel_time": get(row, "เวลาเดินทาง").strip(),
            "repair_start_time": get(row, "เวลาเริ่มซ่อม").strip(),
            "hold": get(row, "Hold").strip(),
            "hold_reason": get(row, "สาเหตุการ Hold").strip(),
            "inoc_name": get(row, "INOC Name").strip(),
            "team_count": get(row, "จำนวนทีมทำงาน").strip(),
            "status_sccd": get(row, "Status_SCCD").strip(),
            "lat": lat, "lon": lon,
        })

    if use_cache:
        with _team_cache_lock:
            _team_cache["data"] = out
            _team_cache["ts"] = now
    return out


def build_nan_team_response(gs_client=None):
    if gs_client is None:
        from pending_trend import get_drive_and_sheets_clients
        _, gs_client = get_drive_and_sheets_clients()

    rows = fetch_nan_team_rows(gs_client)

    # Latest valid-coordinate row per team = that team's current position.
    # Rows are in sheet order (earliest first), so overwriting as we go
    # naturally keeps the last one.
    latest_by_team = {}
    for r in rows:
        if not r["team_id"] or r["lat"] is None or r["lon"] is None:
            continue
        latest_by_team[r["team_id"]] = r

    team_markers = list(latest_by_team.values())

    return {
        "teams": team_markers,
        "tickets": rows,
        "total_teams": len(team_markers),
        "total_tickets": len(rows),
    }
