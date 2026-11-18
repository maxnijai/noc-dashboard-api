"""Lowest-ever (since START) @01:15 count per card, for the "ต่ำสุด X · date" badge on the Total Pending and P0 cards.

Stored in the Google Sheet tab MinRecord (same workbook as PresentationConfig):
    A=row ("pending" | "p0"), B=group (TOTAL | MB | FBB | NW_NSA12 | NSA34), C=min value, D=date YYYY-MM-DD,
    E=updated_at (Bangkok), F=source ("seed" = starting value set by MAX, "auto" = lower value found in the trend)
The tab is created and seeded on first use. Every /api/p0-daily-trend call compares the day series (dates >= START)
with the stored value and writes a new row value only when it is LOWER; equal values keep the older date.
Editing C/D by hand in the sheet is the way to reset a record.
"""
import threading
import time
from datetime import datetime, timedelta, timezone

from pending_ticket import _with_quota_retry, _is_quota_error
from realtime_monitor import REALTIME_SHEET_ID

SHEET = "MinRecord"
START = "2026-11-01"
GROUPS = ["MB", "FBB", "NW_NSA12", "NSA34"]
KEYS = ["TOTAL"] + GROUPS
SEED = {
    "pending": {"TOTAL": (380, "2026-11-16"), "MB": (75, "2026-11-16"), "FBB": (29, "2026-11-03"),
                "NW_NSA12": (17, "2026-11-16"), "NSA34": (251, "2026-11-18")},
    "p0": {"TOTAL": (57, "2026-11-17"), "MB": (12, "2026-11-04"), "FBB": (3, "2026-11-08"),
           "NW_NSA12": (1, "2026-11-17"), "NSA34": (30, "2026-11-18")},
}
_lock = threading.Lock()
_cache = {"v": None, "ts": 0.0}


def _now_str():
    return datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S")


def _seed_rows():
    rows = [["row", "group", "min", "date", "updated_at", "source"]]
    for r in ("pending", "p0"):
        for k in KEYS:
            v, d = SEED[r][k]
            rows.append([r, k, v, d, _now_str(), "seed"])
    return rows


def _ws(gs_client):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    try:
        return _with_quota_retry(lambda: sh.worksheet(SHEET))
    except Exception as e:
        if _is_quota_error(e):
            raise
        ws = _with_quota_retry(lambda: sh.add_worksheet(title=SHEET, rows=20, cols=6))
        _with_quota_retry(lambda: ws.update("A1:F11", _seed_rows(), value_input_option="RAW"))
        return ws


def _read(ws):
    """-> {"pending": {key: {"min", "date", "source"}}, "p0": {...}}, missing entries filled from SEED."""
    vals = _with_quota_retry(lambda: ws.get_all_values())
    out = {"pending": {}, "p0": {}}
    for row in vals[1:]:
        row = (row + [""] * 6)[:6]
        r, k = row[0].strip().lower(), row[1].strip().upper()
        if r in out and k in KEYS:
            try:
                out[r][k] = {"min": int(float(row[2])), "date": row[3].strip()[:10], "source": row[5].strip() or "seed"}
            except ValueError:
                pass
    for r in out:
        for k in KEYS:
            if k not in out[r]:
                v, d = SEED[r][k]
                out[r][k] = {"min": v, "date": d, "source": "seed"}
    return out


def _series(data, field):
    """-> {key: [(date, value), ...]} for one of the trend fields; TOTAL only on days where all 4 groups have a value."""
    dates = data.get("dates") or []
    s = data.get(field) or {}
    out = {k: [] for k in KEYS}
    for i, d in enumerate(dates):
        if d < START:
            continue
        vals = []
        for k in GROUPS:
            arr = s.get(k) or []
            v = arr[i] if i < len(arr) else None
            vals.append(v)
            if v is not None:
                out[k].append((d, v))
        if all(v is not None for v in vals):
            out["TOTAL"].append((d, sum(vals)))
    return out


def update(gs_client, data):
    """Compare the trend (build_p0_daily_trend output) with the stored records, save any new lows, return the records.
    Never raises: on any sheet problem the last known (or seeded) records are returned without saving."""
    now = time.monotonic()
    with _lock:
        cached = _cache["v"]
        fresh = cached is not None and now - _cache["ts"] < 300
    try:
        if fresh:
            rec, ws = {r: {k: dict(v) for k, v in cached[r].items()} for r in cached}, None
        else:
            ws = _ws(gs_client)
            rec = _read(ws)
    except Exception:
        rec, ws = (cached or {r: {k: {"min": v, "date": d, "source": "seed"} for k, (v, d) in SEED[r].items()} for r in SEED}), None
    changed = False
    for r, field in (("pending", "series_total"), ("p0", "series")):
        for k, pts in _series(data, field).items():
            for d, v in pts:
                if v < rec[r][k]["min"]:
                    rec[r][k] = {"min": v, "date": d, "source": "auto"}
                    changed = True
    if changed:
        try:
            ws = ws or _ws(gs_client)
            rows = [["row", "group", "min", "date", "updated_at", "source"]]
            for r in ("pending", "p0"):
                for k in KEYS:
                    e = rec[r][k]
                    rows.append([r, k, e["min"], e["date"], _now_str(), e["source"]])
            _with_quota_retry(lambda: ws.update("A1:F11", rows, value_input_option="RAW"))
        except Exception:
            pass  # still show the new low this time; it is written on the next call
    with _lock:
        _cache.update(v=rec, ts=now if not fresh else _cache["ts"])
    return {"start": START, "records": rec}
