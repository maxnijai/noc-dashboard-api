"""NSA3/NSA4 Flag Priority panel data: P0 / P1 tickets now + daily P0 snapshot (tab P0DailyTrend).

Panel behind the NSA3/NSA4 card's "Flag Priority" button (card numbers themselves are untouched):
  * P0 now (same formula as the card) and P1 tickets, overall and per province, listed per ticket with the work-log
    Detail / Plan_Closed_Date (edited through the existing /api/pending-ticket/update);
  * one row per province per day at ~01:20 in the Google Sheet tab P0DailyTrend, so the "@01:15" comparison and the
    trend lines are read from that small tab - the big ~5MB Drive backup files are never opened for them.
Read-only on the live data; the only write is the P0DailyTrend tab (created on first use).
"""
import logging
import re
import threading
import time
from datetime import datetime, timedelta

import pending_ticket as pt
from pending_ticket import bangkok_now

log = logging.getLogger(__name__)

SNAPSHOT_SHEET = "P0DailyTrend"
SNAPSHOT_HEADER = ["Date", "Province", "NSA3_P0", "NSA4_P0", "P0", "P1", "Total", "RecordedAt"]
FORECAST_DAYS = pt.FLAG_LOGIC_DAYS  # panel lists the same tickets as export column AA (Flag Logic): P0 แล้ว + P0 ใน 1-7 วัน
TREND_HISTORY_DAYS = 30
# Daily history of the 8 Flag Logic steps (S0 = "P0 แล้ว", S1..S7 = "P0 ใน N วัน") per TRUEOWNERGROUP, counted at each day's own 01:15 file
STEPS_ENABLED = False   # the daily 8-step trend chart was removed from the panel; flip to True to record/read it again
STEPS_SHEET = "P0StepTrend"
STEPS_HEADER = ["Date", "Group"] + ["S%d" % i for i in range(8)] + ["RecordedAt"]

# Province code (TRUEOWNERGROUP) -> Thai name. PSN/PCT/PCB/PHE follow the ticket data's own codes.
PROV_TH = {
    "CMI": "เชียงใหม่", "CRI": "เชียงราย", "LPG": "ลำปาง", "LPN": "ลำพูน", "MHS": "แม่ฮ่องสอน",
    "NAN": "น่าน", "PHE": "แพร่", "PYO": "พะเยา", "KPP": "กำแพงเพชร", "PCB": "เพชรบูรณ์",
    "PCT": "พิจิตร", "PSN": "พิษณุโลก", "SKT": "สุโขทัย", "TAK": "ตาก", "UTR": "อุตรดิตถ์",
}

_lock = threading.Lock()
_trend_cache = {"data": None, "ts": 0.0}
_steps_cache = {"data": None, "ts": 0.0}
_TREND_TTL = 300


def _scope_nsa34(rows):
    """Rows in exactly the NSA3/NSA4 scope the Total Pending card uses (NOR1/NOR2, CORP excluded) -> [(row, province code)]."""
    from ticket_views import row_matches_view
    apply_tog = pt._rows_have_owner_group(rows)
    out = []
    for r in rows:
        if str(r.get("Region", "")).strip() not in pt.PENDING_TICKET_REGIONS:
            continue
        if str(r.get("SEVERITY", "")).strip() not in pt.ALLOWED_SEVERITIES:
            continue
        if not pt._owner_group_ok(r, apply_tog):
            continue
        if not row_matches_view(r, "NSA34"):
            continue
        code = (pt._extract_region_province(r.get("TRUEOWNERGROUP"))[1] if apply_tog else None) or ""
        out.append((r, code))
    return out


def _scoped_nsa34_rows(gs_client):
    return _scope_nsa34(pt.fetch_live_rows(gs_client))


def _aggregate(rows, day):
    """{province name: {n3, n4, p0, p1, tot}} for `rows` classified at `day`'s own 01:15 reference."""
    ref = datetime(day.year, day.month, day.day, 1, 15)
    agg = {}
    for r, code in _scope_nsa34(rows):
        a = agg.setdefault(_prov_name(code), {"n3": 0, "n4": 0, "p0": 0, "p1": 0, "tot": 0})
        a["tot"] += 1
        pr = pt._classify_priority_at(r.get("TARGETFINISH"), ref)
        if pr == "P0":
            a["p0"] += 1
            a["n3" if str(r.get("SEVERITY", "")).strip() == "NSA3" else "n4"] += 1
        elif pr == "P1":
            a["p1"] += 1
    return agg


_TOG_RE = re.compile(r"(NOR[12])-([A-Z0-9]+)-")


def _group_key(r):
    m = _TOG_RE.search(str(r.get("TRUEOWNERGROUP", "") or ""))
    return "%s-%s" % (m.group(1), m.group(2)) if m else "#none"


def _aggregate_steps(rows, day):
    """{group key: [8 counts]} - NSA3/NSA4 tickets by Flag Logic step, classified with the same reference the live panel
    uses on that day (next 01:15), so history and today's numbers are comparable."""
    ref0 = datetime(day.year, day.month, day.day, 1, 15) + timedelta(days=1)
    out = {}
    for r, _code in _scope_nsa34(rows):
        k = pt.ticket_p0_offset(r.get("TARGETFINISH"), ref0)
        if k is None or k > pt.FLAG_LOGIC_DAYS:
            continue
        out.setdefault(_group_key(r), [0] * 8)[k] += 1
    return out


def _prov_name(code):
    return PROV_TH.get(code, code or "(ไม่ระบุ)")


# ── Snapshot (nightly ~01:20) ─────────────────────────────────────────────
def _ensure_tab(sh):
    try:
        return sh.worksheet(SNAPSHOT_SHEET)
    except Exception:
        ws = sh.add_worksheet(title=SNAPSHOT_SHEET, rows=2000, cols=len(SNAPSHOT_HEADER))
        ws.append_row(SNAPSHOT_HEADER)
        return ws


def _write_days(gs_client, per_day, force=False):
    """per_day: {date: agg}. Appends the rows of days not stored yet (force=True replaces stored days). Returns rows written."""
    written = 0
    with _lock:
        sh = gs_client.open_by_key(pt.REALTIME_SHEET_ID)
        ws = _ensure_tab(sh)
        values = ws.get_all_values()
        stored = {row[0] for i, row in enumerate(values) if i > 0 and row}
        stamp = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        for day, agg in sorted(per_day.items()):
            ds = day.strftime("%Y-%m-%d")
            if ds in stored:
                if not force:
                    continue
                for rn in sorted([i + 1 for i, row in enumerate(values) if i > 0 and row and row[0] == ds], reverse=True):
                    ws.delete_rows(rn)
            out += [[ds, name, a["n3"], a["n4"], a["p0"], a["p1"], a["tot"], stamp] for name, a in sorted(agg.items())]
        if out:
            ws.append_rows(out, value_input_option="RAW")
            written = len(out)
        _trend_cache["data"] = None
    return written


def _ensure_steps_tab(sh):
    try:
        return sh.worksheet(STEPS_SHEET)
    except Exception:
        ws = sh.add_worksheet(title=STEPS_SHEET, rows=3000, cols=len(STEPS_HEADER))
        ws.update("A1", [STEPS_HEADER], value_input_option="RAW")
        return ws


def _write_steps(gs_client, per_day, force=False):
    """per_day: {date: {group: [8 counts]}}. Same rules as _write_days (stored days skipped unless force)."""
    written = 0
    with _lock:
        ws = _ensure_steps_tab(gs_client.open_by_key(pt.REALTIME_SHEET_ID))
        values = ws.get_all_values()
        stored = {row[0] for i, row in enumerate(values) if i > 0 and row}
        stamp = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
        out = []
        for day, agg in sorted(per_day.items()):
            ds = day.strftime("%Y-%m-%d")
            if ds in stored:
                if not force:
                    continue
                for rn in sorted([i + 1 for i, row in enumerate(values) if i > 0 and row and row[0] == ds], reverse=True):
                    ws.delete_rows(rn)
            out += [[ds, g] + list(c) + [stamp] for g, c in sorted(agg.items())]
        if out:
            ws.append_rows(out, value_input_option="RAW")
            written = len(out)
        _steps_cache["data"] = None
    return written


def _steps_stored_dates(gs_client):
    try:
        ws = gs_client.open_by_key(pt.REALTIME_SHEET_ID).worksheet(STEPS_SHEET)
        return {row[0] for i, row in enumerate(ws.get_all_values()) if i > 0 and row}
    except Exception:
        return set()


def record_snapshot(gs_client, day=None, force=False):
    """Writes one row per province for `day` (default today): P0/P1/Total at that day's own 01:15 reference.
    Skips when the day is already stored unless force=True (force replaces that day's rows). Returns rows written."""
    day = day or bangkok_now().date()
    rows = pt.fetch_live_rows(gs_client)
    n = _write_days(gs_client, {day: _aggregate(rows, day)}, force=force)
    if STEPS_ENABLED:
        try:
            _write_steps(gs_client, {day: _aggregate_steps(rows, day)}, force=force)
        except Exception:
            log.exception("P0StepTrend snapshot failed (P0DailyTrend was written)")
    return n


_bf = {"running": False, "started": None, "finished": None, "done": [], "missing": [], "error": ""}


def backfill_status():
    return dict(_bf)


def backfill_from_drive(drive_service, gs_client, back_days=7):
    """Fills P0DailyTrend for past days from the ~01:15 Drive backup files (Drive keeps only a few days).
    One file at a time, each day written as soon as it is counted (the chart fills up progressively), rows dropped right after."""
    from pending_trend import find_nightly_file, download_xlsx_as_rows
    today = bangkok_now().date()
    with _lock:
        ws = _ensure_tab(gs_client.open_by_key(pt.REALTIME_SHEET_ID))
        stored = {row[0] for i, row in enumerate(ws.get_all_values()) if i > 0 and row}
    stored_steps = _steps_stored_dates(gs_client) if STEPS_ENABLED else None
    for i in range(0, back_days + 1):           # newest first: today's @01:15 is the one people need first
        day = today - timedelta(days=i)
        ds = day.strftime("%Y-%m-%d")
        if ds in stored and (stored_steps is None or ds in stored_steps):
            continue
        with pt._DAY_DL_LOCK:
            info = find_nightly_file(drive_service, day)
            rows = download_xlsx_as_rows(drive_service, info[0]) if info else None
            agg = _aggregate(rows, day) if rows else None
            steps = _aggregate_steps(rows, day) if (rows and STEPS_ENABLED) else None
            rows = None
            pt._release_memory()
        if agg:
            _write_days(gs_client, {day: agg})          # skips a day that is already stored
            if STEPS_ENABLED:
                try:
                    _write_steps(gs_client, {day: steps})
                except Exception:
                    log.exception("P0StepTrend backfill write failed for %s", ds)
            _bf["done"].append(ds)
        else:
            _bf["missing"].append(ds)
    return {"days_written": list(_bf["done"]), "missing": list(_bf["missing"])}


def start_backfill(drive_service, gs_client, back_days=7):
    """Runs backfill_from_drive in a background thread (a Drive download + parse takes far longer than one web request).
    Returns False when one is already running."""
    with _lock:
        if _bf["running"]:
            return False
        _bf.update(running=True, started=bangkok_now().strftime("%Y-%m-%d %H:%M:%S"), finished=None, done=[], missing=[], error="")

    def _run():
        try:
            backfill_from_drive(drive_service, gs_client, back_days)
        except Exception as e:
            log.exception("p0 backfill failed")
            _bf["error"] = str(e)
        finally:
            _bf["running"] = False
            _bf["finished"] = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")

    threading.Thread(target=_run, daemon=True).start()
    return True


def load_trend(gs_client, days=TREND_HISTORY_DAYS):
    """{"dates", "all"/"all_n3"/"all_n4": [P0 per day], "prov"/"prov_n3"/"prov_n4": {name: [P0 per day]}} from the P0DailyTrend tab (None for days with no row)."""
    now = time.monotonic()
    with _lock:
        c = _trend_cache
        if c["data"] is not None and now - c["ts"] < _TREND_TTL:
            return c["data"]
    sh = gs_client.open_by_key(pt.REALTIME_SHEET_ID)
    try:
        values = sh.worksheet(SNAPSHOT_SHEET).get_all_values()[1:]
    except Exception:
        values = []
    per = {}
    for row in values:
        if len(row) < 5 or not row[0]:
            continue
        try:
            per.setdefault(row[0], {})[row[1]] = (int(row[2] or 0), int(row[3] or 0))
        except ValueError:
            continue
    today = bangkok_now().date()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]
    provs = sorted({p for d in per.values() for p in d})

    def pick(d, prov, which):
        if d not in per:
            return None
        rows = per[d].values() if prov is None else [per[d].get(prov, (0, 0))]
        return sum(v[0] if which == "n3" else v[1] if which == "n4" else v[0] + v[1] for v in rows)

    out = {"dates": dates}
    for which, sfx in (("all", ""), ("n3", "_n3"), ("n4", "_n4")):
        out["all" + sfx] = [pick(d, None, which) for d in dates]
        out["prov" + sfx] = {p: [pick(d, p, which) for d in dates] for p in provs}
    with _lock:
        _trend_cache["data"] = out
        _trend_cache["ts"] = time.monotonic()
    return out


def load_steps(gs_client, days=TREND_HISTORY_DAYS):
    """{"dates": [...], "by": {group key: [[8 counts] or None per date]}} from the P0StepTrend tab (5 min cache)."""
    now = time.monotonic()
    with _lock:
        c = _steps_cache
        if c["data"] is not None and now - c["ts"] < _TREND_TTL:
            return c["data"]
    try:
        values = gs_client.open_by_key(pt.REALTIME_SHEET_ID).worksheet(STEPS_SHEET).get_all_values()[1:]
    except Exception:
        values = []
    per = {}
    for row in values:
        if len(row) < 10 or not row[0]:
            continue
        try:
            per.setdefault(row[0], {})[row[1]] = [int(x or 0) for x in row[2:10]]
        except ValueError:
            continue
    today = bangkok_now().date()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]
    groups = sorted({g for d in per.values() for g in d})
    out = {"dates": dates, "by": {g: [(per[d].get(g) or [0] * 8) if d in per else None for d in dates] for g in groups}}
    with _lock:
        _steps_cache["data"] = out
        _steps_cache["ts"] = time.monotonic()
    return out


# ── Forecast ──────────────────────────────────────────────────────────────
def _safe_steps(gs_client):
    if not STEPS_ENABLED:
        return {"dates": [], "by": {}}
    try:
        return load_steps(gs_client)
    except Exception:
        log.exception("P0StepTrend unreadable")
        return {"dates": [], "by": {}}


def build_forecast(gs_client, days=FORECAST_DAYS):
    now_dt = bangkok_now()
    ref0 = (now_dt + timedelta(days=1)).replace(hour=1, minute=15, second=0, microsecond=0)
    work_log = pt.load_work_log(gs_client)
    try:
        kasem = pt.load_kasem_comments(gs_client)
    except Exception:
        log.exception("KasemComment tab unreadable")
        kasem = {}
    # Mateline status per ticket (GGS Daily) - same lookup/rules as the P0 tab's "Mateline Status" column.
    # wait = Wait Action AND Back Date (merged on request), nf = not found in GGS Daily / lookup unavailable.
    try:
        from mateline_status import build_mateline_status_lookup
        ml_lookup = build_mateline_status_lookup(gs_client, now_dt.strftime("%Y-%m-%d"))
    except Exception:
        log.exception("forecast: Mateline lookup failed - ticket 'ml' left as not-found this round")
        ml_lookup = {}
    ml_map = {"Departed": "dep", "Arrived": "arr", "Completed": "done", "Closed": "closed", "Wait Action": "wait", "⚠️ Back Date": "wait"}
    tickets = []
    for r, code in _scoped_nsa34_rows(gs_client):
        k = pt.ticket_p0_offset(r.get("TARGETFINISH"), ref0)
        if k is None or k > days:
            continue
        tid = str(r.get("TICKETID", "")).strip()
        wl = work_log.get(tid, {})
        entry = {"SEVERITY": r.get("SEVERITY"), "TARGETFINISH": r.get("TARGETFINISH"), "plan_closed_date": wl.get("plan_closed_date", "")}
        tickets.append({
            "id": tid, "prov": _prov_name(code), "grp": str(r.get("TRUEOWNERGROUP", "") or "").strip(),
            "region": str(r.get("Region", "") or "").strip(), "nsa": str(r.get("SEVERITY", "")).strip(),
            "cat": pt._category_text({"SEVERITY": r.get("SEVERITY"), "SUBJECT": r.get("SUBJECT")}),
            "site": str(r.get("CINAME", "") or ""), "subject": str(r.get("SUBJECT", "") or ""),
            "tf": str(r.get("TARGETFINISH", "") or ""), "k": k,
            "detail": wl.get("detail", ""), "plan": wl.get("plan_closed_date", ""),
            "group_problem": wl.get("group_problem", ""), "action_team": wl.get("action_team", ""),
            "image_link": wl.get("image_link", ""), "updated_at": wl.get("updated_at", ""), "updated_by": wl.get("updated_by", ""),
            "flag": pt.flag_logic_text(entry, now_dt),
            "ml": ml_map.get((ml_lookup.get(tid.upper()) or {}).get("status_mateline", ""), "nf"),
            "kasem": (kasem.get(tid) or {}).get("comment", ""), "kasem_at": (kasem.get(tid) or {}).get("updated_at", ""),
        })
    tickets.sort(key=lambda t: (t["k"], t["prov"], t["id"]))
    provs = {}
    for t in tickets:
        p = provs.setdefault(t["prov"], {"now": 0, "add": [0] * days, "nsa": {"NSA3": [0] * (days + 1), "NSA4": [0] * (days + 1)}})
        if t["nsa"] in p["nsa"]:
            p["nsa"][t["nsa"]][t["k"]] += 1
        if t["k"] == 0:
            p["now"] += 1
        else:
            p["add"][t["k"] - 1] += 1
    insert_time = ""
    try:
        rows = pt.fetch_live_rows(gs_client)
        insert_time = str(rows[0].get("insert_time", "")) if rows else ""
    except Exception:
        pass
    return {
        "insert_time": insert_time,
        "today": now_dt.strftime("%Y-%m-%d"),
        "ref0": ref0.strftime("%Y-%m-%d"),
        "dates": [(ref0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days + 1)],
        "provinces": provs,
        "tickets": tickets,
        "trend": load_trend(gs_client),
        "steps": _safe_steps(gs_client),
        "backfill": backfill_status(),
    }
