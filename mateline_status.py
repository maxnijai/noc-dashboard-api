"""GGS Daily mateline status: matches NOC P0 tickets against the external
MatelineX field-team tracking sheet (a separate Google Sheet, not
REALTIME_SHEET_ID) by TICKETID == Source Ticket ID or External TicketID,
and derives a "has the field team touched this today" status from the
Departed/Arrived/Completed/Closed timestamp columns.
"""

import logging
import threading
import re
import time
from datetime import datetime

log = logging.getLogger(__name__)

# Source switched (2026-11-10) from the old "GGS Daily" sheet to the "Console OWS APIs" sheet (tab "data"),
# which its owner refreshes about every 5 minutes. Same column names for everything read here and in ofc_monitor.
# Old source for rollback: sheet "1PsJnXf8X7rBbA6G96L0ojE_ioby4mKqYcJQLp7fFiOw", tab "Daily" (gid 1504893566).
GGS_DAILY_SHEET_ID = "1oJYXghJTbdtg_eRxLizZQ9sENcloU-dXWeSGSUBgBIE"
GGS_DAILY_TAB = "data"
GGS_DAILY_GID = 1486319159   # preferred over the tab name, so a renamed tab keeps working
GGS_REQUIRED_HEADERS = ("Source Ticket ID", "External TicketID", "Status", "Departed", "Arrived", "Completed", "Closed")

# The 4 milestone columns checked for "did something happen today", in the
# order their timestamps would naturally occur (Departed -> Arrived ->
# Completed -> Closed) - not used for tie-breaking (the actual latest
# timestamp wins that), just documentation of the expected sequence.
MATELINE_DIMENSIONS = ["Departed", "Arrived", "Completed", "Closed"]

WAIT_ACTION_LABEL = "Wait Action"
BACK_DATE_LABEL = "⚠️ Back Date"

_ggs_cache = {"data": None, "ts": 0, "retry_after": 0}  # retry_after: after a failed refresh, serve the old copy until then instead of re-waiting on a slow sheet
_ggs_lock = threading.Lock()
GGS_CACHE_TTL_SECONDS = 300  # 5 min - external field-ops sheet, doesn't need second-by-second freshness
GGS_FETCH_TIMEOUT_SECONDS = 15
GGS_RETRY_BACKOFF_SECONDS = 60  # after a failed/timed-out refresh, keep serving the last good copy this long before trying the slow sheet again


def _parse_ggs_dt(s):
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _fetch_ggs_daily_rows_raw(gs_client):
    sh = gs_client.open_by_key(GGS_DAILY_SHEET_ID)
    try:
        ws = sh.get_worksheet_by_id(GGS_DAILY_GID)
    except Exception:
        ws = sh.worksheet(GGS_DAILY_TAB)
    rows = ws.get_all_values()
    # The source is rewritten every ~5 min: a read that lands mid-rewrite can come back empty or half-written.
    # Reject those so the last good copy keeps being served (stale-if-error below) instead of showing every ticket as "not found".
    header = [str(h).strip() for h in (rows[0] if rows else [])]
    missing = [h for h in GGS_REQUIRED_HEADERS if h not in header]
    if len(rows) < 2 or missing:
        raise ValueError(f"GGS source looks incomplete (rows={len(rows)}, missing headers={missing})")
    prev = _ggs_cache.get("data")
    if prev and len(prev) > 200 and len(rows) < 0.5 * len(prev):
        raise ValueError(f"GGS source shrank from {len(prev)} to {len(rows)} rows - treating it as a partial write")
    return rows


def _fetch_ggs_daily_rows_bounded(gs_client, timeout_seconds):
    """Runs _fetch_ggs_daily_rows_raw on a daemon thread and waits up to
    timeout_seconds for it - a plain daemon Thread rather than
    concurrent.futures.ThreadPoolExecutor deliberately: that pool's
    worker threads are NOT daemon threads by default, so a call that
    genuinely never returns would sit there non-daemon forever, which
    can itself interfere with clean process shutdown/restart. A daemon
    thread never blocks that, whether it ever finishes or not - if it
    times out here, it keeps running quietly in the background and is
    simply abandoned; the interpreter can still exit normally around it."""
    result, error, done = {}, {}, threading.Event()
    state = {"abandoned": False}

    def _run():
        try:
            result["rows"] = _fetch_ggs_daily_rows_raw(gs_client)
        except Exception as e:
            error["exc"] = e
        finally:
            done.set()
        # The caller gave up waiting (timed out) but this read kept going and has now
        # finished: store it, so the very next request is served instantly from cache
        # instead of waiting on the slow sheet all over again. (Only after done.set(),
        # and only if abandoned - the waiting caller holds _ggs_lock while it waits, so
        # taking the lock before done.set() would deadlock.)
        if "rows" in result and state["abandoned"]:
            if _ggs_lock.acquire(timeout=10):
                try:
                    _ggs_cache["data"] = result["rows"]
                    _ggs_cache["ts"] = time.monotonic()
                    _ggs_cache["retry_after"] = 0
                    log.info("GGS Daily: late fetch finished after the caller timed out - cache refreshed")
                finally:
                    _ggs_lock.release()

    threading.Thread(target=_run, daemon=True, name="ggs-fetch").start()
    if not done.wait(timeout=timeout_seconds):
        state["abandoned"] = True
        if done.is_set() and "rows" in result:   # finished in the instant between the timeout and this line
            return result["rows"]
        raise TimeoutError(f"GGS Daily sheet fetch timed out after {timeout_seconds}s")
    if "exc" in error:
        raise error["exc"]
    return result["rows"]


def fetch_ggs_daily_rows(gs_client, use_cache=True):
    now = time.monotonic()
    if use_cache:
        with _ggs_lock:
            if _ggs_cache["data"] is not None and (now - _ggs_cache["ts"]) < GGS_CACHE_TTL_SECONDS:
                return _ggs_cache["data"]
            # A recent refresh failed: keep serving the last good copy for a minute rather than
            # making every request wait out the same slow sheet (up to 15s each).
            if _ggs_cache["data"] is not None and now < _ggs_cache.get("retry_after", 0):
                return _ggs_cache["data"]
            # Fetch WHILE STILL HOLDING the lock (see pending_ticket.fetch_live_rows
            # for the full rationale) - a concurrent caller hitting a cold cache
            # at the same moment blocks here and reuses this result instead of
            # making its own separate Sheets read.
            #
            # Bounded with a hard timeout (added after a real production hang:
            # this network call had no timeout of its own, so when it stalled -
            # a slow response from Google, a flaky connection, anything - it
            # held _ggs_lock forever, and every future call to this one tab
            # then blocked on that same lock indefinitely too, with no way out
            # short of restarting the server; every OTHER tab stayed fine
            # since none of them touch this lock). Timing out here instead
            # raises a normal, catchable exception and releases the lock
            # right away, so the next request gets a fresh, bounded attempt
            # rather than joining a queue that never moves.
            try:
                rows = _fetch_ggs_daily_rows_bounded(gs_client, GGS_FETCH_TIMEOUT_SECONDS)
            except Exception as e:
                if isinstance(e, TimeoutError):
                    log.error("GGS Daily sheet fetch timed out after %ss - giving up on this attempt", GGS_FETCH_TIMEOUT_SECONDS)
                else:
                    log.exception("GGS Daily sheet fetch failed")
                # Stale-if-error: an out-of-date copy beats an error page for a field-ops sheet.
                # Only when there has never been a successful read does the error still surface.
                if _ggs_cache["data"] is not None:
                    _ggs_cache["retry_after"] = time.monotonic() + GGS_RETRY_BACKOFF_SECONDS
                    log.warning("GGS Daily: serving the last good copy (%.0fs old) after a failed refresh", time.monotonic() - _ggs_cache["ts"])
                    return _ggs_cache["data"]
                raise
            _ggs_cache["data"] = rows
            _ggs_cache["ts"] = time.monotonic()
            _ggs_cache["retry_after"] = 0
            return rows
    return _fetch_ggs_daily_rows_raw(gs_client)


_LOG_EVENT_RE = re.compile(r"(?m)^\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (Depart|Arrive|Complete|Close)\w* ?:")
_LOG_EVENT_LABEL = {"Depart": "Departed", "Arrive": "Arrived", "Complete": "Completed", "Close": "Closed"}


def _log_events_today(text, today_str):
    """[(datetime, 'Departed'|'Arrived'|'Completed'|'Closed')] for the events of today found in a Summary Details log."""
    out = []
    for m in _LOG_EVENT_RE.finditer(text or ""):
        if m.group(1)[:10] == today_str:
            dt = _parse_ggs_dt(m.group(1))
            if dt is not None:
                out.append((dt, _LOG_EVENT_LABEL[m.group(2)]))
    return out


def build_mateline_status_lookup(gs_client, today_str):
    """Returns {ticket_id_upper: {"status_mateline", "mateline_wo_status"}},
    keyed by both Source Ticket ID and External TicketID (whichever a NOC
    TICKETID matches) from the GGS Daily sheet.

    status_mateline: the name of whichever of Departed/Arrived/Completed/
    Closed has today's date and the latest timestamp among those that do;
    "⚠️ Back Date" if none are today but at least one has a past date;
    "Wait Action" if none of the 4 have any value at all.
    mateline_wo_status: the sheet's own raw Status column (K), unmodified.

    Returns {} (rather than raising) if the GGS Daily sheet can't be read
    right now - a P0 ticket list is useful even with this one column
    blank, and this external sheet is the single point in the whole P0
    page that isn't shared/warmed by any other tab, so it's the one most
    likely to hit a cold-cache timeout on a bad network moment.
    """
    try:
        rows = fetch_ggs_daily_rows(gs_client)
    except Exception:
        log.exception("build_mateline_status_lookup: could not read GGS Daily sheet - returning empty lookup")
        return {}
    if not rows:
        return {}
    header = rows[0]
    col = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def get(row, name):
        i = col.get(name)
        if i is None or i >= len(row):
            return ""
        return row[i]

    lookup = {}
    best_dt_by_key = {}
    for row in rows[1:]:
        if not row or not any(row):
            continue
        source_tid = get(row, "Source Ticket ID").strip()
        external_tid = get(row, "External TicketID").strip()
        if not source_tid and not external_tid:
            continue

        today_candidates = []  # (dt, label) - only entries dated today
        for label in MATELINE_DIMENSIONS:
            dt = _parse_ggs_dt(get(row, label))
            if dt is not None and dt.strftime("%Y-%m-%d") == today_str:
                today_candidates.append((dt, label))
        # The work-order event log (Summary Details) is the field team's own record: use its events of today too,
        # so a stamp that the Departed/Arrived/... columns miss (or hold from an earlier trip) cannot hide today's progress.
        for dt, label in _log_events_today(get(row, "Summary Details"), today_str):
            today_candidates.append((dt, label))
        best_dt = None
        if today_candidates:
            today_candidates.sort(key=lambda c: c[0], reverse=True)
            best_dt, status_mateline = today_candidates[0]
        else:
            status_mateline = WAIT_ACTION_LABEL   # nothing today (a past-date stamp counts as Wait Action too - "Back Date" is merged into it)

        entry = {
            "status_mateline": status_mateline,
            "mateline_wo_status": get(row, "Status").strip(),
        }
        for tid in (source_tid, external_tid):
            if not tid:
                continue
            key = tid.strip().upper()
            prev = best_dt_by_key.get(key)
            # several work orders for one ticket: keep the one with the most recent move today, otherwise the later row
            if prev is None or (best_dt is not None and best_dt >= prev) or (best_dt is None and prev is None):
                lookup[key] = entry
                if best_dt is not None:
                    best_dt_by_key[key] = best_dt
            elif key not in lookup:
                lookup[key] = entry
    return lookup



def ggs_source_info(gs_client):
    """Which sheet the Mateline columns (WO Status / Status Mateline, Pending Ticket, Online SLA, Flag Priority, Log) are read from right now."""
    rows = fetch_ggs_daily_rows(gs_client)
    header = [str(h).strip() for h in (rows[0] if rows else [])]
    ins = header.index("insert_time") if "insert_time" in header else None
    times = [r[ins] for r in rows[1:] if ins is not None and ins < len(r) and r[ins]]
    with _ggs_lock:
        age = round(time.monotonic() - _ggs_cache["ts"]) if _ggs_cache.get("data") is not None else None
    return {"sheet_id": GGS_DAILY_SHEET_ID, "tab": GGS_DAILY_TAB, "gid": GGS_DAILY_GID, "rows": max(len(rows) - 1, 0),
            "newest_insert_time": max(times) if times else "", "oldest_insert_time": min(times) if times else "", "cache_age_seconds": age,
            "legacy_sheet_in_use": False, "legacy_only_for": "OFC Monitor (Skill/Region filter)"}


_LOG_BLOCK_SPLIT = re.compile(r"\n-{20,}[ \t]*\n?")
_LOG_HEAD = re.compile(r"^\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (.+?) ?: ?(.*)$")


def _log_events(text):
    """[(datetime, action, reason)] of a Summary Details log, in the order written (action e.g. 'Depart', 'Pause SLA')."""
    out = []
    for blk in _LOG_BLOCK_SPLIT.split(str(text or "").replace("\r", "")):
        lines = blk.strip().split("\n")
        m = _LOG_HEAD.match(lines[0]) if lines and lines[0] else None
        if not m:
            continue
        dt = _parse_ggs_dt(m.group(1))
        if dt is None:
            continue
        reason = ""
        for l in lines[1:]:
            l = l.strip()
            if l.startswith("Reason :"):
                reason = l[8:].strip()
        out.append((dt, m.group(2).strip(), reason))
    return out


def build_ggs_progress_lookup(gs_client):
    """{TICKET_ID_UPPER: {"departed","arrived","completed","closed","pause_resume"}} from the new GGS "data" tab.
    The date columns win unless the work-order log (Summary Details) holds a later stamp of the same step; pause_resume is the
    sheet's own Pause/Resume Time column, else the last "Pause SLA"/"Resume SLA" event of the log."""
    try:
        rows = fetch_ggs_daily_rows(gs_client)
    except Exception:
        log.exception("build_ggs_progress_lookup: could not read the GGS sheet")
        return {}
    if not rows:
        return {}
    col = {name.strip(): i for i, name in enumerate(rows[0]) if name.strip()}

    def get(row, name):
        i = col.get(name)
        return row[i] if i is not None and i < len(row) else ""

    stamp = lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S")
    step = {"Depart": "departed", "Arrive": "arrived", "Complete": "completed", "Close": "closed"}
    out, newest = {}, {}
    for row in rows[1:]:
        if not row or not any(row):
            continue
        res = {k: get(row, c).strip() for k, c in (("departed", "Departed"), ("arrived", "Arrived"), ("completed", "Completed"), ("closed", "Closed"))}
        pause = ""
        last = None
        for dt, act, reason in _log_events(get(row, "Summary Details")):
            key = next((v for k, v in step.items() if act.startswith(k)), None)
            if key and (not _parse_ggs_dt(res[key]) or dt > _parse_ggs_dt(res[key])):
                res[key] = stamp(dt)
            if act in ("Pause SLA", "Resume SLA"):
                pause = ("⏸ พัก SLA " if act == "Pause SLA" else "▶ เริ่ม SLA ต่อ ") + dt.strftime("%Y-%m-%d %H:%M") + (" · " + reason if reason and act == "Pause SLA" else "")
            last = dt if last is None or dt > last else last
        res["pause_resume"] = get(row, "Pause/Resume Time").strip() or pause
        rank = last or datetime.min
        for tid in (get(row, "Source Ticket ID"), get(row, "External TicketID")):
            tid = tid.strip().upper()
            if tid and (tid not in out or rank >= newest[tid]):
                out[tid], newest[tid] = res, rank
    return out


def build_ticket_log(gs_client, ticket_id):
    """Summary Details (the work-order event log) of every Work Order whose Source Ticket ID / External TicketID /
    Work Order ID equals ticket_id, from the same cached copy of the new GGS "data" tab used for Mateline Status.
    -> list of {wo, status, team, subject, summary}; [] when the ticket is not in the sheet (e.g. already closed)."""
    tid = str(ticket_id or "").strip().upper()
    if not tid:
        return []
    rows = fetch_ggs_daily_rows(gs_client)
    if not rows:
        return []
    col = {name.strip(): i for i, name in enumerate(rows[0]) if name.strip()}

    def get(row, name):
        i = col.get(name)
        return row[i] if i is not None and i < len(row) else ""

    out = []
    for row in rows[1:]:
        if not row:
            continue
        keys = {get(row, "Source Ticket ID").strip().upper(), get(row, "External TicketID").strip().upper(), get(row, "Work Order ID").strip().upper()}
        if tid in keys:
            out.append({"wo": get(row, "Work Order ID").strip(), "status": get(row, "Status").strip(), "team": get(row, "Team Full Name").strip(),
                        "subject": get(row, "Subject").strip(), "summary": get(row, "Summary Details")})
    return out


# ---------------------------------------------------------------------------
# Legacy "GGS Daily" sheet - kept ONLY for OFC Monitor.
# OFC Monitor filters on Skill == "OFC" and Region == "NOR", values that exist in the old "Daily" tab but not in the
# new "Console OWS APIs" data tab (there Skill is e.g. "NOP" and Region looks like "NOE_NKR"). Until that page is
# ported to the new source it keeps reading the old sheet, with its own cache so the two never mix.
# ---------------------------------------------------------------------------
LEGACY_DAILY_SHEET_ID = "1PsJnXf8X7rBbA6G96L0ojE_ioby4mKqYcJQLp7fFiOw"
LEGACY_DAILY_TAB = "Daily"
_legacy_cache = {"data": None, "ts": 0.0}
_legacy_lock = threading.Lock()


def fetch_legacy_daily_rows(gs_client, use_cache=True):
    def _raw():
        return gs_client.open_by_key(LEGACY_DAILY_SHEET_ID).worksheet(LEGACY_DAILY_TAB).get_all_values()

    def _bounded():
        result, error, done = {}, {}, threading.Event()

        def _run():
            try:
                result["rows"] = _raw()
            except Exception as e:
                error["exc"] = e
            finally:
                done.set()
        threading.Thread(target=_run, daemon=True, name="ggs-legacy-fetch").start()
        if not done.wait(timeout=GGS_FETCH_TIMEOUT_SECONDS):
            raise TimeoutError(f"legacy GGS Daily fetch timed out after {GGS_FETCH_TIMEOUT_SECONDS}s")
        if "exc" in error:
            raise error["exc"]
        return result["rows"]

    if not use_cache:
        return _bounded()
    with _legacy_lock:
        now = time.monotonic()
        if _legacy_cache["data"] is not None and now - _legacy_cache["ts"] < GGS_CACHE_TTL_SECONDS:
            return _legacy_cache["data"]
        try:
            rows = _bounded()
        except Exception:
            log.exception("legacy GGS Daily fetch failed")
            if _legacy_cache["data"] is not None:   # stale-if-error, same as the main reader
                return _legacy_cache["data"]
            raise
        _legacy_cache["data"], _legacy_cache["ts"] = rows, time.monotonic()
        return rows
