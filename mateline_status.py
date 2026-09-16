"""GGS Daily mateline status: matches NOC P0 tickets against the external
MatelineX field-team tracking sheet (a separate Google Sheet, not
REALTIME_SHEET_ID) by TICKETID == Source Ticket ID or External TicketID,
and derives a "has the field team touched this today" status from the
Departed/Arrived/Completed/Closed timestamp columns.
"""

import logging
import threading
import time
from datetime import datetime

log = logging.getLogger(__name__)

GGS_DAILY_SHEET_ID = "1PsJnXf8X7rBbA6G96L0ojE_ioby4mKqYcJQLp7fFiOw"
GGS_DAILY_TAB = "Daily"

# The 4 milestone columns checked for "did something happen today", in the
# order their timestamps would naturally occur (Departed -> Arrived ->
# Completed -> Closed) - not used for tie-breaking (the actual latest
# timestamp wins that), just documentation of the expected sequence.
MATELINE_DIMENSIONS = ["Departed", "Arrived", "Completed", "Closed"]

WAIT_ACTION_LABEL = "Wait Action"
BACK_DATE_LABEL = "⚠️ Back Date"

_ggs_cache = {"data": None, "ts": 0}
_ggs_lock = threading.Lock()
GGS_CACHE_TTL_SECONDS = 300  # 5 min - external field-ops sheet, doesn't need second-by-second freshness
GGS_FETCH_TIMEOUT_SECONDS = 15


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
    ws = sh.worksheet(GGS_DAILY_TAB)
    return ws.get_all_values()


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

    def _run():
        try:
            result["rows"] = _fetch_ggs_daily_rows_raw(gs_client)
        except Exception as e:
            error["exc"] = e
        finally:
            done.set()

    threading.Thread(target=_run, daemon=True, name="ggs-fetch").start()
    if not done.wait(timeout=timeout_seconds):
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
            except TimeoutError:
                log.error("GGS Daily sheet fetch timed out after %ss - giving up on this attempt", GGS_FETCH_TIMEOUT_SECONDS)
                raise
            _ggs_cache["data"] = rows
            _ggs_cache["ts"] = time.monotonic()
            return rows
    return _fetch_ggs_daily_rows_raw(gs_client)


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
    for row in rows[1:]:
        if not row or not any(row):
            continue
        source_tid = get(row, "Source Ticket ID").strip()
        external_tid = get(row, "External TicketID").strip()
        if not source_tid and not external_tid:
            continue

        today_candidates = []  # (dt, label) - only entries dated today
        latest_any_dt = None
        for label in MATELINE_DIMENSIONS:
            dt = _parse_ggs_dt(get(row, label))
            if dt is None:
                continue
            if latest_any_dt is None or dt > latest_any_dt:
                latest_any_dt = dt
            if dt.strftime("%Y-%m-%d") == today_str:
                today_candidates.append((dt, label))

        if today_candidates:
            today_candidates.sort(key=lambda c: c[0], reverse=True)
            status_mateline = today_candidates[0][1]
        elif latest_any_dt is not None:
            status_mateline = BACK_DATE_LABEL
        else:
            status_mateline = WAIT_ACTION_LABEL

        entry = {
            "status_mateline": status_mateline,
            "mateline_wo_status": get(row, "Status").strip(),
        }
        for tid in (source_tid, external_tid):
            if tid:
                lookup[tid.strip().upper()] = entry
    return lookup
