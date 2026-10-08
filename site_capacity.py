"""Site Capacity / Coverage classification (Site_Info_NOR_2026 -> "GGS site" sheet).

Rules (all explicit requests):
  * A site is **Capacity** when ANOTHER site lies within 1 km (haversine), else **Coverage**.
  * Neighbours counted: LOC TYPE "Site" / "Site-T" only (Event, TempSite, COW,
    Relocate_Temp are never neighbours). A site's own pair (PAIR ID / IBC PAIR ID that
    points at another LOCATION ID, either direction) is never a neighbour.
  * Every row gets a classification (for plotting every site); rows with no usable
    coordinate get "" and are never guessed.
  * A ticket belongs to a site when CINAME equals a LOCATION ID, otherwise when a
    LOCATION ID that really exists in the sheet appears in SUBJECT (no pattern guessing).
  * Coverage + SEVERITY SA1/SA2/SA3 -> "P0", Coverage + SA4 -> "P1". Capacity has no flag.

Everything here is read-only and additive: a failure never raises into the callers,
the Site Type just stays blank.
"""
import logging
import math
import os
import re
import threading
import time
from collections import defaultdict

log = logging.getLogger(__name__)

SITE_SHEET_ID = os.environ.get("SITE_INFO_SHEET_ID", "1oKIM3Bpan70nfgmGlPr99ZPAq6_nacnZmw0-kw0O7AE")
SITE_TAB_NAMES = ("ชีต1", "Sheet1")  # exact names tried first, then gid 0, then the first tab
SITE_TAB_GID = 0
CACHE_TTL_SECONDS = 3600
FETCH_TIMEOUT_SECONDS = 25

NEIGHBOUR_RADIUS_M = 1000.0  # classification radius (was 500 m until 2026-10-07)
REFERENCE_RING_M = 500.0  # dashed reference ring on the zoom map only
ZOOM_RADIUS_M = 1000.0
NEIGHBOUR_LOC_TYPES = {"Site", "Site-T"}
P0_SEVERITIES = {"SA1", "SA2", "SA3"}
P1_SEVERITIES = {"SA4"}
MB_BOOKMARK = "7.MB with SA1-4"

_TOKEN_RE = re.compile(r"[A-Za-z0-9_\-]+")
_GRID = 0.012  # degrees (>= 1.25 km wide in lat, >= 1.25 km in lon at 20.5N): a +-1 cell search always covers the 1 km radius

_state = {"data": None, "ts": 0.0, "error": None, "in_flight": False}
_lock = threading.Lock()


def haversine_m(la1, lo1, la2, lo2):
    p = math.radians
    a = math.sin(p(la2 - la1) / 2) ** 2 + math.cos(p(la1)) * math.cos(p(la2)) * math.sin(p(lo2 - lo1) / 2) ** 2
    return 2 * 6371000.0 * math.asin(math.sqrt(a))


def _cell(la, lo):
    return (int(math.floor(la / _GRID)), int(math.floor(lo / _GRID)))


def _fnum(v):
    try:
        f = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return f


# Test Coverage Plot (additive): band columns grouped 700/900/1800/2100/2300/2600; tower height from TOWER TYPE
BAND_COLUMNS = {
    700: ("4G 700", "5G 700"),
    900: ("2G 900", "3G 900", "4G 900", "3G 850"),
    1800: ("2G 1800", "4G 1800", "5G 1800", "4G 1500"),
    2100: ("3G 2100", "4G 2100", "5G 2100"),
    2300: ("4G 2300", "5G 2300"),
    2600: ("4G 2600", "5G 2600"),
}
_H_RE1 = re.compile(r"(\d+(?:\.\d+)?)\s*m\b", re.I)
_H_RE2 = re.compile(r"\bH\s*(\d+(?:\.\d+)?)")


def _tower_height(text):
    m = _H_RE1.search(text or "") or _H_RE2.search(text or "")
    return float(m.group(1)) if m else 0.0


def build_site_data(values):
    """values: list of rows (list of str), first row = header. Pure function (unit-testable)."""
    if not values or len(values) < 2:
        raise ValueError("site sheet is empty")
    header = [str(h).strip().upper() for h in values[0]]

    def col(*names):
        for n in names:
            if n in header:
                return header.index(n)
        raise ValueError("site sheet is missing column %s (has: %s)" % (names[0], ", ".join(header[:12])))

    c_id, c_lat, c_lon = col("LOCATION ID"), col("LATITUDE"), col("LONGITUDE")
    c_type = header.index("LOC TYPE") if "LOC TYPE" in header else None
    c_pair = header.index("PAIR ID") if "PAIR ID" in header else None
    c_ibc = header.index("IBC PAIR ID") if "IBC PAIR ID" in header else None
    c_name = header.index("NAME_EN") if "NAME_EN" in header else None
    c_prov = header.index("PROVINCE_E") if "PROVINCE_E" in header else None
    c_dist = header.index("DISTRICT_E") if "DISTRICT_E" in header else None
    c_tower = header.index("TOWER TYPE") if "TOWER TYPE" in header else None  # Test Coverage Plot only
    c_area = header.index("AREA TYPE") if "AREA TYPE" in header else None
    c_bands = [[header.index(n) for n in names if n in header] for names in BAND_COLUMNS.values()]

    def get(row, i):
        return str(row[i]).strip() if i is not None and i < len(row) else ""

    sites, index = [], {}
    for row in values[1:]:
        sid = get(row, c_id).upper()
        if not sid or sid in index:
            continue
        la, lo = _fnum(get(row, c_lat)), _fnum(get(row, c_lon))
        if la is None or lo is None or not (5 < la < 21 and 96 < lo < 106):
            la = lo = None
        loc_type = get(row, c_type)
        s = {
            "id": sid, "la": la, "lo": lo, "type": "",
            "perm": (loc_type in NEIGHBOUR_LOC_TYPES) if c_type is not None else True,
            "loc_type": loc_type,
            "pair": {p.upper() for p in (get(row, c_pair), get(row, c_ibc)) if p},
            "name": get(row, c_name), "prov": get(row, c_prov).title(), "dist": get(row, c_dist).title(),
            "h": _tower_height(get(row, c_tower)), "area": get(row, c_area),
            "mask": sum(1 << k for k, cols in enumerate(c_bands) if any(get(row, c) == "1" for c in cols)),
        }
        index[sid] = len(sites)
        sites.append(s)

    grid = defaultdict(list)
    for i, s in enumerate(sites):
        if s["la"] is not None and s["perm"]:
            grid[_cell(s["la"], s["lo"])].append(i)

    data = {"sites": sites, "index": index, "grid": grid, "loaded_at": time.time()}
    for s in sites:
        if s["la"] is None:
            continue
        s["type"] = "Capacity" if neighbours(data, s, NEIGHBOUR_RADIUS_M, first_only=True) else "Coverage"
    data["counts"] = {
        "total": len(sites),
        "capacity": sum(1 for s in sites if s["type"] == "Capacity"),
        "coverage": sum(1 for s in sites if s["type"] == "Coverage"),
        "no_coordinate": sum(1 for s in sites if s["la"] is None),
    }
    return data


def neighbours(data, site, radius_m, first_only=False):
    """Counted neighbours of `site` within radius_m: [(other_site, metres)] sorted by distance."""
    if site["la"] is None:
        return []
    out = []
    ci, cj = _cell(site["la"], site["lo"])
    sites = data["sites"]
    for di in (-1, 0, 1):
        for dj in (-1, 0, 1):
            for k in data["grid"].get((ci + di, cj + dj), ()):
                o = sites[k]
                if o["id"] == site["id"] or o["id"] in site["pair"] or site["id"] in o["pair"]:
                    continue
                d = haversine_m(site["la"], site["lo"], o["la"], o["lo"])
                if d <= radius_m:
                    if first_only:
                        return [(o, d)]
                    out.append((o, d))
    out.sort(key=lambda x: x[1])
    return out


# ---------------------------------------------------------------------------
# Loading (bounded, cached, stale-if-error)
# ---------------------------------------------------------------------------
def _read_sheet_values(gs_client):
    sh = gs_client.open_by_key(SITE_SHEET_ID)
    ws = None
    for name in SITE_TAB_NAMES:
        try:
            ws = sh.worksheet(name)
            break
        except Exception:
            continue
    if ws is None:
        try:
            ws = sh.get_worksheet_by_id(SITE_TAB_GID)
        except Exception:
            ws = sh.get_worksheet(0)
    return ws.get_all_values()


def _load_into_cache(gs_client):
    try:
        data = build_site_data(_read_sheet_values(gs_client))
        with _lock:
            _state.update(data=data, ts=time.time(), error=None)
        log.info("site_capacity: loaded %s", data["counts"])
    except Exception as e:
        log.exception("site_capacity: load failed")
        with _lock:
            _state["error"] = str(e)
    finally:
        with _lock:
            _state["in_flight"] = False


def get_site_data(gs_client, use_cache=True, wait_seconds=FETCH_TIMEOUT_SECONDS):
    """Returns the site dataset or None (never raises). Serves stale data on error/timeout;
    a slow fetch keeps running in the background and fills the cache when it lands."""
    with _lock:
        fresh = _state["data"] is not None and (time.time() - _state["ts"]) < CACHE_TTL_SECONDS
        if use_cache and fresh:
            return _state["data"]
        start = not _state["in_flight"]
        if start:
            _state["in_flight"] = True
    if start:
        t = threading.Thread(target=_load_into_cache, args=(gs_client,), daemon=True)
        t.start()
    else:
        t = None
    deadline = time.time() + max(0, wait_seconds)
    while time.time() < deadline:
        with _lock:
            if not _state["in_flight"]:
                break
        time.sleep(0.1)
    with _lock:
        return _state["data"]  # may be stale (or None if never loaded)


def warm(gs_client):
    """Fire-and-forget preload at start-up so the first page view already has Site Type."""
    threading.Thread(target=get_site_data, args=(gs_client,), kwargs={"wait_seconds": 0}, daemon=True).start()


def last_error():
    with _lock:
        return _state["error"]


# ---------------------------------------------------------------------------
# Ticket <-> site matching and flags
# ---------------------------------------------------------------------------
def match_site_ids(data, ciname, subject):
    """All real LOCATION IDs referenced by the ticket, CINAME first, then SUBJECT, in order."""
    idx = data["index"]
    found = []

    def scan(text):
        for tok in _TOKEN_RE.findall(str(text or "").upper()):
            for cand in (tok, *re.split(r"[_\-]", tok)):
                if cand in idx and cand not in found:
                    found.append(cand)
                    break

    ci = str(ciname or "").strip().upper()
    if ci in idx:
        found.append(ci)
    else:
        scan(ciname)
    scan(subject)
    return found


def flag_for(site_type, severity):
    if site_type != "Coverage":
        return ""
    sev = str(severity or "").strip().upper()
    if sev in P0_SEVERITIES:
        return "P0"
    if sev in P1_SEVERITIES:
        return "P1"
    return ""


def bucket_for(data, ciname, subject, severity):
    """'cp0' / 'cp1' (Coverage P0 / P1), 'cap' (Capacity), 'cov' (Coverage with no P0/P1 flag, e.g. an NSA
    severity) or 'na' (no site found). Mirrors annotate_entries so the card and the tables always agree."""
    ids = match_site_ids(data, ciname, subject) if data else []
    if not ids:
        return "na"
    s = data["sites"][data["index"][ids[0]]]
    if s["type"] == "Capacity":
        return "cap"
    if s["type"] == "Coverage":
        f = flag_for("Coverage", severity)
        return "cp0" if f == "P0" else "cp1" if f == "P1" else "cov"
    return "na"  # site has no usable coordinate -> cannot be classified


def annotate_entries(entries, gs_client, severity_key="SEVERITY", wait_seconds=8):
    """Adds site_id / site_type / site_flag to each entry dict in place. Never raises."""
    try:
        data = get_site_data(gs_client, wait_seconds=wait_seconds)
    except Exception:
        log.exception("site_capacity: annotate failed")
        data = None
    for e in entries:
        e.setdefault("site_id", "")
        e.setdefault("site_type", "")
        e.setdefault("site_flag", "")
    if not data:
        return entries
    for e in entries:
        try:
            ids = match_site_ids(data, e.get("CINAME"), e.get("SUBJECT"))
            if not ids:
                continue
            s = data["sites"][data["index"][ids[0]]]
            e["site_id"] = s["id"]
            e["site_type"] = s["type"]
            e["site_flag"] = flag_for(s["type"], e.get(severity_key))
        except Exception:
            log.exception("site_capacity: annotate row failed")
    return entries


# ---------------------------------------------------------------------------
# API payloads
# ---------------------------------------------------------------------------
def build_site_map_response(gs_client):
    """Every site (compact) + the 7.MB with SA1-4 tickets matched onto sites."""
    from pending_ticket import (fetch_live_rows, _extract_region_province, PENDING_TICKET_REGIONS,
                                ALLOWED_SEVERITIES, _safe_str)
    data = get_site_data(gs_client)
    if not data:
        raise RuntimeError("Site sheet not loaded: %s" % (last_error() or "no data yet - try again in a minute"))
    sites = data["sites"]
    out_sites = [[s["id"], round(s["la"], 6), round(s["lo"], 6), 1 if s["type"] == "Capacity" else 0, s["prov"]]
                 for s in sites if s["la"] is not None]

    tickets, unmatched = [], []
    for r in fetch_live_rows(gs_client):
        if str(r.get("Region", "")).strip() not in PENDING_TICKET_REGIONS:
            continue
        sev = str(r.get("SEVERITY", "")).strip()
        if sev not in ALLOWED_SEVERITIES or str(r.get("Bookmark", "")).strip() != MB_BOOKMARK:
            continue
        _, prov = _extract_region_province(r.get("TRUEOWNERGROUP"))
        if prov is None:
            continue
        base = {"tid": _safe_str(r.get("TICKETID")), "ci": _safe_str(r.get("CINAME")),
                "subject": _safe_str(r.get("SUBJECT")), "sev": sev, "province": prov,
                "aging": _safe_str(r.get("Aging_Flag_Group"))}
        ids = [i for i in match_site_ids(data, r.get("CINAME"), r.get("SUBJECT"))
               if data["sites"][data["index"][i]]["la"] is not None]
        if not ids:
            unmatched.append(base)
            continue
        s = sites[data["index"][ids[0]]]
        base.update(site_id=s["id"], site_ids=ids, site_type=s["type"], flag=flag_for(s["type"], sev),
                    la=s["la"], lo=s["lo"], name=s["name"], site_province=s["prov"])
        tickets.append(base)
    return {
        "counts": data["counts"], "sites": out_sites, "tickets": tickets, "unmatched": unmatched,
        "loaded_at": data["loaded_at"], "bookmark": MB_BOOKMARK,
        "rules": {"radius_m": NEIGHBOUR_RADIUS_M, "zoom_m": ZOOM_RADIUS_M,
                  "neighbour_loc_types": sorted(NEIGHBOUR_LOC_TYPES)},
    }


def build_site_neighbours_response(gs_client, site_id):
    data = get_site_data(gs_client)
    if not data:
        raise RuntimeError("Site sheet not loaded: %s" % (last_error() or "no data yet - try again in a minute"))
    sid = str(site_id or "").strip().upper()
    if sid not in data["index"]:
        raise ValueError("unknown site id %s" % sid)
    s = data["sites"][data["index"][sid]]
    if s["la"] is None:
        raise ValueError("site %s has no usable coordinate" % sid)
    near = neighbours(data, s, ZOOM_RADIUS_M)
    return {
        "site": {"id": s["id"], "la": s["la"], "lo": s["lo"], "type": s["type"], "name": s["name"],
                 "province": s["prov"], "district": s["dist"], "loc_type": s["loc_type"]},
        "radius_m": NEIGHBOUR_RADIUS_M, "zoom_m": ZOOM_RADIUS_M, "ref_ring_m": REFERENCE_RING_M,
        "within_radius": sum(1 for _, d in near if d <= NEIGHBOUR_RADIUS_M),
        "within_500": sum(1 for _, d in near if d <= REFERENCE_RING_M),
        "neighbours": [{"id": o["id"], "la": o["la"], "lo": o["lo"], "m": round(d), "type": o["type"],
                        "name": o["name"], "counted": d <= NEIGHBOUR_RADIUS_M, "in_500": d <= REFERENCE_RING_M} for o, d in near],
    }
