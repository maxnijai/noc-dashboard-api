"""Data for the per-Bookmark pages of the "Present" slideshow. Read-only: reuses the Pending Ticket entries
(Bookmark / Aging / Site Type / Detail / Plan Closed Date) and the GGS Daily travel stage that the Focus P0 drill-down
already uses. Cached for 60 s so several people pressing Present do not each re-read the sheets."""
import threading
import time

import pending_ticket as pt
from ticket_views import row_matches_view

KEYS = ["MB", "FBB", "NW_NSA12", "NSA34"]
TRAVEL = ["wait", "dep", "arr", "done", "closed"]  # ยังไม่เดินทาง, เดินทางแล้ว, ถึงไซต์แล้ว, Complete, Closed
OVER_GROUPS = [3, 2, 1, 0]  # indexes into AGING_ORDER: 4) <3d, 3) <7d, 2) <30d, 1) >30d (shown in that order)
MAX_PER_GROUP = 400
PROV_THAI = {"CMI": "เชียงใหม่", "CRI": "เชียงราย", "LPG": "ลำปาง", "LPN": "ลำพูน", "MHS": "แม่ฮ่องสอน", "NAN": "น่าน",
             "PHE": "แพร่", "PYO": "พะเยา", "KPP": "กำแพงเพชร", "PCB": "เพชรบูรณ์", "PCT": "พิจิตร", "PSN": "พิษณุโลก",
             "SKT": "สุโขทัย", "TAK": "ตาก", "UTR": "อุตรดิตถ์"}
_lock = threading.Lock()
_cache = {"v": None, "ts": 0.0}


def _s(v):
    return "" if v is None else str(v).strip()


def build(gs_client, use_cache=True):
    now = time.monotonic()
    with _lock:
        if use_cache and _cache["v"] is not None and now - _cache["ts"] < 60:
            return _cache["v"]
    entries = pt._fetch_full_ticket_entries(gs_client)
    try:
        extras = pt._dd_daily_extras(gs_client)
    except Exception:
        extras = {}
    order = list(pt.AGING_ORDER)
    out = {"aging_order": order, "groups": {}}
    for key in KEYS:
        rows = [e for e in entries if row_matches_view(e, key)]
        allowed = pt._p0_allowed(key)
        travel = {t: 0 for t in TRAVEL}
        aging = [0] * len(order)
        prov = {}
        p0 = 0
        det = {str(g): [] for g in OVER_GROUPS}
        for e in rows:
            st = (extras.get(_s(e.get("TICKETID")).upper()) or {}).get("st") or "wait"
            travel[st if st in travel else "wait"] += 1
            try:
                ai = order.index(_s(e.get("Aging_Flag_Group")))
            except ValueError:
                ai = -1
            if ai >= 0:
                aging[ai] += 1
            code = (pt._extract_region_province(e.get("TRUEOWNERGROUP"))[1] or "")
            if code:
                prov[code] = prov.get(code, 0) + 1
            if _s(e.get("priority")) in allowed:
                p0 += 1
            if key != "NSA34" and ai in OVER_GROUPS and len(det[str(ai)]) < MAX_PER_GROUP:
                det[str(ai)].append({"id": _s(e.get("TICKETID")), "ci": _s(e.get("CINAME")), "sj": _s(e.get("SUBJECT")),
                                     "st": _s(e.get("site_type")), "dt": _s(e.get("detail")),
                                     "pl": _s(e.get("plan_closed_date")), "od": bool(e.get("plan_closed_overdue")),
                                     "d": float(e.get("over_sla_day") or 0)})
        for g in det.values():
            g.sort(key=lambda t: -t["d"])
        top = sorted(prov.items(), key=lambda kv: -kv[1])
        out["groups"][key] = {"now": len(rows), "p0": p0, "travel": travel, "aging": aging,
                              "prov": [[PROV_THAI.get(c, c), n] for c, n in top],
                              "over": det if key != "NSA34" else None}
    out["generated_at"] = pt.bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    with _lock:
        _cache["v"], _cache["ts"] = out, time.monotonic()
    return out
