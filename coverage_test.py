"""Test Coverage Plot - experimental tab (read-only, isolated).

Serves site records (position, tower height, band mask, province, nearest counted neighbour) built from the
GGS site sheet that site_capacity already loads and caches, plus the worst 7.MB with SA1-4 severity per site.
Nothing here changes Capacity/Coverage flags used elsewhere.
"""
import math
import threading
import logging

import site_capacity

log = logging.getLogger(__name__)
_BC = 0.1  # degrees; 3x3 cells cover >= 10 km
_lock = threading.Lock()
_nn_cache = {"loaded_at": None, "nn": {}}


def _hv_km(la1, lo1, la2, lo2):
    p = math.pi / 180.0
    x = (lo2 - lo1) * p * math.cos((la1 + la2) / 2 * p)
    y = (la2 - la1) * p
    return 6371.0 * math.hypot(x, y)


def _compute_nn(data):
    cells = {}
    for i, s in enumerate(data["sites"]):
        if s["la"] is not None and s["perm"]:
            cells.setdefault((int(s["la"] / _BC), int(s["lo"] / _BC)), []).append(i)
    sites, nn = data["sites"], {}
    for s in sites:
        if s["la"] is None:
            continue
        ci, cj = int(s["la"] / _BC), int(s["lo"] / _BC)
        best = 99.0
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for k in cells.get((ci + di, cj + dj), ()):
                    o = sites[k]
                    if o["id"] == s["id"] or o["id"] in s["pair"] or s["id"] in o["pair"]:
                        continue
                    d = _hv_km(s["la"], s["lo"], o["la"], o["lo"])
                    if d < best:
                        best = d
        nn[s["id"]] = round(best, 3)
    return nn


def build_response(gs_client):
    data = site_capacity.get_site_data(gs_client)
    if not data:
        raise RuntimeError("Site sheet not loaded: %s" % (site_capacity.last_error() or "try again in a minute"))
    with _lock:
        if _nn_cache["loaded_at"] != data["loaded_at"]:
            _nn_cache["nn"] = _compute_nn(data)
            _nn_cache["loaded_at"] = data["loaded_at"]
        nn = _nn_cache["nn"]
    sites = [[s["id"], round(s["la"], 6), round(s["lo"], 6), s["h"], s["mask"], s["prov"], s["area"],
              1 if s["perm"] else 0, nn.get(s["id"], 99.0)]
             for s in data["sites"] if s["la"] is not None]
    tickets = {}
    try:  # a ticket-side failure must never take the map down
        for t in site_capacity.build_site_map_response(gs_client).get("tickets", []):
            sid, sev = t.get("site_id"), t.get("sev")
            if sid and sev and (sid not in tickets or sev < tickets[sid]):
                tickets[sid] = sev
    except Exception:
        log.exception("coverage-test: ticket overlay unavailable")
    return {"sites": sites, "tickets": tickets, "loaded_at": data["loaded_at"],
            "bookmark": site_capacity.MB_BOOKMARK}
