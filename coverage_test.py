"""Test Coverage Plot - experimental tab (read-only, isolated).

Serves site records built from the GGS site sheet that site_capacity already loads and caches, plus the worst
7.MB with SA1-4 severity per site. Nothing here changes the Capacity/Coverage flags used on other pages.

Terrain ("ภูเขาบัง") support is optional: it needs numpy and a down-sampled DEM file
(data/dem_small.npz, or the path in env COVTEST_DEM, made with shrink_dem.py). Without either, every endpoint keeps
working in free-space mode and /api/coverage-test reports terrain=false, so the tab falls back to client-side maths.

Propagation model (same as the browser): Okumura-Hata (+clutter correction) with a vertical-pattern/tilt term,
per-band power offset and per-band RSRP threshold. Terrain adds a dominant-edge knife-edge diffraction loss
(ITU J(v), capped at 40 dB) on the path from the transmitting tower to the receiver (1.5 m above ground).
All numbers are ASSUMPTIONS (no per-sector tilt/azimuth/power data); the DEM is a 185 m max-pooled surface model.
"""
import math
import os
import threading
import logging

import site_capacity

log = logging.getLogger(__name__)

try:
    import numpy as np
except Exception:  # numpy missing -> terrain features disabled, tab still works
    np = None

BANDS = (700, 900, 1800, 2100, 2300, 2600)
FREQ = (750.0, 900.0, 1800.0, 2100.0, 2300.0, 2600.0)
MAX_KM = 12.0
NS = 48                # samples along each path for the terrain profile
TERRAIN_MIN_KM = 0.5   # shorter paths get no terrain loss
EDGE_KM = 0.2          # keep terrain samples this far from both ends of a path
LOSS_CAP_DB = 40.0
_BC = 0.1              # bucket size (deg) for neighbour search: 3x3 buckets cover >= 10 km
_AZ = 36
_RAY_STEP = 0.2
_RAY_N = 60
_DEFAULT_DEM = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "dem_small.npz")

_lock = threading.RLock()
_work = threading.Semaphore(2)   # at most two heavy numpy jobs at once
_state = {"loaded_at": None, "st": None, "building": False, "error": None}
_dem = {"tried": False, "ok": False, "E": None, "w": 0.0, "n": 0.0, "sx": 1.0, "sy": 1.0, "err": None}


# --------------------------------------------------------------------------------------------------
# basic helpers
# --------------------------------------------------------------------------------------------------
def _hv_km(la1, lo1, la2, lo2):
    p = math.pi / 180.0
    x = (lo2 - lo1) * p * math.cos((la1 + la2) / 2 * p)
    y = (la2 - la1) * p
    return 6371.0 * math.hypot(x, y)


def _load_dem():
    with _lock:
        if _dem["tried"]:
            return _dem["ok"]
        _dem["tried"] = True
        if np is None:
            _dem["err"] = "numpy is not installed"
            return False
        path = os.environ.get("COVTEST_DEM", _DEFAULT_DEM)
        try:
            z = np.load(path)
            _dem.update(E=z["elev"].astype(np.int16), w=float(z["west"]), n=float(z["north"]),
                        sx=float(z["step_x"]), sy=float(z["step_y"]), ok=True)
            log.info("coverage-test: DEM loaded %s %s", path, _dem["E"].shape)
        except Exception as e:
            _dem["err"] = "DEM not available (%s)" % e
            log.warning("coverage-test: %s", _dem["err"])
        return _dem["ok"]


def _elev(la, lo):
    E = _dem["E"]
    r = np.clip(((_dem["n"] - la) / _dem["sy"]).astype(np.int64), 0, E.shape[0] - 1)
    c = np.clip(((lo - _dem["w"]) / _dem["sx"]).astype(np.int64), 0, E.shape[1] - 1)
    v = E[r, c].astype(np.float32)
    return np.where(v < -1000, 0.0, v)


def terrain_available():
    return np is not None and _load_dem()


def _J(v):
    """ITU-R P.526 knife-edge approximation (dB), 0 when v <= -0.78."""
    with np.errstate(all="ignore"):
        j = 6.9 + 20.0 * np.log10(np.sqrt((v - 0.1) ** 2 + 1.0) + v - 0.1)
    return np.where(v > -0.78, j, 0.0)


def _terrain_loss(la1, lo1, g1, h1, la2, lo2, g2, dkm):
    """Dominant-edge loss (dB, shape (n, 6)) for paths from tower 1 (ground g1 + h1) to a receiver at point 2."""
    n = len(dkm)
    out = np.zeros((n, len(BANDS)), dtype=np.float32)
    if n == 0 or not terrain_available():
        return out
    # samples keep one DEM cell (~0.2 km) away from both ends so a neighbouring max-pooled cell cannot fake a ridge
    tmin = np.minimum(EDGE_KM / np.maximum(dkm, 1e-6), 0.45)
    t = tmin[:, None] + (1.0 - 2.0 * tmin)[:, None] * ((np.arange(NS, dtype=np.float64) + 0.5) / NS)
    lat = la1[:, None] + (la2 - la1)[:, None] * t
    lon = lo1[:, None] + (lo2 - lo1)[:, None] * t
    z = _elev(lat, lon)
    ha = (g1 + h1)[:, None]
    hb = (g2 + 1.5)[:, None]
    ray = ha + (hb - ha) * t
    d1 = dkm[:, None] * t
    d2 = dkm[:, None] * (1.0 - t)
    cl = z - (ray - d1 * d2 * 1000.0 / (2 * 6371.0 * 4.0 / 3.0))
    k = np.argmax(cl, axis=1)
    rr = np.arange(n)
    hh = cl[rr, k]
    d1k = np.maximum(d1[rr, k] * 1000.0, 1.0)
    d2k = np.maximum(d2[rr, k] * 1000.0, 1.0)
    for i, f in enumerate(FREQ):
        lam = 0.3 / (f / 1000.0)
        v = hh * np.sqrt(2.0 / lam * (1.0 / d1k + 1.0 / d2k))
        out[:, i] = np.minimum(_J(v), LOSS_CAP_DB)
    out[dkm < TERRAIN_MIN_KM] = 0.0
    return out


def _params(body):
    """Validated parameter set from the browser (arrays of 6 per band)."""
    body = body or {}

    def arr(key, default):
        v = body.get(key)
        if not isinstance(v, list) or len(v) != 6:
            return list(default)
        return [float(x) for x in v]
    u = body.get("u")
    return {
        "u": [bool(x) for x in u] if isinstance(u, list) and len(u) == 6 else [True] * 6,
        "th": arr("th", [-95, -95, -105, -105, -105, -105]),
        "off": arr("off", [0] * 6),
        "t": arr("t", [2, 3, 4, 5, 6, 6]),
        "bw": float(body.get("bw") or 7.0),
        "dh": float(body.get("dh") or 30.0),
        "cl": int(body.get("cl")) if body.get("cl") in (0, 1, 2) else 1,
        "lossp": float(body.get("lossp") or 30.0),
    }


def _rsrp(h, dkm, i, P):
    """Free-space-model RSRP (dBm) for band index i; h / dkm are arrays (or scalars)."""
    f = FREQ[i]
    dkm = np.maximum(dkm, 0.03)
    hb = np.maximum(h, 10.0)
    a = (1.1 * math.log10(f) - 0.7) * 1.5 - (1.56 * math.log10(f) - 0.8)
    pl = 69.55 + 26.16 * math.log10(f) - 13.82 * np.log10(hb) - a + (44.9 - 6.55 * np.log10(hb)) * np.log10(dkm)
    if P["cl"] == 1:
        pl = pl - (2 * math.log10(f / 28) ** 2 + 5.4)
    elif P["cl"] == 2:
        pl = pl - (4.78 * math.log10(f) ** 2 - 18.33 * math.log10(f) + 40.94)
    th = np.degrees(np.arctan(h / (dkm * 1000.0)))
    v = np.minimum(12.0 * ((th - P["t"][i]) / P["bw"]) ** 2, 20.0)
    return 32.0 - pl - v - (6.0 if P["cl"] == 0 else 0.0) + P["off"][i]


# --------------------------------------------------------------------------------------------------
# site state + neighbour pairs (built once per sheet load, in a background thread)
# --------------------------------------------------------------------------------------------------
def _build_state(data):
    sites = [s for s in data["sites"] if s["la"] is not None]
    n = len(sites)
    st = {"sites": sites, "n": n, "index": {s["id"]: i for i, s in enumerate(sites)}}
    st["la"] = np.array([s["la"] for s in sites], dtype=np.float64)
    st["lo"] = np.array([s["lo"] for s in sites], dtype=np.float64)
    st["h"] = np.array([s.get("h") or 0.0 for s in sites], dtype=np.float64)
    st["mask"] = np.array([int(s.get("mask") or 0) for s in sites], dtype=np.int64)
    st["cnt"] = np.array([1 if s["perm"] else 0 for s in sites], dtype=bool)
    st["g"] = _elev(st["la"], st["lo"]).astype(np.float64) if terrain_available() else np.zeros(n)
    ex = [set() for _ in range(n)]
    idx = st["index"]
    for i, s in enumerate(sites):
        for p in s["pair"]:
            j = idx.get(p)
            if j is not None:
                ex[i].add(j)
                ex[j].add(i)
    st["ex"] = ex
    return st


def _build_pairs(st):
    n = st["n"]
    la, lo = st["la"], st["lo"]
    buckets = {}
    for i in range(n):
        if st["cnt"][i]:
            buckets.setdefault((int(la[i] / _BC), int(lo[i] / _BC)), []).append(i)
    pa, pb, pd = [], [], []
    p = math.pi / 180.0
    for i in range(n):
        if not st["cnt"][i]:
            continue
        ci, cj = int(la[i] / _BC), int(lo[i] / _BC)
        cand = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                cand.extend(buckets.get((ci + di, cj + dj), ()))
        if not cand:
            continue
        c = np.array(cand, dtype=np.int64)
        c = c[c != i]
        if len(c) == 0:
            continue
        x = (lo[c] - lo[i]) * p * np.cos((la[c] + la[i]) / 2 * p)
        y = (la[c] - la[i]) * p
        d = 6371.0 * np.hypot(x, y)
        m = d <= MAX_KM
        c, d = c[m], d[m]
        o = np.argsort(d, kind="stable")
        pa.append(np.full(len(c), i, dtype=np.int32))
        pb.append(c[o].astype(np.int32))
        pd.append(d[o].astype(np.float32))
    st["pa"] = np.concatenate(pa) if pa else np.zeros(0, np.int32)
    st["pb"] = np.concatenate(pb) if pb else np.zeros(0, np.int32)
    st["pd"] = np.concatenate(pd) if pd else np.zeros(0, np.float32)
    N = len(st["pa"])
    st["own"] = (np.array([int(b) in st["ex"][int(a)] for a, b in zip(st["pa"], st["pb"])], dtype=bool)
                 if N else np.zeros(0, bool))
    PL = np.zeros((N, len(BANDS)), dtype=np.uint8)
    if terrain_available():
        for s0 in range(0, N, 20000):
            sl = slice(s0, min(N, s0 + 20000))
            a, b = st["pa"][sl], st["pb"][sl]
            PL[sl] = np.rint(_terrain_loss(la[a], lo[a], st["g"][a], st["h"][a], la[b], lo[b], st["g"][b],
                                           st["pd"][sl].astype(np.float64))).astype(np.uint8)
    st["PL"] = PL
    st["ord_b"] = np.argsort(st["pb"], kind="stable")
    st["pb_sorted"] = st["pb"][st["ord_b"]]
    st["rows_ready"] = True
    log.info("coverage-test: %d sites, %d pairs, terrain=%s", n, N, terrain_available())


def _ensure_state(gs_client, wait=True, data=None, tries=240):
    """Returns the prepared state, or None while it is still being built."""
    if np is None:
        return None
    if data is None:
        data = site_capacity.get_site_data(gs_client)
    if not data:
        raise RuntimeError("Site sheet not loaded: %s" % (site_capacity.last_error() or "try again in a minute"))
    with _lock:
        if _state["loaded_at"] == data["loaded_at"] and _state["st"] is not None:
            return _state["st"]
        if not _state["building"]:
            _state["building"] = True
            _state["error"] = None

            def job():
                try:
                    with _work:
                        _load_dem()
                        st = _build_state(data)
                        _build_pairs(st)
                    with _lock:
                        _state.update(st=st, loaded_at=data["loaded_at"], error=None)
                except Exception as e:
                    log.exception("coverage-test: build failed")
                    with _lock:
                        _state["error"] = str(e)
                finally:
                    with _lock:
                        _state["building"] = False
            threading.Thread(target=job, daemon=True).start()
    if wait:
        for _ in range(tries):  # default ~120 s on the very first call
            with _lock:
                if _state["error"]:
                    raise RuntimeError(_state["error"])
                if _state["loaded_at"] == data["loaded_at"] and _state["st"] is not None:
                    return _state["st"]
            threading.Event().wait(0.5)
    return None


# --------------------------------------------------------------------------------------------------
# classification
# --------------------------------------------------------------------------------------------------
def _row_margins(st, P, rows=None, terrain=True):
    """Margin (dB above the band threshold, best eligible band) for A->B rows; -999 when no band is eligible."""
    pa, pd = st["pa"], st["pd"].astype(np.float64)
    PL = st["PL"]
    if rows is not None:
        pa, pd, PL = pa[rows], pd[rows], PL[rows]
    h = np.where(st["h"][pa] > 0, st["h"][pa], P["dh"])
    mk = st["mask"][pa]
    best = np.full(len(pa), -999.0)
    for i in range(6):
        if not P["u"][i]:
            continue
        el = ((mk >> i) & 1) == 1
        if not el.any():
            continue
        m = _rsrp(h, pd, i, P) - (PL[:, i].astype(np.float64) if terrain else 0.0) - P["th"][i]
        best = np.where(el & (m > best), m, best)
    return best


def classify(gs_client, body):
    st = _ensure_state(gs_client)
    if st is None:
        return {"status": "preparing"}
    if body.get("n") is not None and int(body["n"]) != st["n"]:
        raise ValueError("site list changed on the server - reload the tab")
    P = _params(body)
    terrain = bool(body.get("terrain", True)) and terrain_available()
    cap = _cap_flags(st, P, terrain)
    return {"status": "ok", "terrain": terrain, "cap": [int(x) for x in cap],
            "counted": int(st["cnt"].sum()), "capacity": int(cap.sum())}


def _cap_flags(st, P, terrain):
    """Boolean array (per st site): reaches / is reached by another counted, non-pair site on an enabled band."""
    with _work:
        mg = _row_margins(st, P, terrain=terrain)
        reach = (mg >= 0) & (~st["own"])
        cap = np.zeros(st["n"], dtype=bool)
        cap[np.unique(st["pa"][reach])] = True
        cap[np.unique(st["pb"][reach])] = True
        cap &= st["cnt"]
    return cap


def apply_to_data(data):
    """Production switch: overwrite s['type'] of every counted site in `data` (site_capacity's dataset) with the
    signal-model result at the default parameters, terrain included. The 1 km result stays in s['type_1km'].
    Sites that are not counted keep their 1 km type. Needs numpy AND the DEM; otherwise nothing is changed
    (so a missing file can never silently turn into a no-terrain result). Returns a status dict, never raises."""
    try:
        if np is None:
            return {"status": "fallback", "reason": "numpy is not installed"}
        _load_dem()
        if not terrain_available():
            return {"status": "fallback", "reason": "DEM file not found (%s)" % (_dem.get("err") or "data/dem_small.npz")}
        st = _ensure_state(None, wait=True, data=data, tries=1200)
        if st is None:
            return {"status": "fallback", "reason": "model build did not finish"}
        cap = _cap_flags(st, _params({}), True)
        changed = 0
        for i, s in enumerate(st["sites"]):
            if not st["cnt"][i]:
                continue
            new = "Capacity" if cap[i] else "Coverage"
            if s["type"] != new:
                changed += 1
            s["type"] = new
        return {"status": "ok", "terrain": True, "changed_vs_1km": changed, "counted": int(st["cnt"].sum()),
                "capacity_counted": int(cap.sum())}
    except Exception as e:
        log.exception("coverage-test: apply_to_data failed")
        return {"status": "fallback", "reason": str(e)}


# --------------------------------------------------------------------------------------------------
# single-site detail: reach list, terrain polygon, outage loss
# --------------------------------------------------------------------------------------------------
def _margin_at(st, P, src, la, lo, terrain=True):
    """Best band margin (dB) and best RSRP (dBm) at points (la, lo arrays) for tower index src."""
    la0, lo0 = st["la"][src], st["lo"][src]
    p = math.pi / 180.0
    dkm = 6371.0 * np.hypot((lo - lo0) * p * np.cos((la + la0) / 2 * p), (la - la0) * p)
    h = st["h"][src] if st["h"][src] > 0 else P["dh"]
    n = len(la)
    if terrain and terrain_available():
        PL = _terrain_loss(np.full(n, la0), np.full(n, lo0), np.full(n, st["g"][src]), np.full(n, h),
                           la, lo, _elev(la, lo).astype(np.float64), dkm)
    else:
        PL = np.zeros((n, 6), dtype=np.float32)
    mk = int(st["mask"][src])
    mg = np.full(n, -999.0)
    dbm = np.full(n, -999.0)
    for i in range(6):
        if not P["u"][i] or not (mk >> i) & 1:
            continue
        r = _rsrp(np.full(n, h), dkm, i, P) - PL[:, i]
        mg = np.maximum(mg, r - P["th"][i])
        dbm = np.maximum(dbm, r)
    return mg, dbm


def _rays(st, P, i, terrain):
    """Per-azimuth reach (km) from site i, honouring terrain and per-band thresholds."""
    la0, lo0 = st["la"][i], st["lo"][i]
    dm = _RAY_STEP * np.arange(1, _RAY_N + 1)
    az = np.radians(np.arange(_AZ) * 360.0 / _AZ)
    lat = la0 + np.outer(np.cos(az), dm) / 111.32
    lon = lo0 + np.outer(np.sin(az), dm) / (111.32 * math.cos(math.radians(la0)))
    h = st["h"][i] if st["h"][i] > 0 else P["dh"]
    mk = int(st["mask"][i])
    reach = np.zeros((_AZ, _RAY_N), dtype=bool)
    if terrain and terrain_available():
        z = _elev(lat.ravel(), lon.ravel()).reshape(_AZ, _RAY_N).astype(np.float64)
        ha = st["g"][i] + h
        hbm = z + 1.5
        ratio = dm[:, None] / dm[None, :]
        bulge = (dm[:, None] * (dm[None, :] - dm[:, None])) / (2 * 6371.0 * 4.0 / 3.0) * 1000.0
        tri = np.triu(np.ones((_RAY_N, _RAY_N), bool), 1)
        rayh = ha + (hbm[:, None, :] - ha) * ratio[None] - bulge[None]
        cl = np.where(tri[None], z[:, :, None] - rayh, -1e9)
        k = np.argmax(cl, axis=1)
        hh = np.take_along_axis(cl, k[:, None, :], 1)[:, 0, :]
        d1 = np.maximum(dm[k] * 1000.0, 1.0)
        d2 = np.maximum((dm[None, :] - dm[k]) * 1000.0, 1.0)

        def loss_f(f):
            v = hh * np.sqrt(2.0 / (0.3 / (f / 1000.0)) * (1.0 / d1 + 1.0 / d2))
            return np.where(hh > -1e8, np.minimum(_J(v), LOSS_CAP_DB), 0.0)
    else:
        def loss_f(f):
            return 0.0
    for b in range(6):
        if not P["u"][b] or not (mk >> b) & 1:
            continue
        reach |= (_rsrp(np.full(_RAY_N, h), dm, b, P)[None, :] - loss_f(FREQ[b]) - P["th"][b]) >= 0
    out = []
    for a in range(_AZ):
        r = 0
        for m in range(_RAY_N):
            if reach[a, m]:
                r = m + 1
            elif dm[m] >= 0.6:
                break
        out.append(round(r * _RAY_STEP, 1))
    return out


def _ideal_radius(st, P, i):
    ds = np.arange(0.05, MAX_KM + 0.001, 0.05)
    h = st["h"][i] if st["h"][i] > 0 else P["dh"]
    best = np.full(len(ds), -999.0)
    for b in range(6):
        if P["u"][b] and (int(st["mask"][i]) >> b) & 1:
            best = np.maximum(best, _rsrp(np.full(len(ds), h), ds, b, P) - P["th"][b])
    ok = best >= 0
    return float(ds[ok].max()) if ok.any() else 0.0


def _outage(st, P, i, terrain, ideal):
    """Share of the area served by site i (margin >= 0) that no other counted site serves when i is down."""
    if ideal <= 0:
        return None
    la0, lo0 = st["la"][i], st["lo"][i]
    cs = math.cos(math.radians(la0))
    cell = max(0.25, ideal / 12.0)
    xs = np.arange(-ideal, ideal + 1e-9, cell)
    X, Y = np.meshgrid(xs, xs)
    m = X ** 2 + Y ** 2 <= ideal ** 2
    la = la0 + Y[m] / 111.32
    lo = lo0 + X[m] / (111.32 * cs)
    mg, _ = _margin_at(st, P, i, la, lo, terrain)
    served = mg >= 0
    if not served.any():
        return None
    la, lo = la[served], lo[served]
    covered = np.zeros(len(la), dtype=bool)
    rows = np.where(st["pa"] == i)[0]
    cand = [int(j) for j in st["pb"][rows] if st["cnt"][j]]
    for j in cand:
        if covered.all():
            break
        mj, _ = _margin_at(st, P, j, la, lo, terrain)
        covered |= mj >= 0
    tot = len(la)
    lost = int((~covered).sum())
    return {"pct": 100.0 * lost / tot, "lost": lost, "tot": tot}


def site_detail(gs_client, body):
    st = _ensure_state(gs_client)
    if st is None:
        return {"status": "preparing"}
    i = st["index"].get(str(body.get("id", "")).upper())
    if i is None:
        raise ValueError("unknown site")
    P = _params(body)
    terrain = bool(body.get("terrain", True)) and terrain_available()
    with _work:
        ideal = _ideal_radius(st, P, i)
        rays = _rays(st, P, i, terrain)
        rows_a = np.where(st["pa"] == i)[0]
        lo_b = np.searchsorted(st["pb_sorted"], i, "left")
        hi_b = np.searchsorted(st["pb_sorted"], i, "right")
        rows_b = st["ord_b"][lo_b:hi_b]
        ma = _row_margins(st, P, rows_a, terrain)
        mb = _row_margins(st, P, rows_b, terrain)
        nb = {}
        for r, m in zip(rows_a, ma):
            if st["own"][r]:
                continue
            e = nb.setdefault(int(st["pb"][r]), [float(st["pd"][r]), False, False])
            e[1] = e[1] or bool(m >= 0)
        for r, m in zip(rows_b, mb):
            if st["own"][r]:
                continue
            e = nb.setdefault(int(st["pa"][r]), [float(st["pd"][r]), False, False])
            e[2] = e[2] or bool(m >= 0)
        reach = sorted(([st["sites"][j]["id"], round(e[0], 2), e[1], e[2]] for j, e in nb.items() if e[1] or e[2]),
                       key=lambda x: x[1])
        outage = _outage(st, P, i, terrain, ideal)
    s = st["sites"][i]
    return {"status": "ok", "terrain": terrain, "id": s["id"], "ground": round(float(st["g"][i])),
            "ideal_km": round(ideal, 2), "rays_km": rays, "reach": reach[:40], "reach_n": len(reach),
            "outage": outage, "capacity": bool(reach)}


# --------------------------------------------------------------------------------------------------
# RSRP raster for the map viewport
# --------------------------------------------------------------------------------------------------
def raster(gs_client, body):
    st = _ensure_state(gs_client)
    if st is None:
        return {"status": "preparing"}
    P = _params(body)
    terrain = bool(body.get("terrain", True)) and terrain_available()
    s_, n_, w_, e_ = [float(body[k]) for k in ("south", "north", "west", "east")]
    if (n_ - s_) > 0.9 or (e_ - w_) > 0.9 or n_ <= s_ or e_ <= w_:
        return {"status": "too_large"}
    band = body.get("band")
    bsel = None if band in (None, "best") else BANDS.index(int(band))
    excl = st["index"].get(str(body.get("exclude", "")).upper())
    holeref = st["index"].get(str(body.get("hole", "")).upper())
    step = max(0.0007, math.sqrt((n_ - s_) * (e_ - w_) / 3500.0))
    lat_v = np.arange(s_, n_, step)
    lon_v = np.arange(w_, e_, step)
    LA, LO = np.meshgrid(lat_v, lon_v, indexing="ij")
    la, lo = LA.ravel(), LO.ravel()
    npt = len(la)
    pad = 0.09
    sel = np.where(st["cnt"] & (st["la"] > s_ - pad) & (st["la"] < n_ + pad) &
                   (st["lo"] > w_ - pad) & (st["lo"] < e_ + pad))[0]
    best_dbm = np.full(npt, -999.0)
    best_mg = np.full(npt, -999.0)
    p = math.pi / 180.0
    with _work:
        for j in sel:
            if excl is not None and j == excl:
                continue
            dkm = 6371.0 * np.hypot((lo - st["lo"][j]) * p * np.cos((la + st["la"][j]) / 2 * p), (la - st["la"][j]) * p)
            m = dkm <= 9.0
            if not m.any():
                continue
            la_m, lo_m, d_m = la[m], lo[m], dkm[m]
            h = st["h"][j] if st["h"][j] > 0 else P["dh"]
            if terrain:
                PL = _terrain_loss(np.full(len(d_m), st["la"][j]), np.full(len(d_m), st["lo"][j]),
                                   np.full(len(d_m), st["g"][j]), np.full(len(d_m), h), la_m, lo_m,
                                   _elev(la_m, lo_m).astype(np.float64), d_m)
            else:
                PL = np.zeros((len(d_m), 6), dtype=np.float32)
            mk = int(st["mask"][j])
            bd = np.full(len(d_m), -999.0)
            bm = np.full(len(d_m), -999.0)
            for b in range(6):
                if not P["u"][b] or not (mk >> b) & 1:
                    continue
                r = _rsrp(np.full(len(d_m), h), d_m, b, P) - PL[:, b]
                bm = np.maximum(bm, r - P["th"][b])
                if bsel is None or bsel == b:
                    bd = np.maximum(bd, r)
            idx = np.where(m)[0]
            best_dbm[idx] = np.maximum(best_dbm[idx], bd)
            best_mg[idx] = np.maximum(best_mg[idx], bm)
        hole = np.zeros(npt, dtype=bool)
        if holeref is not None:
            hm, _ = _margin_at(st, P, holeref, la, lo, terrain)
            hole = (hm >= 0) & (best_mg < 0)
    v = np.where(best_dbm > -140, np.clip(np.rint(best_dbm), -126, 30), -128).astype(np.int16)
    v = np.where(hole, 127, v)
    return {"status": "ok", "terrain": terrain, "south": float(lat_v[0]) if len(lat_v) else s_,
            "west": float(lon_v[0]) if len(lon_v) else w_, "step": step, "ny": int(len(lat_v)),
            "nx": int(len(lon_v)), "v": [int(x) for x in v], "sites": int(len(sel))}


# --------------------------------------------------------------------------------------------------
# data for the page
# --------------------------------------------------------------------------------------------------
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


_nn_cache = {"loaded_at": None, "nn": {}}


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
    ter = terrain_available()
    if ter:  # start building pairs/terrain in the background so the first classify call is quick
        try:
            _ensure_state(gs_client, wait=False)
        except Exception:
            log.exception("coverage-test: warm-up failed")
    return {"sites": sites, "tickets": tickets, "loaded_at": data["loaded_at"],
            "bookmark": site_capacity.MB_BOOKMARK, "terrain": bool(ter),
            "terrain_note": None if ter else (_dem["err"] or "numpy is not installed")}
