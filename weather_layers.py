"""Weather layers for the NOR Monitoring Leaflet maps: realtime rain radar
(RainViewer) and rainfall accumulation (Open-Meteo). Kept as its own
module, separate from flood_nan.py, so this stays fully independent of
that already-large module - nothing here touches ticket data, sites, or
any existing NOR Monitoring logic.

RainViewer (realtime radar) needs NO backend involvement at all - its
public JSON/tile endpoints are meant to be called directly from the
browser (no API key, no CORS restriction), so that part is pure frontend
(see the dashboard template). This module only covers the OTHER half:
rainfall accumulation, which needs a backend because Open-Meteo has no
"give me a raster over this area" endpoint - it's fundamentally a
point-by-point API (query specific lat/lon coordinates, get a time
series back). To show something meaningful on a map, this samples one
point per province (province capital coordinates, a reasonable stand-in
for "how much has it rained in this province") and renders those as
colored markers rather than a smooth heatmap - an honest reflection of
what the API can actually provide, not an invented raster.
"""

import json
import logging
import threading
import time
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# One representative point per province (approximate provincial capital
# coordinates) - NOR1 (upper north) + NOR2 (lower north), matching the
# same 15-province scope the rest of NOR Monitoring covers.
PROVINCE_POINTS = [
    {"code": "CMI", "name": "เชียงใหม่", "lat": 18.7883, "lon": 98.9853},
    {"code": "CRI", "name": "เชียงราย", "lat": 19.9105, "lon": 99.8406},
    {"code": "MHS", "name": "แม่ฮ่องสอน", "lat": 19.3020, "lon": 97.9654},
    {"code": "LPG", "name": "ลำปาง", "lat": 18.2888, "lon": 99.4909},
    {"code": "LPN", "name": "ลำพูน", "lat": 18.5744, "lon": 99.0087},
    {"code": "NAN", "name": "น่าน", "lat": 18.7756, "lon": 100.7730},
    {"code": "PHE", "name": "แพร่", "lat": 18.1445, "lon": 100.1405},
    {"code": "PYO", "name": "พะเยา", "lat": 19.1664, "lon": 99.8998},
    {"code": "KPP", "name": "กำแพงเพชร", "lat": 16.4827, "lon": 99.5226},
    {"code": "PCB", "name": "เพชรบูรณ์", "lat": 16.4189, "lon": 101.1591},
    {"code": "PCT", "name": "พิจิตร", "lat": 16.4429, "lon": 100.3487},
    {"code": "PSN", "name": "พิษณุโลก", "lat": 16.8211, "lon": 100.2659},
    {"code": "SKT", "name": "สุโขทัย", "lat": 17.0068, "lon": 99.8265},
    {"code": "TAK", "name": "ตาก", "lat": 16.8697, "lon": 99.1257},
    {"code": "UTR", "name": "อุตรดิตถ์", "lat": 17.6200, "lon": 100.0993},
]

# Rainfall accumulation color scale (mm), roughly matching common
# meteorological conventions - light blue for light rain up through
# purple for very heavy accumulation.
ACCUMULATION_BREAKS = [
    (0, 0.1, "#00000000"),       # 0mm - no marker drawn (frontend skips these)
    (0.1, 10, "#a8d8ff"),
    (10, 25, "#4aa3f5"),
    (25, 50, "#1f6feb"),
    (50, 100, "#6b3fa0"),
    (100, None, "#c0388a"),
]

_accumulation_cache = {}  # hours -> {"data": [...], "ts": float}
_accumulation_lock = threading.Lock()
ACCUMULATION_CACHE_TTL_SECONDS = 900  # 15 min - rainfall accumulation doesn't need second-by-second freshness


def color_for_accumulation(mm):
    for lo, hi, color in ACCUMULATION_BREAKS:
        if mm >= lo and (hi is None or mm < hi):
            return color
    return ACCUMULATION_BREAKS[-1][2]


def _fetch_open_meteo(hours):
    """One batched request for all province points at once (Open-Meteo
    supports comma-separated lat/lon lists in a single call), asking for
    enough past_days to cover the requested window plus a one-day margin
    for timezone/rounding safety."""
    lats = ",".join(str(p["lat"]) for p in PROVINCE_POINTS)
    lons = ",".join(str(p["lon"]) for p in PROVINCE_POINTS)
    past_days = min(92, max(2, (hours // 24) + 1))  # Open-Meteo's forecast endpoint caps past_days at 92
    params = {
        "latitude": lats, "longitude": lons, "hourly": "precipitation",
        "past_days": past_days, "forecast_days": 1, "timezone": "Asia/Bangkok",
    }
    url = OPEN_METEO_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "bbtec-noc-dashboard/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read()
    data = json.loads(body)
    # Single-location requests return one object; multi-location requests
    # return a list of objects in the same order as the input coordinates.
    if isinstance(data, dict):
        data = [data]
    return data


def build_rainfall_accumulation(hours, use_cache=True):
    """Returns a list of {code, name, lat, lon, accumulated_mm, color} -
    one entry per province, summing the trailing `hours` hours of hourly
    precipitation from Open-Meteo. Cached per distinct `hours` value."""
    now = time.monotonic()
    if use_cache:
        with _accumulation_lock:
            cached = _accumulation_cache.get(hours)
            if cached is not None and (now - cached["ts"]) < ACCUMULATION_CACHE_TTL_SECONDS:
                return cached["data"]

    raw = _fetch_open_meteo(hours)
    results = []
    for point, entry in zip(PROVINCE_POINTS, raw):
        hourly = entry.get("hourly", {})
        precip = hourly.get("precipitation", [])
        # Take the LAST `hours` values (most recent), sum them - Open-Meteo
        # returns past_days of history followed by the current forecast
        # day, so the tail of the array is "now" and walking backward
        # `hours` steps covers exactly the requested trailing window.
        trailing = precip[-hours:] if len(precip) >= hours else precip
        total_mm = round(sum(v for v in trailing if v is not None), 1)
        results.append({
            "code": point["code"], "name": point["name"],
            "lat": point["lat"], "lon": point["lon"],
            "accumulated_mm": total_mm, "color": color_for_accumulation(total_mm),
        })

    if use_cache:
        with _accumulation_lock:
            _accumulation_cache[hours] = {"data": results, "ts": now}
    return results
