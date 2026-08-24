"""Summary NAN: a static snapshot report (uploaded once, not live-synced)
of closed SA1-4/Mobile tickets in Nan province for the 19-22 Aug flood
event - site-level root cause + per-ticket repair detail, joined by
CI_Name (ticket data) == SITE ID (site/root-cause data).

Both source tables were extracted once from the uploaded Excel workbook
into data/summary_nan_sites.json and data/summary_nan_tickets.json - this
module just loads and joins them; there's no live Google Sheet behind it.
"""

import json
import os
import re
import threading
from datetime import datetime

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
_SITES_PATH = os.path.join(_DATA_DIR, "summary_nan_sites.json")
_TICKETS_PATH = os.path.join(_DATA_DIR, "summary_nan_tickets.json")

ROOT_CAUSE_COLORS = {
    "Hardware Faulty - Other": "#a371f7",
    "OFC": "#E24B4A",
    "PEA": "#1f6feb",
    "PEA + OFC": "#EF9F27",
}
UNSPECIFIED_ROOT_CAUSE = "(ไม่ระบุ)"

_cache_lock = threading.Lock()
_cache = {"sites": None, "tickets": None}


def _load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _get_raw():
    with _cache_lock:
        if _cache["sites"] is None:
            _cache["sites"] = _load_json(_SITES_PATH)
        if _cache["tickets"] is None:
            _cache["tickets"] = _load_json(_TICKETS_PATH)
        return _cache["sites"], _cache["tickets"]


def _parse_dt(s):
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _last_classification_segment_like(raw):
    """SUB_CAUSE / PROBLEM strings sometimes carry the same backslash-
    delimited hierarchy CLASSIFICATION does elsewhere in this app - not
    required here, kept unused unless a field needs it later."""
    raw = str(raw or "").strip()
    if not raw:
        return UNSPECIFIED_ROOT_CAUSE
    parts = [p.strip() for p in re.split(r"\\+", raw) if p.strip()]
    return parts[-1] if parts else raw


def build_summary_nan_response():
    sites_raw, tickets_raw = _get_raw()

    sites = []
    for s in sites_raw:
        # Blank root_cause in the source data defaults to PEA (per request) -
        # distinct from a ticket whose site couldn't be matched at all,
        # which stays genuinely "(ไม่ระบุ)" below.
        root_cause = s.get("root_cause") or "PEA"
        sites.append({
            "district": s.get("district", ""),
            "site_id": s.get("site_id", ""),
            "root_cause": root_cause,
            "color": ROOT_CAUSE_COLORS.get(root_cause, "#8b949e"),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "ticket_count": s.get("ticket_count", 0),
        })

    site_by_id = {s["site_id"].upper(): s for s in sites}

    # Exclude tickets opened before the flood event window (19 Aug) - the
    # uploaded workbook's raw ticket list includes some older tickets that
    # aren't part of this incident.
    CUTOFF_DATE = datetime(2026, 8, 19)

    tickets = []
    for t in tickets_raw:
        creation_dt = _parse_dt(t.get("creation_date"))
        if creation_dt is not None and creation_dt < CUTOFF_DATE:
            continue

        ci = (t.get("ci_name") or "").strip().upper()
        matched_site = site_by_id.get(ci)
        down_time_min = t.get("down_time_minute")
        down_time_hour = round(down_time_min / 60, 1) if down_time_min is not None else None

        target_finish_dt = _parse_dt(t.get("target_finish"))
        restoration_dt = _parse_dt(t.get("restoration_date"))
        repair_delay_hour = None
        if target_finish_dt and restoration_dt:
            repair_delay_hour = round((restoration_dt - target_finish_dt).total_seconds() / 3600, 1)

        tickets.append({
            "ticket_id": t.get("ticket_id", ""),
            "severity": t.get("severity", ""),
            "subject": t.get("subject", ""),
            "creation_date": t.get("creation_date", ""),
            "target_finish": t.get("target_finish", ""),
            "restoration_date": t.get("restoration_date", ""),
            "closed_time": t.get("closed_time", ""),
            "problem": t.get("problem", ""),
            "sub_cause": t.get("sub_cause", ""),
            "remedy": t.get("remedy", ""),
            "down_time_hour": down_time_hour,
            "ci_name": t.get("ci_name", ""),
            "district": matched_site["district"] if matched_site else t.get("district_en", ""),
            "site_id": matched_site["site_id"] if matched_site else t.get("ci_name", ""),
            "root_cause": matched_site["root_cause"] if matched_site else UNSPECIFIED_ROOT_CAUSE,
            "repair_delay_hour": repair_delay_hour,
        })

    # District summary: sites affected + root-cause breakdown per district.
    district_map = {}
    for s in sites:
        d = s["district"] or "(ไม่ระบุ)"
        entry = district_map.setdefault(d, {"district": d, "site_count": 0, "ticket_count": 0, "root_causes": {}})
        entry["site_count"] += 1
        entry["ticket_count"] += s["ticket_count"]
        entry["root_causes"][s["root_cause"]] = entry["root_causes"].get(s["root_cause"], 0) + 1
    district_summary = sorted(district_map.values(), key=lambda r: r["site_count"], reverse=True)

    # Ranking: longest repair delay first (RESTORATIONDATE - TARGETFINISH).
    ranked = [t for t in tickets if t["repair_delay_hour"] is not None]
    ranked.sort(key=lambda t: t["repair_delay_hour"], reverse=True)

    root_cause_totals = {}
    for s in sites:
        root_cause_totals[s["root_cause"]] = root_cause_totals.get(s["root_cause"], 0) + 1

    return {
        "sites": sites,
        "tickets": tickets,
        "district_summary": district_summary,
        "ranking": ranked,
        "root_cause_totals": [
            {"root_cause": k, "total": v, "color": ROOT_CAUSE_COLORS.get(k, "#8b949e")}
            for k, v in sorted(root_cause_totals.items(), key=lambda kv: kv[1], reverse=True)
        ],
        "total_sites": len(sites),
        "total_tickets": len(tickets),
    }
