"""Loy Krathong 2026 site-support monitor (tab "Loy Krathong 2026" under Regional Monitoring).

Site list = Site_Support_Loy_Krathong_2026_UPC1.xlsx (sheet "Site Serve_UPC1", R01-North Upper), embedded below:
(Event Name, Event Start Date, Event End Date, Site Serving, Site_Latitude, Site_Longitude). To change the list,
edit SITES and redeploy.

A pending ticket belongs to a site when the site ID appears as a token in its CINAME or SUBJECT (same idea as
site_capacity.match_site_ids). Only SEVERITY SA1-SA4 tickets count (explicit request - NSA is ignored).
Field-team status = Mateline Status (OWS GGS), today-only rule, same as the Focus P0 page.
"""
import logging
import re

from pending_ticket import fetch_live_rows, load_work_log, _dd_daily_extras, _safe_str, _exclusive_bookmark_label
from pending_trend import bangkok_now
from mateline_status import build_mateline_status_lookup
from realtime_monitor import _parse_dt, _classify_priority

log = logging.getLogger(__name__)

SEVERITIES = {"SA1", "SA2", "SA3", "SA4"}
SEASON = ("2026-11-15", "2026-11-26")
MAIN_NIGHT = "2026-11-24"

SITES = [
    ('Loy Krathong Chiang Rai', '2026-11-15', '2026-11-26', 'CRI0021', 19.92811, 99.81526),
    ('Loy Krathong Chiang Rai', '2026-11-15', '2026-11-26', 'UNOEV03', 19.921911, 99.820471),
    ('Loy Krathong Chiang Rai', '2026-11-15', '2026-11-26', 'UNOEV16', 19.921911, 99.820471),
    ('Loy Krathong PYO', '2026-11-15', '2026-11-26', 'UNOEV34', 19.169119, 99.896277),
    ('Loy Krathong PYO', '2026-11-15', '2026-11-26', 'UNOEV26', 19.170488, 99.897616),
    ('Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO0001', 19.170488, 99.897616),
    ('Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO0092', 19.165909, 99.900784),
    ('Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO7251', 19.163302, 99.89984),
    ('Loy Krathong PYO', '2026-11-15', '2026-11-26', 'PYO8512', 19.167991, 99.898531),
    ('2026_Loy Krathong Chiang Mai Authentic Sky Lanterns (Dutch Farm)', '2026-11-24', '2026-11-25', 'CMI3017', 18.7414, 99.1963),
    ('2026_Loy Krathong Chiang Mai Authentic Sky Lanterns (Dutch Farm)', '2026-11-24', '2026-11-25', 'CMI6993', 18.72627, 99.19482),
    ('2026_Loy Krathong Chiang Mai Lanna Lantern Festival (Wat Phra That Doi Saket)', '2026-11-24', '2026-11-25', 'CMI0009', 18.87035, 99.12854),
    ('2026_Loy Krathong Chiang Mai Lanna Lantern Festival (Wat Phra That Doi Saket)', '2026-11-24', '2026-11-25', 'CMI8742', 18.87445, 99.14955),
    ('2026_Loy Krathong Chiang Mai Yee Peng Lanna International (Thudongkasathan)', '2026-11-24', '2026-11-25', 'CMI1953', 18.91004, 99.01862),
    ('2026_Loy Krathong Chiangmai CAD Khomloy Sky Lanterns Festival', '2026-11-24', '2026-11-25', 'UNOCW03', 18.765237, 99.244852),
    ('2026_Loy Krathong Chiangmai CAD Khomloy Sky Lanterns Festival', '2026-11-24', '2026-11-25', 'UNOCW04', 18.765163, 99.242134),
    ('2026_Loy Krathong Chiangmai CAD Khomloy Sky Lanterns Festival', '2026-11-24', '2026-11-25', 'UNOEV18', 18.767917, 99.244769),
    ('2026_Loy Krathong Chiang Mai Voyage of Light', '2026-11-24', '2026-11-25', 'CMI3010', 19.01778, 99.30689),
    ('2026_Loy Krathong Chiang Mai Voyage of Light', '2026-11-24', '2026-11-25', 'CMI6859', 19.03809, 99.33148),
    ('2026_Loy Krathong Chiang Mai Golden Light CM Lantern Festival', '2026-11-24', '2026-11-25', 'CMI0156', 18.61638, 98.77696),
    ('2026_Loy Krathong Chiang Mai Loy Krathong (Miracle Land)', '2026-11-24', '2026-11-25', 'CMIC061', 19.133, 98.95032),
    ('2026_Loy Krathong Chiang Mai Loy Krathong (Miracle Land)', '2026-11-24', '2026-11-25', 'CMI7837', 19.12435, 98.93947),
    ('2026_Loy Krathong Chiang Mai Sky of Wishes (MUNGMANA FARM)', '2026-11-24', '2026-11-25', 'CMIA069', 18.69515, 98.81134),
    ('2026_Loy Krathong Chiang Mai Sky of Wishes (MUNGMANA FARM)', '2026-11-24', '2026-11-25', 'CMIA069', 18.6677, 98.817),
    ('2026_Loy Krathong Lamphun Lanterns Festival (Wat PhraThatHariphunchai)', '2026-11-24', '2026-11-25', 'LPN8501', 18.57603, 99.00911),
    ('2026_CMI_Loy Krathong-Wat Pa Ngio วัดป่างิ้ว - สะพานป่าแดด-ท่าวังตาล', '2026-11-23', '2026-11-25', 'CMI1645', 18.71474, 98.98782),
    ('2026_CMI_Loy Krathong-Wat Pa Ngio วัดป่างิ้ว - สะพานป่าแดด-ท่าวังตาล', '2026-11-23', '2026-11-25', 'CMI8541', 18.728438, 98.992343),
    ('2026_CMI_Loy Krathong-Wat Pa Ngio วัดป่างิ้ว - สะพานป่าแดด-ท่าวังตาล', '2026-11-23', '2026-11-25', 'UNOCW02', 18.7193, 98.987708),
    ('2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMI0384', 18.786971, 98.993668),
    ('2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMI6898', 18.78815, 98.99302),
    ('2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMI6924', 18.78797, 98.993805),
    ('2026_CMI_Loy Krathong_YI PENG-Tha Phae Gate ประตูท่าแพ', '2026-11-24', '2026-11-25', 'CMIA361', 18.787827, 98.995011),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG0590', 18.2907, 99.4885),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG7611', 18.294987, 99.494198),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG6847', 18.288848, 99.485928),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG6870', 18.289016, 99.491142),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG7006', 18.28919, 99.49567),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'LPG7647', 18.291154, 99.491862),
    ('2026_LPG_Loy Krathong_Ban Sing Chai Pier ท่าน้ำบ้านสิงห์ชัย', '2026-11-24', '2026-11-25', 'UNOEV22', 18.289016, 99.491142),
    ('2026_Loy Krathong Chiang Mai Mae Kha Canal คลองแม่ข่า', '2026-11-24', '2026-11-25', 'CMIC006', 18.77614, 98.99774),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI0627', 18.785875, 99.002372),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI6717', 18.78427, 99.00743),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI6896', 18.78891, 99.00183),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI7171', 18.786993, 99.009533),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI0046', 18.7873, 99.00249),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI6717', 18.785193, 99.005129),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI7227', 18.78239, 99.005768),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMI8815', 18.787892, 99.005292),
    ('2026_Loy Krathong CMI-YI PENG-Nawarat Bridge สะพานนวรัฐ - Khua Lek (Iron Bridge) ขัวเหล็ก', '2026-11-24', '2026-11-25', 'CMIC011', 18.7823, 99.0019),
    ('2026_Loy Krathong MHS-สะพานท่าปาย ปางหมู', '2026-11-24', '2026-11-25', 'MHS0093', 19.36321, 97.96642),
    ('2026_Loy Krathong MHS-Mae Kong Bridge สะพาน แม่สะเรียง - แม่คง', '2026-11-24', '2026-11-25', 'MHS0013', 18.160965, 97.921936),
    ('2026_Loy Krathong MHS-Mae Kong Bridge สะพาน แม่สะเรียง - แม่คง', '2026-11-24', '2026-11-25', 'MHS1606', 18.15945, 97.93638),
    ('2026_Loy Krathong MHS-Mae Kong Bridge สะพาน แม่สะเรียง - แม่คง', '2026-11-24', '2026-11-25', 'MHS7673', 18.158578, 97.928297),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI0627', 18.785875, 99.002372),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI6717', 18.78427, 99.00743),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI6896', 18.78891, 99.00183),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI7171', 18.786993, 99.009533),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI7227', 18.78239, 99.005768),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMIC011', 18.7823, 99.0019),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI0010', 18.794293, 99.000774),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI1934', 18.793038, 98.999106),
    ('2026_Loy Krathong CMI-YI PENG-Nakhon Phing Bridge สะพานนครพิงค์', '2026-11-24', '2026-11-25', 'CMI8912', 18.796699, 98.998286),
]

_TOKEN_RE = re.compile(r"[A-Z0-9]+")


def _site_ids(text, known):
    out = []
    for tok in _TOKEN_RE.findall(str(text or "").upper()):
        if tok in known and tok not in out:
            out.append(tok)
    return out


def build_response(gs_client):
    now_dt = bangkok_now()
    today = now_dt.strftime("%Y-%m-%d")
    known = {s[3].upper() for s in SITES}
    rows = fetch_live_rows(gs_client)
    try:
        work_log = load_work_log(gs_client)
    except Exception:
        log.exception("loy: work log failed")
        work_log = {}
    try:
        mateline = build_mateline_status_lookup(gs_client, today)
    except Exception:
        log.exception("loy: mateline lookup failed")
        mateline = {}
    try:
        extras = _dd_daily_extras(gs_client, today)
    except Exception:
        log.exception("loy: GGS team lookup failed")
        extras = {}
    tickets = []
    for r in rows:
        sev = str(r.get("SEVERITY", "")).strip().upper()
        if sev not in SEVERITIES:
            continue
        sites = _site_ids(r.get("CINAME"), known)
        for x in _site_ids(r.get("SUBJECT"), known):
            if x not in sites:
                sites.append(x)
        if not sites:
            continue
        tid = str(r.get("TICKETID", "")).strip()
        m = mateline.get(tid.upper()) or {}
        wl = work_log.get(tid, {})
        target = _parse_dt(r.get("TARGETFINISH"))
        tickets.append({
            "id": tid, "sites": sites, "sev": sev,
            "pr": _classify_priority(r.get("TARGETFINISH"), now_dt),
            "bm": _exclusive_bookmark_label(r.get("Bookmark")),
            "ciname": _safe_str(r.get("CINAME")), "subject": _safe_str(r.get("SUBJECT")),
            "created": _safe_str(r.get("CREATIONDATE")), "target": _safe_str(r.get("TARGETFINISH")),
            "left": round((target - now_dt).total_seconds() / 3600, 1) if target else None,
            "ows": m.get("status_mateline") or "(ไม่พบใน MatelineX)", "at": m.get("mateline_at", ""),
            "team": (extras.get(tid.upper()) or {}).get("team") or _safe_str(r.get("Tech_Team")),
            "detail": wl.get("detail", ""), "group_problem": wl.get("group_problem", ""), "plan": wl.get("plan_closed_date", ""),
        })
    return {
        "generated_at": now_dt.strftime("%Y-%m-%d %H:%M"), "today": today,
        "season": SEASON, "main_night": MAIN_NIGHT, "severities": sorted(SEVERITIES),
        "sites": [{"event": s[0], "start": s[1], "end": s[2], "site": s[3], "lat": s[4], "lon": s[5]} for s in SITES],
        "tickets": tickets,
    }
