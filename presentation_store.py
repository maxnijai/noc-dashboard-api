"""Presentation settings (which slides the "Present" button shows) - one JSON document in the Google Sheet tab
PresentationConfig (A1 = header, A2 = JSON, B2 = updated_at, C2 = updated_by). Only PRESENTATION_EDITORS may save."""
import json
import threading
import time

from pending_ticket import _with_quota_retry, _is_quota_error
from realtime_monitor import REALTIME_SHEET_ID

SHEET = "PresentationConfig"
PRESENTATION_EDITORS = ["saridphong_n@bbtec.co.th"]
SLIDE_TYPES = {"strategy", "fctotal", "fcbm", "agfc", "cover", "aging", "oversla", "bkdrill", "bkdetail", "nsasum", "nsaplan", "title", "kpi", "trend", "provinces", "groups", "late", "text", "closing"}
ACCENTS = {"cyan", "violet", "emerald", "amber", "rose"}
SCOPES = {"all", "NSA3", "NSA4"}
_lock = threading.Lock()
_cache = {"v": None, "ts": 0.0}


def is_editor(email):
    return str(email or "").strip().lower() in PRESENTATION_EDITORS


def sanitize(cfg):
    """Keeps only known fields/values so a bad payload can never break the viewer."""
    if not isinstance(cfg, dict):
        raise ValueError("config ต้องเป็น object")
    slides = []
    for i, s in enumerate((cfg.get("slides") or [])[:30]):
        if not isinstance(s, dict) or s.get("type") not in SLIDE_TYPES:
            continue
        slides.append({"id": str(s.get("id") or "s%d" % i)[:24], "type": s["type"], "enabled": bool(s.get("enabled", True)),
                       "title": str(s.get("title") or "")[:120], "body": str(s.get("body") or "")[:1500]})
    if not slides:
        raise ValueError("ต้องมีอย่างน้อย 1 สไลด์")

    def num(v, lo, hi, d):
        try:
            return max(lo, min(hi, int(v)))
        except (TypeError, ValueError):
            return d
    return {"version": 1, "autoplay": bool(cfg.get("autoplay", True)), "seconds": num(cfg.get("seconds"), 3, 60, 9),
            "scope": cfg.get("scope") if cfg.get("scope") in SCOPES else "all", "topN": num(cfg.get("topN"), 3, 15, 8),
            "accent": cfg.get("accent") if cfg.get("accent") in ACCENTS else "cyan", "insight": bool(cfg.get("insight", True)),
            "slides": slides}


def load(gs_client, use_cache=True):
    now = time.monotonic()
    with _lock:
        if use_cache and _cache["v"] is not None and now - _cache["ts"] < 120:
            return _cache["v"]
        stale = _cache["v"]
    try:
        sh = gs_client.open_by_key(REALTIME_SHEET_ID)
        ws = _with_quota_retry(lambda: sh.worksheet(SHEET))
        vals = _with_quota_retry(lambda: ws.get_all_values())
    except Exception as e:
        if stale is not None:
            return stale
        if _is_quota_error(e):
            raise
        vals = []
    out = {"config": None, "updated_at": "", "updated_by": ""}
    if len(vals) > 1 and vals[1] and vals[1][0].strip():
        try:
            out = {"config": sanitize(json.loads(vals[1][0])), "updated_at": (vals[1] + ["", ""])[1], "updated_by": (vals[1] + ["", "", ""])[2]}
        except Exception:
            pass
    with _lock:
        _cache.update(v=out, ts=now)
    return out


def save(gs_client, cfg, who, when):
    cfg = sanitize(cfg)
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    try:
        ws = _with_quota_retry(lambda: sh.worksheet(SHEET))
    except Exception as e:
        if _is_quota_error(e):
            raise
        ws = _with_quota_retry(lambda: sh.add_worksheet(title=SHEET, rows=10, cols=4))
    _with_quota_retry(lambda: ws.update("A1:C2", [["config_json", "updated_at", "updated_by"], [json.dumps(cfg, ensure_ascii=False), when, who]], value_input_option="RAW"))
    out = {"config": cfg, "updated_at": when, "updated_by": who}
    with _lock:
        _cache.update(v=out, ts=time.monotonic())
    return out
