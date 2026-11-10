"""Presentation settings (which slides the "Present" button shows) - one JSON document in the Google Sheet tab
PresentationConfig (A1 = header, A2 = JSON, B2 = updated_at, C2 = updated_by). Any signed-in user may save."""
import json
import threading
import time

from pending_ticket import _with_quota_retry, _is_quota_error
from realtime_monitor import REALTIME_SHEET_ID

SHEET = "PresentationConfig"
PRESENTATION_EDITORS = ["saridphong_n@bbtec.co.th"]
SLIDE_TYPES = {"plan", "strategy", "fctotal", "fcbm", "agfc", "cover", "aging", "oversla", "bkdrill", "bkdetail", "nsasum", "nsaplan", "title", "kpi", "trend", "provinces", "groups", "late", "text", "closing", "images"}
ACCENTS = {"cyan", "violet", "emerald", "amber", "rose"}
SCOPES = {"all", "NSA3", "NSA4"}
_lock = threading.Lock()
_cache = {"v": None, "ts": 0.0}


def is_editor(email):
    """Everyone who is signed in (has an email in the session) may edit and save the slides."""
    return bool(str(email or "").strip())


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
        if s["type"] == "plan":
            slides[-1]["ex"] = str(s.get("ex") or "")[:4000]
            slides[-1]["lock"] = str(s.get("lock") or "")[:4000]
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
            "theme": cfg.get("theme") if cfg.get("theme") in ("light", "dark") else "light",
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


# ---------------------------------------------------------------------------------------------------------------
# Manual image slide: up to IMG_SLOTS pictures kept in the Sheet tab PresentationImages.
# One row per slot (row 2..): A=slot, B=updated_at, C=updated_by, D=chunk count, E..=base64 data-URL split in
# <=45 000-char pieces (a Sheet cell holds max 50 000 chars). The browser shrinks every picture to <=1600 px first.
# ---------------------------------------------------------------------------------------------------------------
IMG_SHEET = "PresentationImages"
IMG_SLOTS = 2
IMG_MAX_CHARS = 1100000
IMG_CHUNK = 45000
_img_cache = {"v": None, "ts": 0.0}


def _img_ws(sh, create):
    try:
        return _with_quota_retry(lambda: sh.worksheet(IMG_SHEET))
    except Exception as e:
        if _is_quota_error(e):
            raise
        if not create:
            return None
        return _with_quota_retry(lambda: sh.add_worksheet(title=IMG_SHEET, rows=10, cols=4 + IMG_MAX_CHARS // IMG_CHUNK + 2))


def images_load(gs_client, use_cache=True):
    """-> list of {slot, src, updated_at, updated_by} for slots that hold a picture."""
    now = time.monotonic()
    with _lock:
        if use_cache and _img_cache["v"] is not None and now - _img_cache["ts"] < 120:
            return _img_cache["v"]
        stale = _img_cache["v"]
    try:
        sh = gs_client.open_by_key(REALTIME_SHEET_ID)
        ws = _img_ws(sh, False)
        vals = _with_quota_retry(lambda: ws.get_all_values()) if ws is not None else []
    except Exception:
        if stale is not None:
            return stale
        raise
    out = []
    for row in vals[1:]:
        row = row + [""] * 5
        try:
            slot = int(row[0])
        except ValueError:
            continue
        src = "".join(row[4:])
        if 1 <= slot <= IMG_SLOTS and src.startswith("data:image/"):
            out.append({"slot": slot, "src": src, "updated_at": row[1], "updated_by": row[2]})
    out.sort(key=lambda x: x["slot"])
    with _lock:
        _img_cache.update(v=out, ts=now)
    return out


def image_save(gs_client, slot, data_url, who, when):
    """data_url=None/'' deletes the picture of that slot."""
    slot = int(slot)
    if not 1 <= slot <= IMG_SLOTS:
        raise ValueError("slot ต้องเป็น 1-%d" % IMG_SLOTS)
    data_url = str(data_url or "")
    if data_url:
        if not data_url.startswith(("data:image/jpeg;base64,", "data:image/png;base64,", "data:image/webp;base64,")):
            raise ValueError("รองรับเฉพาะรูป JPEG / PNG / WebP")
        if len(data_url) > IMG_MAX_CHARS:
            raise ValueError("รูปใหญ่เกินไป (จำกัด ~800 KB หลังย่อ)")
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    ws = _img_ws(sh, True)
    chunks = [data_url[i:i + IMG_CHUNK] for i in range(0, len(data_url), IMG_CHUNK)]
    width = 4 + max(len(chunks), 1)
    row = [str(slot), when if data_url else "", who if data_url else "", str(len(chunks))] + chunks
    row += [""] * (4 + IMG_MAX_CHARS // IMG_CHUNK + 1 - len(row))  # blank old tail so a shorter picture leaves nothing behind
    _with_quota_retry(lambda: ws.update("A1:D1", [["slot", "updated_at", "updated_by", "chunks"]], value_input_option="RAW"))
    _with_quota_retry(lambda: ws.update("A%d" % (slot + 1), [row], value_input_option="RAW"))
    with _lock:
        _img_cache.update(v=None, ts=0.0)
    return images_load(gs_client, use_cache=False)
