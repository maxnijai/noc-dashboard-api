"""Material stock lookup (Material, Material Desc, CMI/CRI/NAN/PLK/TAK, Total).

Stored in the Google Sheet tab "MaterialStock" (row 1 = header, metadata in J1:K2) so every viewer and every
server restart sees the same list. When the tab is empty the bundled seed file data/material_stock_seed.json
is served, so the feature works before the first import.
Row format used everywhere: [material, desc, CMI, CRI, NAN, PLK, TAK, total]
"""
import io
import json
import logging
import os
import threading
import time

from pending_ticket import _with_quota_retry, _is_quota_error
from realtime_monitor import REALTIME_SHEET_ID

log = logging.getLogger(__name__)

SHEET = "MaterialStock"
PROVS = ["CMI", "CRI", "NAN", "PLK", "TAK"]
HEADER = ["Material", "Material Desc"] + PROVS + ["Total"]
SEED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "material_stock_seed.json")
CACHE_TTL = 300

_lock = threading.Lock()
_cache = {"data": None, "ts": 0.0}


def _int(v):
    try:
        return int(round(float(str(v).replace(",", "").strip()))) if str(v).strip() != "" else 0
    except (ValueError, TypeError):
        return 0


def _seed():
    try:
        with open(SEED_PATH, encoding="utf-8") as f:
            return {"rows": json.load(f), "updated_at": "ไฟล์ตั้งต้น (2026-11-03)", "updated_by": "", "source": "seed"}
    except Exception:
        return {"rows": [], "updated_at": "", "updated_by": "", "source": "empty"}


def _ws(gs_client, create=False):
    sh = gs_client.open_by_key(REALTIME_SHEET_ID)
    try:
        return _with_quota_retry(lambda: sh.worksheet(SHEET))
    except Exception as e:
        if _is_quota_error(e) or not create:
            raise
        return _with_quota_retry(lambda: sh.add_worksheet(title=SHEET, rows=2000, cols=12))


def load(gs_client, use_cache=True):
    now = time.monotonic()
    with _lock:
        if use_cache and _cache["data"] is not None and now - _cache["ts"] < CACHE_TTL:
            return _cache["data"]
        stale = _cache["data"]
    try:
        ws = _ws(gs_client)
        vals = _with_quota_retry(lambda: ws.get_all_values())
    except Exception as e:
        if stale is not None:
            return stale
        if _is_quota_error(e):
            raise
        vals = []          # tab missing -> seed
    rows = []
    for r in vals[1:]:
        if not r or not str(r[0]).strip():
            continue
        p = list(r[:8]) + [""] * (8 - len(r[:8]))
        v = [_int(x) for x in p[2:7]]
        rows.append([str(p[0]).strip(), str(p[1]).strip()] + v + [sum(v)])
    if rows:
        meta = {"updated_at": (vals[0][10] if len(vals[0]) > 10 else ""), "updated_by": (vals[1][10] if len(vals) > 1 and len(vals[1]) > 10 else "")}
        data = {"rows": rows, "updated_at": meta["updated_at"], "updated_by": meta["updated_by"], "source": "sheet"}
    else:
        data = _seed()
    with _lock:
        _cache.update(data=data, ts=now)
    return data


def parse_xlsx(raw):
    """Reads the uploaded workbook (first sheet). Returns (rows, error)."""
    import openpyxl
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        it = wb.worksheets[0].iter_rows(values_only=True)
        head = [str(x or "").strip() for x in next(it, [])]
    except Exception as e:
        return None, "อ่านไฟล์ Excel ไม่ได้: %s" % e
    need = ["Material", "Material Desc"] + PROVS
    miss = [n for n in need if n not in head]
    if miss:
        return None, "ไฟล์ไม่ถูกต้อง: ไม่พบคอลัมน์ %s (ต้องมี %s)" % (", ".join(miss), ", ".join(need))
    ix = [head.index(n) for n in need]
    rows = []
    for r in it:
        if not r or r[ix[0]] in (None, ""):
            continue
        g = lambda i: r[i] if i < len(r) else None
        v = [_int(g(i)) for i in ix[2:]]
        rows.append([str(g(ix[0])).strip(), str(g(ix[1]) or "").strip()] + v + [sum(v)])
    if not rows:
        return None, "ไม่พบข้อมูลในไฟล์"
    return rows, ""


def diff(old_rows, new_rows):
    old = {r[0]: r for r in old_rows}
    seen, add, chg, same = set(), 0, 0, 0
    for r in new_rows:
        seen.add(r[0])
        o = old.get(r[0])
        if o is None:
            add += 1
        elif o[2:7] != r[2:7]:
            chg += 1
        else:
            same += 1
    return {"added": add, "changed": chg, "same": same, "removed": sum(1 for k in old if k not in seen)}


def save(gs_client, rows, who, when):
    ws = _ws(gs_client, create=True)
    body = [HEADER] + [[r[0], r[1]] + r[2:8] for r in rows]
    need_rows = len(body) + 5
    if ws.row_count < need_rows:
        _with_quota_retry(lambda: ws.resize(rows=need_rows, cols=12))
    _with_quota_retry(lambda: ws.clear())
    _with_quota_retry(lambda: ws.update("A1", body, value_input_option="RAW"))
    _with_quota_retry(lambda: ws.update("J1:K2", [["updated_at", when], ["updated_by", who]], value_input_option="RAW"))
    data = {"rows": rows, "updated_at": when, "updated_by": who, "source": "sheet"}
    with _lock:
        _cache.update(data=data, ts=time.monotonic())
    return data
