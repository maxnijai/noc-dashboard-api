"""Profile photos for Oncall Supervisor / Eng Zone people.

One photo per PERSON (keyed by the title-less, spelling-normalised name), stored in its own
worksheet `OncallPhotos` (Name | Photo | UpdatedBy | UpdatedAt) so the OncallEscalation grid is
never touched. The photo is a small JPEG data URL produced in the browser (<= ~200px)."""
import threading
import time

from oncall import _get_spreadsheet
from oncall_escalation import _norm_name, bangkok_now

PHOTO_SHEET = "OncallPhotos"
HEADER = ["Name", "Photo", "UpdatedBy", "UpdatedAt"]
MAX_PHOTO_CHARS = 45000          # Google Sheets cell limit is 50,000 characters
ALLOWED_UPLOADERS = {"saridphong_n@bbtec.co.th", "natchawan.b@rfplusthailand.com"}

_lock = threading.Lock()
_cache = {"data": None, "ts": 0}
_CACHE_TTL = 3600


def can_upload(email):
    return (email or "").strip().lower() in ALLOWED_UPLOADERS


def _ws(sh, create=False):
    try:
        return sh.worksheet(PHOTO_SHEET)
    except Exception:
        if not create:
            return None
        ws = sh.add_worksheet(title=PHOTO_SHEET, rows=200, cols=len(HEADER))
        ws.update("A1:D1", [HEADER], value_input_option="RAW")
        return ws


def load_photos(gs_client, use_cache=True):
    """{normalised name: data-URL}"""
    with _lock:
        if use_cache and _cache["data"] is not None and time.time() - _cache["ts"] < _CACHE_TTL:
            return _cache["data"]
    ws = _ws(_get_spreadsheet(gs_client))
    out = {}
    if ws is not None:
        for line in ws.get_all_values()[1:]:
            if len(line) >= 2 and line[0] and line[1]:
                out[_norm_name(line[0])] = line[1]
    with _lock:
        _cache["data"], _cache["ts"] = out, time.time()
    return out


def save_photo(gs_client, name, data_url, updated_by=None):
    key = _norm_name(name)
    if not key:
        raise ValueError("name is required")
    if data_url:
        if not str(data_url).startswith("data:image/"):
            raise ValueError("photo must be an image")
        if len(data_url) > MAX_PHOTO_CHARS:
            raise ValueError("photo is too large")
    ws = _ws(_get_spreadsheet(gs_client), create=True)
    values = ws.get_all_values()
    row = next((i for i, l in enumerate(values[1:], start=2) if l and _norm_name(l[0]) == key), None)
    now = bangkok_now().strftime("%Y-%m-%d %H:%M:%S")
    if not data_url:                       # empty photo = remove
        if row:
            ws.delete_rows(row)
    elif row:
        ws.update(f"A{row}:D{row}", [[key, data_url, updated_by or "", now]], value_input_option="RAW")
    else:
        ws.append_row([key, data_url, updated_by or "", now], value_input_option="RAW")
    with _lock:
        _cache["data"], _cache["ts"] = None, 0
    return {"name": key, "removed": not data_url, "updated_at": now}
