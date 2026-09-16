"""Historical Closed Ticket - lookup ticket history by CI Name (explicit
request). Manually imported Excel (single sheet, first sheet used
regardless of its name - confirmed via the sample file that the real
export just has one sheet), kept in memory (module-level _store) like
sla_improvement.py's import flow - not a live Google Sheet.

Search logic (explicit request, confirmed choice): exact match against
CI_Name first; if that finds nothing, fall back to a substring match
against SUBJECT (case-insensitive, matches anywhere in the text) - since
SUBJECT often embeds the CI_Name as a prefix (e.g. CI_Name "CMI8506" inside
SUBJECT "CMI8506L_6NB03 (A71+ORIGIN): ERIC Link Failure") but isn't always
identical to it, so a ticket can be relevant even when CI_Name itself is
blank or doesn't match exactly. Never search rows that have no CI_Name AND
no SUBJECT to check - that never happens here since SUBJECT is required
and never blank in the real file, but CI_Name can be.
"""

import io
import logging
from datetime import datetime

import openpyxl

log = logging.getLogger(__name__)

_store = {"rows": None, "warnings": None, "imported_at": None, "filename": None}

REQUIRED_COLUMNS = [
    "TICKETID", "SUBJECT", "CREATIONDATE", "CLOSEDTIME", "TRUESEVERITY_DESC",
    "NODENAME", "CLASSIFICATION", "TRUEOWNERGROUP", "PROBLEM", "SUB_CAUSE",
    "REMEDY", "DISTRICT_EN", "CI_Name",
]


class ImportValidationError(Exception):
    pass


def _parse_dt(v):
    """Cell values come back as datetime objects already when the sheet
    stores them as real dates (openpyxl with data_only=True) - falls back
    to string parsing only if the cell was stored as text."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_excel(file_bytes):
    """Reads the FIRST sheet regardless of its name (confirmed via the
    real sample file - it's a single-sheet export, name not guaranteed to
    stay "Sheet1" on future exports). Raises ImportValidationError with a
    clear message on a missing required column - never silently proceeds
    with misaligned columns."""
    try:
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
    except Exception as e:
        raise ImportValidationError(f"เปิดไฟล์ Excel ไม่สำเร็จ: {e}")

    ws = wb[wb.sheetnames[0]]
    rows_iter = ws.iter_rows(values_only=True)
    try:
        header_raw = next(rows_iter)
    except StopIteration:
        raise ImportValidationError("ไฟล์ไม่มีข้อมูล")
    header = [str(h).strip() if h is not None else "" for h in header_raw]
    header_idx = {h: i for i, h in enumerate(header)}
    missing = [c for c in REQUIRED_COLUMNS if c not in header_idx]
    if missing:
        raise ImportValidationError(f"ไฟล์ขาด Column ที่จำเป็น: {', '.join(missing)}")

    def get(raw, col):
        idx = header_idx.get(col)
        if idx is None or idx >= len(raw):
            return None
        return raw[idx]

    def get_str(raw, col):
        v = get(raw, col)
        return "" if v is None else str(v).strip()

    rows = []
    blank_ci_name_count = 0
    for raw in rows_iter:
        if raw is None or not any(c is not None and str(c).strip() for c in raw):
            continue  # fully blank row

        ci_name = get_str(raw, "CI_Name")
        if not ci_name:
            blank_ci_name_count += 1

        rows.append({
            "TICKETID": get_str(raw, "TICKETID"),
            "SUBJECT": get_str(raw, "SUBJECT"),
            "CREATIONDATE": _parse_dt(get(raw, "CREATIONDATE")),
            "CLOSEDTIME": _parse_dt(get(raw, "CLOSEDTIME")),
            "TRUESEVERITY_DESC": get_str(raw, "TRUESEVERITY_DESC"),
            "NODENAME": get_str(raw, "NODENAME"),
            "CLASSIFICATION": get_str(raw, "CLASSIFICATION"),
            "TRUEOWNERGROUP": get_str(raw, "TRUEOWNERGROUP"),
            "PROBLEM": get_str(raw, "PROBLEM"),
            "SUB_CAUSE": get_str(raw, "SUB_CAUSE"),
            "REMEDY": get_str(raw, "REMEDY"),
            "DISTRICT_EN": get_str(raw, "DISTRICT_EN"),
            "CI_Name": ci_name,
        })

    warnings = []
    if blank_ci_name_count:
        warnings.append(f"{blank_ci_name_count} แถวไม่มีค่า CI_Name (ยังค้นหาเจอได้ผ่าน SUBJECT)")

    return rows, warnings


def import_excel(file_bytes, filename=None):
    rows, warnings = parse_excel(file_bytes)
    _store["rows"] = rows
    _store["warnings"] = warnings
    _store["imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _store["filename"] = filename
    return {"row_count": len(rows), "warnings": warnings}


def get_rows():
    return _store["rows"]


def distinct_severities(rows):
    return sorted({r["TRUESEVERITY_DESC"] for r in rows if r["TRUESEVERITY_DESC"]})


def _fmt_dt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


def search(ci_name_query, severities=None):
    """Returns (results, match_mode) where match_mode is "exact" (found
    via CI_Name), "subject" (fell back to SUBJECT substring), or None (no
    query typed / no data yet). Results are ALWAYS sorted by CREATIONDATE
    oldest -> newest (explicit request), regardless of which match mode
    found them."""
    rows = _store["rows"]
    if rows is None:
        return None, None, "ยังไม่มีข้อมูล กรุณา Import ไฟล์ก่อน"

    query = (ci_name_query or "").strip()
    if not query:
        return [], None, None

    query_lower = query.lower()
    exact = [r for r in rows if r["CI_Name"].lower() == query_lower]
    match_mode = "exact"
    if not exact:
        exact = [r for r in rows if query_lower in r["SUBJECT"].lower()]
        match_mode = "subject"

    if severities:
        wanted = set(severities)
        exact = [r for r in exact if r["TRUESEVERITY_DESC"] in wanted]

    exact.sort(key=lambda r: r["CREATIONDATE"] or datetime.min)

    results = [{
        "CREATIONDATE": _fmt_dt(r["CREATIONDATE"]),
        "CLOSEDTIME": _fmt_dt(r["CLOSEDTIME"]),
        "TRUESEVERITY_DESC": r["TRUESEVERITY_DESC"],
        "TICKETID": r["TICKETID"],
        "SUBJECT": r["SUBJECT"],
        "NODENAME": r["NODENAME"],
        "CLASSIFICATION": r["CLASSIFICATION"],
        "TRUEOWNERGROUP": r["TRUEOWNERGROUP"],
        "PROBLEM": r["PROBLEM"],
        "SUB_CAUSE": r["SUB_CAUSE"],
        "REMEDY": r["REMEDY"],
        "DISTRICT_EN": r["DISTRICT_EN"],
        "CI_Name": r["CI_Name"],
    } for r in exact]

    return results, (match_mode if results else None), None
