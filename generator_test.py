"""
generator_test.py
-----------------
"Generator Test" tab: Portable Generator reports from the BBTEC Smart App
(sheet "PortableGenTest"), summarised for NODE teams only - Weekly Generator
Start Test (by ISO week of UpdatedAt) and Daily Generator Check (by day).

The rules are the ones from the "Portable Test GEN" summary prompt, applied in
code instead of by an AI so the numbers are identical every time:
  * Scope = teams in the roster (sheet "Team": TypeTeam = NODE (or NOD) and Active = Y).
    Rows of other teams are not counted (the number dropped is reported); a
    NODE-looking team that is not in the roster is listed separately.
  * Province = the 3 letters after "-NR-" in TeamID; Region from a fixed table.
    The Region/Province typed in the report row is only checked, never used.
  * The week comes from UpdatedAt converted to Bangkok time FIRST (ISO 8601,
    Monday-Sunday), so Monday 00:00-06:59 Bangkok doesn't fall into last week.
  * A team with several rows in the period counts once, using the latest row.
  * "Submitted" is not "passed": the status buckets are shown separately.
  * The totals are cross-checked (sent + not sent = roster, provinces =
    regions = NOR) and any mismatch is reported, never adjusted.

Read-only: nothing here writes to any sheet.
"""

import logging
import os
import re
import threading
import time
from datetime import date, datetime, timedelta

from pending_trend import bangkok_now

log = logging.getLogger(__name__)

GEN_SHEET_ID = os.environ.get("GENERATOR_SHEET_ID", "1KJQvrRwhGl61hK3v7XfJj4H3RljOZnuIHj8OJ9YTwIk")
GEN_TAB_NAME = "PortableGenTest"
GEN_TAB_GID = 1325696950           # fallback if the tab is ever renamed
TEAMS_SHEET_ID = os.environ.get("GENERATOR_TEAMS_SHEET_ID", GEN_SHEET_ID)
TEAMS_TAB_NAMES = ("Teams", "Team")   # the roster tab is called "Team" in the Smart App file; "Teams" is also accepted
TEAMS_TAB_GID = 120379322

NODE_TYPES = {"NOD", "NODE"}     # the Team tab writes "NODE" (the prompt says NOD) - both mean a NODE team
WEEKLY_TYPE = "Weekly Generator Start Test"
DAILY_TYPE = "Daily Generator Check"
WEEKLY_STATUSES = ["ติดปกติ", "สตาร์ทไม่ติด", "ส่งซ่อม", "ไม่มี Gen"]
DAILY_STATUSES = ["พร้อมใช้งาน", "ชำรุด", "ส่งซ่อม", "ไม่มี Gen"]
WEEKLY_FOLLOW = {"สตาร์ทไม่ติด", "ส่งซ่อม", "ไม่มี Gen"}
DAILY_FOLLOW = {"ชำรุด", "ส่งซ่อม", "ไม่มี Gen"}

# Region table is fixed by the prompt.
REGIONS = {
    "NOR1": ["CMI", "CRI", "LPG", "LPN", "MHS", "NAN", "PHE", "PYO"],
    "NOR2": ["KPP", "PCB", "PCT", "PSN", "SKT", "TAK", "UTR"],
}
REGION_OF = {p: r for r, ps in REGIONS.items() for p in ps}

CACHE_TTL_SECONDS = 60
_cache = {"data": None, "ts": 0.0}
_cache_lock = threading.Lock()

_TEAM_PROV_RX = re.compile(r"-NR-([A-Z]{3})-")
_DT_RX = re.compile(
    r"^\s*(\d{1,4})[/\-.](\d{1,2})[/\-.](\d{1,4})"
    r"(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?\s*([AaPp][Mm])?)?"
    r"\s*(Z|[+-]\d{2}:?\d{2})?\s*$"
)
_PERIOD_RX = re.compile(r"(\d{4})\D{0,3}[Ww]?\D?(\d{1,2})")


def province_of(team_id):
    m = _TEAM_PROV_RX.search(str(team_id or ""))
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Time handling
# ---------------------------------------------------------------------------
def _build_dt(y, mo, d, hh, mi, ss, ampm, tz):
    if y > 2400:                       # Buddhist-era year typed by a Thai-locale sheet
        y -= 543
    if ampm:
        hh = hh % 12 + (12 if ampm.lower() == "pm" else 0)
    try:
        dt = datetime(y, mo, d, hh, mi, ss)
    except ValueError:
        return None
    if tz:                             # explicit zone (e.g. "...Z") -> convert to Bangkok wall time
        if tz == "Z":
            off = 0
        else:
            sign = 1 if tz[0] == "+" else -1
            digits = tz[1:].replace(":", "")
            off = sign * (int(digits[:2]) * 60 + int(digits[2:]))
        dt = dt - timedelta(minutes=off) + timedelta(hours=7)
    return dt                          # no zone -> already Bangkok wall time (as the Smart App writes it)


def parse_sheet_datetime(text, dayfirst=True):
    """Parses the formats the sheet may hold ("5/10/2026, 11:59:42", "05/10/2026 11:59:42",
    "2026-10-05 11:59:42", "2026-10-05T04:59:42Z", ...) into a naive Bangkok datetime, or None."""
    if text is None:
        return None
    m = _DT_RX.match(str(text))
    if not m:
        return None
    a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hh, mi, ss = int(m.group(4) or 0), int(m.group(5) or 0), int(m.group(6) or 0)
    ampm, tz = m.group(7), m.group(8)
    if len(m.group(1)) == 4:           # year first: y-m-d
        y, mo, d = a, b, c
    else:                              # a/b/yyyy: day-first or month-first
        y = c
        d, mo = (a, b) if dayfirst else (b, a)
    return _build_dt(y, mo, d, hh, mi, ss, ampm, tz)


def iso_week_key(dt):
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def week_monday(key):
    y, w = key.split("-W")
    return date.fromisocalendar(int(y), int(w), 1)


_TH_MONTHS = ["ม.ค.", "ก.พ.", "มี.ค.", "เม.ย.", "พ.ค.", "มิ.ย.", "ก.ค.", "ส.ค.", "ก.ย.", "ต.ค.", "พ.ย.", "ธ.ค."]


def week_label(key):
    mon = week_monday(key)
    sun = mon + timedelta(days=6)
    start = f"{mon.day}" + (f" {_TH_MONTHS[mon.month - 1]}" if mon.month != sun.month else "")
    return f"{key} ({start} – {sun.day} {_TH_MONTHS[sun.month - 1]} {sun.year})"


def previous_week_key(key):
    return iso_week_key(datetime.combine(week_monday(key) - timedelta(days=7), datetime.min.time()))


def _norm_period(text):
    m = _PERIOD_RX.search(str(text or ""))
    if not m:
        return None
    return f"{int(m.group(1))}-W{int(m.group(2)):02d}"


def detect_dayfirst(date_strings, weekly_pairs):
    """Decides whether a/b/yyyy values are day/month or month/day.
    1) any first number > 12 -> day-first; any second number > 12 -> month-first (decisive);
    2) otherwise compare against the Period column of Weekly rows: whichever reading gives the
       ISO week that matches Period more often wins;
    3) still undecided -> day-first (Thai locale) and say so.
    Returns (dayfirst, how)."""
    day_ev = month_ev = seen = 0
    for s in date_strings:
        m = re.match(r"^\s*(\d{1,2})[/\-.](\d{1,2})[/\-.]\d{4}", str(s or ""))
        if m:
            seen += 1
            a, b = int(m.group(1)), int(m.group(2))
            if a > 12 and b <= 12:
                day_ev += 1
            elif b > 12 and a <= 12:
                month_ev += 1
    if not seen:
        return True, "ปี-เดือน-วัน (ไม่กำกวม)"
    if day_ev or month_ev:
        if day_ev and month_ev:
            return day_ev >= month_ev, f"ค่าวันที่ปนกัน (วัน/เดือน {day_ev} · เดือน/วัน {month_ev}) ใช้แบบที่พบมากกว่า"
        return day_ev > 0, "วัน/เดือน/ปี (ตรวจจากวันที่ > 12)" if day_ev else "เดือน/วัน/ปี (ตรวจจากวันที่ > 12)"
    score = {True: 0, False: 0}
    checked = 0
    for ts, period in weekly_pairs:
        want = _norm_period(period)
        if not want:
            continue
        checked += 1
        for df in (True, False):
            dt = parse_sheet_datetime(ts, dayfirst=df)
            if dt and iso_week_key(dt) == want:
                score[df] += 1
    if checked and score[True] != score[False]:
        df = score[True] > score[False]
        return df, f"{'วัน/เดือน/ปี' if df else 'เดือน/วัน/ปี'} (ตรวจจากคอลัมน์ Period: ตรง {max(score.values())}/{checked} แถว)"
    return True, "วัน/เดือน/ปี (ค่าเริ่มต้น - วันที่ทุกค่า ≤ 12 และเทียบ Period ไม่ได้)"


# ---------------------------------------------------------------------------
# Reading the sheet
# ---------------------------------------------------------------------------
def _header_map(header):
    return {re.sub(r"[\s_]+", "", str(h)).lower(): i for i, h in enumerate(header) if str(h).strip()}


def _cell(row, hm, name, default=""):
    i = hm.get(name.lower())
    return str(row[i]).strip() if i is not None and i < len(row) and row[i] is not None else default


def _norm_title(t):
    return re.sub(r"[\s_]+", "", str(t)).lower()


def _find_ws(sh, names, gid=None):
    """Tries each name exactly, then ignoring case/spaces/underscores, then the gid. If nothing matches,
    raises LookupError listing the tab names that DO exist, so the page can say what to fix."""
    names = (names,) if isinstance(names, str) else tuple(names)
    for name in names:
        try:
            return sh.worksheet(name)
        except Exception:
            pass
    sheets = sh.worksheets()
    for name in names:
        for ws in sheets:
            if _norm_title(ws.title) == _norm_title(name):
                return ws
    if gid is not None:
        for ws in sheets:
            if ws.id == gid:
                return ws
    raise LookupError(f"ไม่พบแท็บ {' / '.join(chr(34) + n + chr(34) for n in names)} ในไฟล์นี้ (แท็บที่มี: {', '.join(ws.title for ws in sheets) or '-'})")


def _read_sheets(gs_client):
    gen_sh = gs_client.open_by_key(GEN_SHEET_ID)
    gen_ws = _find_ws(gen_sh, GEN_TAB_NAME, GEN_TAB_GID)
    gen_values = gen_ws.get_all_values()
    teams_values, teams_error, teams_title = [], None, None
    try:
        teams_sh = gen_sh if TEAMS_SHEET_ID == GEN_SHEET_ID else gs_client.open_by_key(TEAMS_SHEET_ID)
        teams_ws = _find_ws(teams_sh, TEAMS_TAB_NAMES, TEAMS_TAB_GID)
        teams_title, teams_values = teams_ws.title, teams_ws.get_all_values()
    except Exception as e:  # missing tab / no access -> roster missing (reported, never guessed)
        teams_error = f"{type(e).__name__}: {e}"
        log.warning("Generator Test: could not read the roster tab: %s", teams_error)
    return gen_values, teams_values, teams_error, teams_title


def _pick_col(hm, *aliases):
    for a in aliases:
        if a in hm:
            return hm[a]
    return None


def build_dataset(gen_values, teams_values, teams_error=None, teams_title=None):
    """Raw sheet values -> {roster, rows, diagnostics}. Pure, so it can be tested without Google."""
    roster, roster_unknown_province = [], []
    roster_stats = {"rows": 0, "type_values": {}, "active_values": {}}
    if teams_values:
        hm = _header_map(teams_values[0])
        c_id, c_type, c_act = _pick_col(hm, "teamid", "teamcode"), _pick_col(hm, "typeteam", "teamtype", "type"), _pick_col(hm, "active", "isactive")
        missing = [n for n, c in (("TeamID", c_id), ("TypeTeam", c_type), ("Active", c_act)) if c is None]
        if missing:
            shown = ", ".join(str(h) for h in teams_values[0] if str(h).strip()) or "-"
            teams_error = f"แท็บ \"{teams_title or 'Team'}\" ไม่มีคอลัมน์ {', '.join(missing)} (หัวคอลัมน์ที่พบ: {shown})"
            log.warning("Generator Test: %s", teams_error)
        at = lambda r, i: str(r[i]).strip() if i is not None and i < len(r) and r[i] is not None else ""
        for r in teams_values[1:] if not missing else []:
            team = at(r, c_id)
            if not team:
                continue
            roster_stats["rows"] += 1
            tv, av = at(r, c_type).upper() or "(ว่าง)", at(r, c_act).upper() or "(ว่าง)"
            roster_stats["type_values"][tv] = roster_stats["type_values"].get(tv, 0) + 1
            roster_stats["active_values"][av] = roster_stats["active_values"].get(av, 0) + 1
            if at(r, c_type).upper() not in NODE_TYPES or at(r, c_act).upper() != "Y":
                continue
            if province_of(team) not in REGION_OF:
                roster_unknown_province.append(team)
                continue
            if team not in roster:
                roster.append(team)

    rows, missing_cols = [], []
    raw_dates, weekly_pairs = [], []
    if gen_values:
        hm = _header_map(gen_values[0])
        missing_cols = [c for c in ("TeamID", "ReportType", "UpdatedAt", "GenStatus") if c.lower() not in hm]
        for idx, r in enumerate(gen_values[1:], start=2):
            if not any(str(c).strip() for c in r):
                continue
            row = {
                "row": idx, "TeamID": _cell(r, hm, "teamid"), "ReportType": _cell(r, hm, "reporttype"),
                "UpdatedAtRaw": _cell(r, hm, "updatedat"), "Period": _cell(r, hm, "period"),
                "Region": _cell(r, hm, "region"), "Province": _cell(r, hm, "province"),
                "GenStatus": _cell(r, hm, "genstatus"), "FuelEnough": _cell(r, hm, "fuelenough"),
                "FuelLevel": _cell(r, hm, "fuellevel"), "Remark": _cell(r, hm, "remark"),
            }
            if row["UpdatedAtRaw"]:
                raw_dates.append(row["UpdatedAtRaw"])
                if row["ReportType"] == WEEKLY_TYPE:
                    weekly_pairs.append((row["UpdatedAtRaw"], row["Period"]))
            rows.append(row)
    dayfirst, how = detect_dayfirst(raw_dates, weekly_pairs)
    for row in rows:
        row["dt"] = parse_sheet_datetime(row["UpdatedAtRaw"], dayfirst=dayfirst)
    return {
        "roster": roster, "rows": rows,
        "diag": {
            "date_format": how, "missing_columns": missing_cols, "teams_error": teams_error,
            "teams_tab": teams_title, "roster_stats": roster_stats,
            "roster_unknown_province": roster_unknown_province, "rows_total": len(rows),
            "unreadable_updatedat": sum(1 for r in rows if r["dt"] is None),
        },
    }


def load_dataset(gs_client, use_cache=True):
    now = time.monotonic()
    with _cache_lock:
        if use_cache and _cache["data"] is not None and now - _cache["ts"] < CACHE_TTL_SECONDS:
            return _cache["data"], False
        try:
            data = build_dataset(*_read_sheets(gs_client))
        except Exception:
            if _cache["data"] is not None:
                # An out-of-date copy beats an error page; the response says it is stale.
                log.exception("Generator Test: refresh failed - serving the last good copy")
                return _cache["data"], True
            raise
        _cache["data"], _cache["ts"] = data, now
        return data, False


# ---------------------------------------------------------------------------
# The analysis (the prompt's rules)
# ---------------------------------------------------------------------------
def _empty_counts(statuses):
    return {"total": 0, "sent": 0, "not_sent": 0, "unknown": 0, **{s: 0 for s in statuses}}


def analyse(dataset, kind, pick):
    """kind: 'weekly' | 'daily'. pick(dt) -> True when a row's Bangkok time is in the chosen week/day."""
    weekly = kind == "weekly"
    report_type = WEEKLY_TYPE if weekly else DAILY_TYPE
    statuses = WEEKLY_STATUSES if weekly else DAILY_STATUSES
    follow_set = WEEKLY_FOLLOW if weekly else DAILY_FOLLOW
    roster = dataset["roster"]
    roster_set = set(roster)

    anomalies = {"dropped_other_team": 0, "dropped_no_time": 0, "dups": [], "mismatch": [],
                 "not_in_roster": [], "period_bad": [], "unknown_status": []}
    by_team, not_in_roster = {}, set()
    for r in dataset["rows"]:
        if r["ReportType"] != report_type:
            continue
        if r["dt"] is None:
            anomalies["dropped_no_time"] += 1
            continue
        if not pick(r["dt"]):
            continue
        if r["TeamID"] not in roster_set:
            if "-NOD-" in r["TeamID"]:
                not_in_roster.add(r["TeamID"])
            else:
                anomalies["dropped_other_team"] += 1
            continue
        by_team.setdefault(r["TeamID"], []).append(r)
    anomalies["not_in_roster"] = sorted(not_in_roster)

    result = {}
    for team, lst in by_team.items():
        lst.sort(key=lambda x: (x["dt"], x["row"]), reverse=True)     # latest UpdatedAt wins; ties -> later sheet row
        if len(lst) > 1:
            anomalies["dups"].append(team)
        r = lst[0]
        prov = province_of(team)
        if r["Region"] != REGION_OF[prov] or r["Province"] != prov:
            anomalies["mismatch"].append(f"{team}: กรอก {r['Region'] or '-'}/{r['Province'] or '-'} แต่ TeamID = {REGION_OF[prov]}/{prov}")
        if weekly:
            want, have = _norm_period(r["Period"]), iso_week_key(r["dt"])
            if want and want != have:
                anomalies["period_bad"].append(f"{team}: Period={r['Period']} แต่ UpdatedAt = {have}")
        if r["GenStatus"] not in statuses:
            anomalies["unknown_status"].append(f"{team}: \"{r['GenStatus']}\"")
        result[team] = r
    anomalies["dups"].sort()

    prov_rows = {p: _empty_counts(statuses) for ps in REGIONS.values() for p in ps}
    reg_rows = {g: _empty_counts(statuses) for g in REGIONS}
    nor = _empty_counts(statuses)
    for t in roster:
        p = province_of(t)
        r = result.get(t)
        for o in (prov_rows[p], reg_rows[REGION_OF[p]], nor):
            o["total"] += 1
            if r:
                o["sent"] += 1
                if r["GenStatus"] in statuses:
                    o[r["GenStatus"]] += 1
                else:
                    o["unknown"] += 1
            else:
                o["not_sent"] += 1

    sent_list = [{
        "team": t, "province": province_of(t), "region": REGION_OF[province_of(t)], "status": r["GenStatus"],
        "fuel_enough": r["FuelEnough"], "fuel_level": r["FuelLevel"], "remark": r["Remark"],
        "updated_at": r["dt"].strftime("%Y-%m-%d %H:%M"),
    } for t, r in sorted(result.items())]
    not_sent = [t for t in roster if t not in result]
    follow = []
    for s in sent_list:
        why = []
        if s["status"] in follow_set:
            why.append(s["status"])
        if s["fuel_enough"] == "ไม่มี":
            why.append("น้ำมันไม่เพียงพอ")
        if s["fuel_level"] == "0-25%":
            why.append("น้ำมัน 0-25%")
        if why:
            follow.append({"team": s["team"], "status": s["status"], "why": why, "remark": s["remark"]})

    sum_p = {g: sum(prov_rows[p]["sent"] for p in ps) for g, ps in REGIONS.items()}
    checks = [
        {"ok": nor["sent"] + nor["not_sent"] == nor["total"], "text": f"ลงข้อมูล ({nor['sent']}) + ยังไม่ส่ง ({nor['not_sent']}) = ทีมทั้งหมด ({nor['total']})"},
        {"ok": all(sum_p[g] == reg_rows[g]["sent"] for g in REGIONS) and sum(sum_p.values()) == nor["sent"],
         "text": f"ผลรวมจังหวัด = Region ({sum_p['NOR1']}+{sum_p['NOR2']}) = NOR รวม ({nor['sent']})"},
        {"ok": len(sent_list) + len(not_sent) == len(roster), "text": f"รายชื่อส่งแล้ว ({len(sent_list)}) + ยังไม่ส่ง ({len(not_sent)}) = roster ({len(roster)})"},
    ]
    return {
        "statuses": statuses, "nor": nor, "reg": reg_rows, "prov": prov_rows, "regions": REGIONS,
        "sent": sent_list, "not_sent": not_sent, "follow": follow, "anomalies": anomalies, "checks": checks,
        "rows_in_period": sum(len(v) for v in by_team.values()),
    }


def build_generator_test_response(gs_client, week=None, day=None, use_cache=True):
    now = bangkok_now()
    today = now.date()
    current_week = iso_week_key(now)
    week = current_week if not week or week == "current" else week
    if not re.fullmatch(r"\d{4}-W\d{2}", week):
        raise ValueError("week must look like 2026-W41")
    try:
        week_monday(week)
    except ValueError:
        raise ValueError("unknown ISO week")
    if not day or day == "today":
        day = today.isoformat()
    try:
        date.fromisoformat(day)
    except ValueError:
        raise ValueError("day must look like 2026-10-06")

    dataset, stale = load_dataset(gs_client, use_cache=use_cache)
    weeks, k = [], current_week
    for i in range(8):
        weeks.append({"key": k, "label": week_label(k), "current": i == 0})
        k = previous_week_key(k)
    base = {
        "as_of": now.strftime("%Y-%m-%d %H:%M"), "today": today.isoformat(), "stale": stale,
        "roster_size": len(dataset["roster"]), "weeks": weeks, "diagnostics": dataset["diag"],
    }
    if not dataset["roster"]:
        # Without the roster nobody can be called "not sent" - say so instead of guessing.
        base.update({"roster_missing": True, "message": "ไม่พบรายชื่อทีม NODE (แท็บ Team: TypeTeam = NODE/NOD และ Active = Y) จึงสรุปทีมที่ยังไม่ส่งไม่ได้"})
        return base

    prev_key = previous_week_key(week)
    weekly = analyse(dataset, "weekly", lambda dt: iso_week_key(dt) == week)
    prev = analyse(dataset, "weekly", lambda dt: iso_week_key(dt) == prev_key)
    daily = analyse(dataset, "daily", lambda dt: dt.date().isoformat() == day)
    base.update({
        "roster_missing": False,
        "week": {"key": week, "label": week_label(week), "is_current": week == current_week},
        "day": {"date": day, "is_today": day == today.isoformat(), "as_of_time": now.strftime("%H:%M")},
        "weekly": weekly, "daily": daily,
        "previous_week": {
            "key": prev_key, "label": week_label(prev_key), "has_data": prev["rows_in_period"] > 0,
            "nor_sent": prev["nor"]["sent"], "reg_sent": {g: prev["reg"][g]["sent"] for g in REGIONS},
        },
    })
    return base
