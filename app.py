import os, json, logging, threading, time, re, math
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, send_from_directory
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler

SHEET_ID      = '1_l5UAj1etjGgLCR4DSG6qDoK8c1unFnO6NVHVwvmbAU'
SHEET_NAME    = 'Sheet1'
CM_BASE       = 3
OFC_BASE      = 2
REBUILD_HOURS = 6
EXCLUDE       = {'PS_CMI_ofc_011','PS_CMI_ofc_012'}
VALID_YEAR    = '2569'   # กรองเฉพาะปี พ.ศ. นี้

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
app = Flask(__name__, template_folder=TEMPLATE_DIR)
CORS(app)

_cache = None
_cache_lock = threading.Lock()
_building   = False

PROV_MAP = {
    'TRUE-TH-BBT-NOR1-CMI1-NOP':'CMI', 'TRUE-TH-BBT-NOR1-CRI-NOP':'CRI',
    'TRUE-TH-BBT-NOR2-PSN-NOP':'PSN',  'TRUE-TH-BBT-NOR2-PCB-NOP':'PCB',
    'TRUE-TH-BBT-NOR2-TAK-NOP':'TAK',  'TRUE-TH-BBT-NOR1-MHS-NOP':'MHS',
    'TRUE-TH-BBT-NOR1-NAN-NOP':'NAN',  'TRUE-TH-BBT-NOR2-PCT-NOP':'PCT',
    'TRUE-TH-BBT-NOR1-LPG-NOP':'LPG',  'TRUE-TH-BBT-NOR2-UTR-NOP':'UTR',
    'TRUE-TH-BBT-NOR2-KPP-NOP':'KPP',  'TRUE-TH-BBT-NOR2-SKT-NOP':'SKT',
    'TRUE-TH-BBT-NOR1-PHE-NOP':'PHE',  'TRUE-TH-BBT-NOR1-LPN-NOP':'LPN',
    'TRUE-TH-BBT-NOR1-PYO-NOP':'PYO'
}
PROV_THAI = {
    'CMI':'เชียงใหม่','CRI':'เชียงราย','PSN':'พิษณุโลก','PCB':'พิจิตร',
    'TAK':'ตาก','MHS':'แม่ฮ่องสอน','NAN':'น่าน','PCT':'พิชัย',
    'LPG':'ลำปาง','UTR':'อุตรดิตถ์','KPP':'กำแพงเพชร','SKT':'สุโขทัย',
    'PHE':'เพชรบูรณ์','LPN':'ลำพูน','PYO':'พะเยา'
}
NOR1 = {'CMI','CRI','MHS','NAN','LPG','PHE','LPN','PYO'}
THAI_MONTHS = ['','ม.ค.','ก.พ.','มี.ค.','เม.ย.','พ.ค.','มิ.ย.',
               'ก.ค.','ส.ค.','ก.ย.','ต.ค.','พ.ย.','ธ.ค.']

def get_client():
    info  = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
    creds = Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    return gspread.authorize(creds)

BOUNDARY_SHEET = 'team_boudary'

def parse_coord(v):
    if not v: return None
    m = re.match(r'([-\d.]+),\s*([-\d.]+)', str(v).strip())
    return (float(m.group(1)), float(m.group(2))) if m else None

def haversine(lat1,lon1,lat2,lon2):
    R=6371; dlat=math.radians(lat2-lat1); dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.asin(math.sqrt(a))

def find_home_coords(coords_list, radius_km=5):
    """หา home base จากพิกัดของ 'งานแรกของวัน' ที่เกิดซ้ำบ่อยที่สุด
    โดยรวมพิกัดที่อยู่ในรัศมีเดียวกันเป็น cluster เดียวกัน
    """
    if not coords_list:
        return None

    best = None
    best_count = 0
    for lat, lon in coords_list:
        cluster = [(lt, ln) for lt, ln in coords_list if haversine(lat, lon, lt, ln) <= radius_km]
        if len(cluster) > best_count:
            best_count = len(cluster)
            best = (
                round(sum(c[0] for c in cluster) / len(cluster), 6),
                round(sum(c[1] for c in cluster) / len(cluster), 6),
                best_count,
                len(coords_list),
            )
    return best

def build_boundary(gc):
    """ดึง boundary data จาก team_boundary sheet"""
    try:
        ws   = gc.open_by_key(SHEET_ID).worksheet(BOUNDARY_SHEET)
        rows = ws.get_all_values()
        if not rows: return []
        headers = [h.strip() for h in rows[0]]
        col = {h:i for i,h in enumerate(headers) if h}
        def g(row,name):
            i=col.get(name)
            return str(row[i]).strip() if i is not None and i<len(row) else ''
        boundary = []
        for row_idx, row in enumerate(rows[1:], start=1):
            tid = g(row,'Team ID')
            if not tid or tid=='nan': continue
            type_team = g(row,'Type Team')
            prov_code = g(row,'Province')
            prov_name = g(row,'Province1')
            home      = g(row,'อำเภอ home base')
            group     = g(row,'Group District')
            reg       = 'NOR1' if prov_code in NOR1 else 'NOR2'
            resp = []
            for i in range(1,10):
                v = g(row, f'อำเภอที่รับผิดชอบที่ {i}')
                if v and v!='nan': resp.append(v)
            boundary.append(dict(
                tid=tid, type=type_team, reg=reg,
                prov=prov_code, prov_name=prov_name,
                home=home, resp=resp,
                group=group
            ))
        log.info(f'Boundary: {len(boundary)} teams')
        return boundary
    except Exception as e:
        log.error(f'build_boundary error: {e}')
        return []


def parse_dt(v):
    """รองรับ D/M/YYYY HH:MM, D/M/YYYY, YYYY-MM-DD HH:MM และ YYYY-MM-DD"""
    if not v: return None
    s = str(v).strip()
    if not s or s.lower() == 'nan':
        return None

    patterns = [
        r'^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$',
        r'^(\d{1,2})/(\d{1,2})/(\d{4})$',
        r'^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$',
        r'^(\d{4})-(\d{1,2})-(\d{1,2})$'
    ]

    for idx, pat in enumerate(patterns):
        m = re.match(pat, s)
        if not m:
            continue
        try:
            if idx == 0:
                d, mo, y, h, mi = map(int, m.groups()[:5])
            elif idx == 1:
                d, mo, y = map(int, m.groups()[:3]); h = 0; mi = 0
            elif idx == 2:
                y, mo, d, h, mi = map(int, m.groups()[:5])
            else:
                y, mo, d = map(int, m.groups()[:3]); h = 0; mi = 0
            if y > 2100:
                y -= 543
            return datetime(y, mo, d, h, mi)
        except Exception:
            return None
    return None


def week_bucket_label(dt):
    """แปลงวันที่ Plan เป็น ISO week ตามปฏิทินสากล
    คืนค่าเป็น dict ที่มี key สำหรับ sort, label และช่วงวันที่ของสัปดาห์
    """
    if not dt:
        return None
    iso_year, iso_week, _ = dt.isocalendar()
    week_start = dt - timedelta(days=dt.weekday())
    week_end = week_start + timedelta(days=6)
    buddhist_iso_year = iso_year + 543
    return {
        'sort': f"{buddhist_iso_year}-{iso_week:02d}",
        'label': f"Wk{iso_week:02d}",
        'iso_year': buddhist_iso_year,
        'iso_week': iso_week,
        'start': to_by_date(week_start),
        'end': to_by_date(week_end),
    }

def to_by_month(dt):
    """datetime ค.ศ. → พ.ศ. month string เช่น 2569-01"""
    if not dt: return None
    return f'{dt.year+543}-{dt.month:02d}'

def to_by_date(dt):
    """datetime ค.ศ. → พ.ศ. date string เช่น 2569-01-15"""
    if not dt: return None
    return f'{dt.year+543}-{dt.month:02d}-{dt.day:02d}'

def fmt_time(v):
    if not v: return ''
    m = re.search(r'(\d{1,2}:\d{2})', str(v))
    return m.group(1) if m else str(v).strip()

def dedupe_ticket_key(ticket_value, row_index=None):
    s = str(ticket_value or '').strip()
    if s:
        return s
    return f'ROW_{row_index if row_index is not None else "X"}'

def is_real_active_team_row(status_value, dt_travel=None, dt_start=None):
    status = str(status_value or '').strip()
    if dt_travel is not None or dt_start is not None:
        return True
    return ('เดินทาง' in status) or ('เริ่มซ่อม' in status)

def is_valid_month(month_str):
    """กรองเฉพาะปี พ.ศ. ปัจจุบัน"""
    return month_str and month_str.startswith(VALID_YEAR + '-')

def build_data():
    log.info('Building dashboard data...')
    t0 = time.time()

    gc   = get_client()
    ws   = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    rows = ws.get_all_values()
    if not rows: raise RuntimeError('Sheet is empty')

    headers = [' '.join(h.split()) for h in rows[0]]
    col = {h: i for i, h in enumerate(headers) if h}
    def fc(*names):
        for n in names:
            if n in col: return col[n]
            for k,v in col.items():
                if k.lower()==n.lower(): return v
        return None

    C = {
        'team_id':      fc('Team ID'),
        'type_team':    fc('Type Team'),
        'province':     fc('Province'),
        'ticket':       fc('Ticket'),
        'sla':          fc('SLA'),
        'subject':      fc('Subject'),
        'que':          fc('Que'),
        'travel':       fc('เวลาเดินทาง'),
        'start':        fc('เวลาเริ่มซ่อม'),
        'hold':         fc('Hold'),
        'linkup':       fc('Link Up'),
        'status':       fc('Status Team'),
        'holdcause':    fc('สาเหตุการ Hold'),
        'log':          fc('Update Log'),
        'cause1':       fc('สาเหตุ 1'),
        'fix1':         fc('วิธีแก้ไข'),
        'update_pikat': fc('Update พิกัด'),
        'plan':         fc('Plan', 'PLAN'),
    }

    def g(row, key):
        i = C.get(key)
        return str(row[i]).strip() if i is not None and i < len(row) else ''

    teams  = {}
    months = set()
    drill  = {}
    cutoff = datetime.now() - timedelta(days=90)
    plan_daily = {}
    plan_weekly = {}
    team_plan_daily = {}
    team_plan_weekly = {}
    daily_team_stats = {}
    drill_seen_keys = {}

    for row_idx, row in enumerate(rows[1:], start=1):
        team_id   = g(row,'team_id')
        type_team = g(row,'type_team')
        prov_full = g(row,'province')
        if not team_id or type_team not in ('CM','OFC'): continue
        if team_id in EXCLUDE: continue

        prov = PROV_MAP.get(prov_full,'')
        reg  = 'NOR1' if prov in NOR1 else 'NOR2'

        dt_linkup = parse_dt(g(row,'linkup'))
        dt_travel = parse_dt(g(row,'travel'))
        dt_start  = parse_dt(g(row,'start'))
        dt_hold   = parse_dt(g(row,'hold'))
        dt_plan   = parse_dt(g(row,'plan'))

        has_lu   = dt_linkup is not None
        has_hold = dt_hold   is not None

        # date จาก Link Up ?? เวลาเดินทาง
        dt_date   = dt_linkup or dt_travel
        month_str = to_by_month(dt_date)
        date_str  = to_by_date(dt_date)

        # กรองเฉพาะ valid year (2569) — ตัด outlier
        row_valid = is_valid_month(month_str)
        if month_str: months.add(month_str) if row_valid else None


        # is_ticket: TT หรือ INC (ตาม v17 label "TT + INC pattern")
        # แต่นับเฉพาะ row ที่ year valid
        tkt_val   = g(row,'ticket')
        ticket_key = dedupe_ticket_key(tkt_val, row_idx)
        is_ticket = tkt_val.startswith('TT') or tkt_val.startswith('INC')

        # เก็บ Update พิกัด สำหรับ homeCoords
        coord_raw = g(row,'update_pikat')
        # last_ts = เวลาปิดงานล่าสุดของ row ระหว่าง Link Up / Hold
        last_candidates = [d for d in (dt_linkup, dt_hold) if d is not None]
        last_ts = max(last_candidates) if last_candidates else None
        status_val = g(row,'status')
        sla_val = g(row,'sla')
        que_val = g(row,'que')
        holdcause_val = g(row,'holdcause')
        cause1_val = g(row,'cause1')
        log_val = g(row,'log')
        work_start = dt_travel or dt_start
        row_work_hrs = 0
        if work_start and last_ts:
            diff = (last_ts - work_start).total_seconds() / 3600
            if 0 < diff < 24:
                row_work_hrs = round(diff, 2)

        if team_id not in teams:
            teams[team_id] = dict(
                type=type_team, prov=prov, reg=reg,
                pdt1_dates={}, pdt2_dates={},
                pdt1_keys={}, pdt2_keys={},
                all_dates=set(),
                day_first_travel={},
                day_last_ts={},
                day_first_coord={},
                tkt=0, non=0, monthly={},
                coords=[],
                summary={'all': None, 'by_month': {}}
            )
        tm = teams[team_id]

        def ensure_summary(month_key=None):
            bucket_map = tm['summary']['by_month'] if month_key else tm['summary']
            bucket_key = month_key if month_key else 'all'
            if bucket_map.get(bucket_key) is None:
                bucket_map[bucket_key] = {
                    'tot': 0, 'tkt': 0, 'non': 0,
                    'done_p1': 0, 'done_p2': 0,
                    'hold': 0, 'inc_work': 0,
                    'st': {}, 'sw': {}, 'sla': {},
                    'logs': [], 'hr': [], 'c1': [],
                    'que_rows': {}, 'que_hours': {},
                    'days_set': set(),
                }
            return bucket_map[bucket_key]

        summary_all = ensure_summary()
        summary_month = ensure_summary(month_str) if row_valid and month_str else None

        # เก็บ coords และจดจำพิกัดของ 'งานแรกของวัน'
        coord = parse_coord(coord_raw) if coord_raw else None
        if coord and row_valid:
            tm['coords'].append(coord)

        # นับ tkt/non เฉพาะ valid year rows
        if row_valid:
            if is_ticket: tm['tkt'] += 1
            else:         tm['non'] += 1

            for sb in [summary_all, summary_month]:
                if not sb:
                    continue
                sb['tot'] += 1
                if is_ticket:
                    sb['tkt'] += 1
                else:
                    sb['non'] += 1
                    non_key = que_val or 'Non-Ticket'
                    sb['sw'][non_key] = sb['sw'].get(non_key, 0) + 1
                if has_lu:
                    sb['done_p1'] += 1
                if has_lu or has_hold:
                    sb['done_p2'] += 1
                if has_hold:
                    sb['hold'] += 1
                if status_val:
                    sb['st'][status_val] = sb['st'].get(status_val, 0) + 1
                    if 'ไม่แล้วเสร็จ' in status_val:
                        sb['inc_work'] += 1
                if sla_val:
                    sb['sla'][sla_val] = sb['sla'].get(sla_val, 0) + 1
                if holdcause_val:
                    sb['hr'].append(holdcause_val)
                if cause1_val:
                    sb['c1'].append(cause1_val)
                if log_val:
                    sb['logs'].append(log_val)
                if que_val:
                    sb['que_rows'][que_val] = sb['que_rows'].get(que_val, 0) + 1
                    sb['que_hours'][que_val] = sb['que_hours'].get(que_val, 0) + row_work_hrs
                if date_str:
                    sb['days_set'].add(date_str)

        # PDT counts เฉพาะ valid rows
        if row_valid and date_str:
            tm['all_dates'].add(date_str)
            # Logic C: track min travel และ max last_ts per day
            if dt_travel:
                prev = tm['day_first_travel'].get(date_str)
                if prev is None or dt_travel < prev:
                    tm['day_first_travel'][date_str] = dt_travel
            if last_ts:
                prev = tm['day_last_ts'].get(date_str)
                if prev is None or last_ts > prev:
                    tm['day_last_ts'][date_str] = last_ts

            # Home base = พิกัดของงานแรกที่เช็คอินบ่อยที่สุดในแต่ละวัน
            # ใช้เวลาเดินทางก่อน, ถ้าไม่มีใช้เวลาเริ่มซ่อม, ถ้ายังไม่มีใช้ Link Up/Hold fallback
            checkin_dt = dt_travel or dt_start or dt_linkup or dt_hold
            if coord and checkin_dt:
                prev = tm['day_first_coord'].get(date_str)
                if prev is None or checkin_dt < prev['dt']:
                    tm['day_first_coord'][date_str] = {'dt': checkin_dt, 'coord': coord}
            if has_lu:
                tm['pdt1_keys'].setdefault(date_str, set()).add(ticket_key)
                tm['pdt1_dates'][date_str] = len(tm['pdt1_keys'][date_str])
            if has_lu or has_hold:
                tm['pdt2_keys'].setdefault(date_str, set()).add(ticket_key)
                tm['pdt2_dates'][date_str] = len(tm['pdt2_keys'][date_str])
            if month_str:
                if month_str not in tm['monthly']:
                    tm['monthly'][month_str] = {'p1d':{},'p2d':{},'p1keys':{},'p2keys':{},'dates':set(),'tkt':0,'non':0,'first':{},'last':{}}
                mm = tm['monthly'][month_str]
                mm['dates'].add(date_str)
                if is_ticket: mm['tkt'] += 1
                else: mm['non'] += 1
                if dt_travel:
                    prev = mm['first'].get(date_str)
                    if prev is None or dt_travel < prev: mm['first'][date_str] = dt_travel
                if last_ts:
                    prev = mm['last'].get(date_str)
                    if prev is None or last_ts > prev: mm['last'][date_str] = last_ts
                if has_lu:
                    mm['p1keys'].setdefault(date_str, set()).add(ticket_key)
                    mm['p1d'][date_str] = len(mm['p1keys'][date_str])
                if has_lu or has_hold:
                    mm['p2keys'].setdefault(date_str, set()).add(ticket_key)
                    mm['p2d'][date_str] = len(mm['p2keys'][date_str])

        # trend รายวัน/รายสัปดาห์ จากวันที่ในคอลัมน์ Plan
        plan_month = to_by_month(dt_plan)
        plan_date  = to_by_date(dt_plan)
        if dt_plan and plan_month and is_valid_month(plan_month) and plan_date:
            day_key = f"{plan_date}||{plan_month}||{reg}||{type_team}"
            # รายวัน: นับตามวันตรง ๆ
            if day_key not in plan_daily:
                plan_daily[day_key] = {}
            if team_id not in plan_daily[day_key]:
                plan_daily[day_key][team_id] = {'p1': 0, 'p2': 0, 'p1keys': set(), 'p2keys': set()}
            if has_lu:
                plan_daily[day_key][team_id]['p1keys'].add(ticket_key)
                plan_daily[day_key][team_id]['p1'] = len(plan_daily[day_key][team_id]['p1keys'])
            if has_lu or has_hold:
                plan_daily[day_key][team_id]['p2keys'].add(ticket_key)
                plan_daily[day_key][team_id]['p2'] = len(plan_daily[day_key][team_id]['p2keys'])

            if team_id not in team_plan_daily:
                team_plan_daily[team_id] = {}
            if plan_date not in team_plan_daily[team_id]:
                team_plan_daily[team_id][plan_date] = {'p1': 0, 'p2': 0, 'p1keys': set(), 'p2keys': set(), 'm': plan_month, 'reg': reg, 'type': type_team}
            if has_lu:
                team_plan_daily[team_id][plan_date]['p1keys'].add(ticket_key)
                team_plan_daily[team_id][plan_date]['p1'] = len(team_plan_daily[team_id][plan_date]['p1keys'])
            if has_lu or has_hold:
                team_plan_daily[team_id][plan_date]['p2keys'].add(ticket_key)
                team_plan_daily[team_id][plan_date]['p2'] = len(team_plan_daily[team_id][plan_date]['p2keys'])

            # รายสัปดาห์: ISO week ตามปฏิทินสากล
            wk = week_bucket_label(dt_plan)
            wk_key  = f"{wk['sort']}||{wk['label']}||{plan_month}||{reg}||{type_team}"
            if wk_key not in plan_weekly:
                plan_weekly[wk_key] = {'meta': wk, 'teams': {}}
            if team_id not in plan_weekly[wk_key]['teams']:
                plan_weekly[wk_key]['teams'][team_id] = {'p1': 0, 'p2': 0, 'p1keys': set(), 'p2keys': set(), 'dates': set()}
            plan_weekly[wk_key]['teams'][team_id]['dates'].add(plan_date)
            if has_lu:
                plan_weekly[wk_key]['teams'][team_id]['p1keys'].add(ticket_key)
                plan_weekly[wk_key]['teams'][team_id]['p1'] = len(plan_weekly[wk_key]['teams'][team_id]['p1keys'])
            if has_lu or has_hold:
                plan_weekly[wk_key]['teams'][team_id]['p2keys'].add(ticket_key)
                plan_weekly[wk_key]['teams'][team_id]['p2'] = len(plan_weekly[wk_key]['teams'][team_id]['p2keys'])

            if team_id not in team_plan_weekly:
                team_plan_weekly[team_id] = {}
            team_wk_key = f"{wk['sort']}||{wk['label']}"
            if team_wk_key not in team_plan_weekly[team_id]:
                team_plan_weekly[team_id][team_wk_key] = {'p1': 0, 'p2': 0, 'p1keys': set(), 'p2keys': set(), 'dates': set(), 'm': plan_month, 'reg': reg, 'type': type_team, 'meta': wk}
            team_plan_weekly[team_id][team_wk_key]['dates'].add(plan_date)
            if has_lu:
                team_plan_weekly[team_id][team_wk_key]['p1keys'].add(ticket_key)
                team_plan_weekly[team_id][team_wk_key]['p1'] = len(team_plan_weekly[team_id][team_wk_key]['p1keys'])
            if has_lu or has_hold:
                team_plan_weekly[team_id][team_wk_key]['p2keys'].add(ticket_key)
                team_plan_weekly[team_id][team_wk_key]['p2'] = len(team_plan_weekly[team_id][team_wk_key]['p2keys'])

            if plan_date not in daily_team_stats:
                daily_team_stats[plan_date] = {}
            if team_id not in daily_team_stats[plan_date]:
                daily_team_stats[plan_date][team_id] = {
                    'id': team_id,
                    'type': type_team,
                    'reg': reg,
                    'prov': prov,
                    'p1': 0,
                    'p2': 0,
                    'rows': 0,
                    'is_active': False,
                    'p1keys': set(),
                    'p2keys': set(),
                }
            daily_team_stats[plan_date][team_id]['rows'] += 1
            if is_real_active_team_row(status_val, dt_travel, dt_start):
                daily_team_stats[plan_date][team_id]['is_active'] = True
            if has_lu:
                daily_team_stats[plan_date][team_id]['p1keys'].add(ticket_key)
                daily_team_stats[plan_date][team_id]['p1'] = len(daily_team_stats[plan_date][team_id]['p1keys'])
            if has_lu or has_hold:
                daily_team_stats[plan_date][team_id]['p2keys'].add(ticket_key)
                daily_team_stats[plan_date][team_id]['p2'] = len(daily_team_stats[plan_date][team_id]['p2keys'])

        # drill down รายวันให้ยึดวันจาก Plan เป็นหลัก เพื่อให้ตรงกับ Summary
        drill_dt = dt_plan if (dt_plan and is_valid_month(plan_month)) else (dt_linkup or dt_travel or dt_start or dt_hold)
        drill_date_str = to_by_date(drill_dt)
        drill_month_str = to_by_month(drill_dt)
        if drill_dt and drill_dt >= cutoff and drill_date_str and is_valid_month(drill_month_str):
            if team_id not in drill:
                drill[team_id] = {}
            if drill_date_str not in drill[team_id]:
                drill[team_id][drill_date_str] = []
            drill_seen_keys.setdefault(team_id, {}).setdefault(drill_date_str, {'p1': set(), 'p2': set()})
            row_p1 = 0
            row_p2 = 0
            if has_lu and ticket_key not in drill_seen_keys[team_id][drill_date_str]['p1']:
                drill_seen_keys[team_id][drill_date_str]['p1'].add(ticket_key)
                row_p1 = 1
            if (has_lu or has_hold) and ticket_key not in drill_seen_keys[team_id][drill_date_str]['p2']:
                drill_seen_keys[team_id][drill_date_str]['p2'].add(ticket_key)
                row_p2 = 1
            if len(drill[team_id][drill_date_str]) < 50:
                drill[team_id][drill_date_str].append([
                    tkt_val,
                    g(row,'sla'),
                    g(row,'subject')[:80],
                    g(row,'que'),
                    fmt_time(g(row,'travel')),
                    fmt_time(g(row,'start')),
                    fmt_time(g(row,'hold')),
                    fmt_time(g(row,'linkup')),
                    g(row,'status'),
                    g(row,'holdcause'),
                    g(row,'log')[:150],
                    g(row,'cause1'),
                    g(row,'fix1'),
                    '',
                    'Ticket' if is_ticket else 'Non-Ticket',
                    row_p1,
                    row_p2,
                    row_work_hrs,
                    drill_month_str or '',
                ])

    log.info(f'Parsed {len(teams)} teams, months={sorted(months)}')

    sorted_months = sorted(months)
    ml = {}
    for m in sorted_months:
        p = m.split('-')
        ml[m] = f'{THAI_MONTHS[int(p[1])]} {p[0][2:]}'

    ts = []; rank_data = []
    for tid, tm in teams.items():
        days = len(tm['all_dates'])
        if days == 0: continue
        tot1 = sum(tm['pdt1_dates'].values())
        tot2 = sum(tm['pdt2_dates'].values())
        p1   = round(tot1/days, 2)
        p2   = round(tot2/days, 2)
        base = CM_BASE if tm['type']=='CM' else OFC_BASE
        vs1  = round(p1-base, 2)
        vs2  = round(p2-base, 2)
        st   = 'above' if vs1>=0 else ('below' if vs1<-0.5 else 'near')
        max1 = max(tm['pdt1_dates'].values()) if tm['pdt1_dates'] else 0
        max2 = max(tm['pdt2_dates'].values()) if tm['pdt2_dates'] else 0
        daily_h_vals = [round((tm['day_last_ts'][d] - tm['day_first_travel'][d]).total_seconds()/3600, 2) for d in tm['all_dates'] if d in tm['day_first_travel'] and d in tm['day_last_ts'] and 0 < (tm['day_last_ts'][d] - tm['day_first_travel'][d]).total_seconds()/3600 < 24]
        h = round(sum(daily_h_vals)/len(daily_h_vals), 2) if daily_h_vals else 0

        month_stats = {}
        month_work_days_list = []
        for m, mm in tm['monthly'].items():
            mdays = len(mm['dates'])
            mtot1 = sum(mm['p1d'].values()) if mm['p1d'] else 0
            mtot2 = sum(mm['p2d'].values()) if mm['p2d'] else 0
            mp1 = round(mtot1 / mdays, 2) if mdays else 0
            mp2 = round(mtot2 / mdays, 2) if mdays else 0
            mhours = []
            for d in mm['dates']:
                if d in mm['first'] and d in mm['last']:
                    diff = (mm['last'][d] - mm['first'][d]).total_seconds() / 3600
                    if 0 < diff < 24:
                        mhours.append(round(diff, 2))
            mh = round(sum(mhours)/len(mhours), 2) if mhours else 0
            month_work_days_list.append(mdays)
            month_stats[m] = {
                'p1': mp1, 'p2': mp2, 'tot1': mtot1, 'tot2': mtot2,
                'days': mdays, 'tkt': mm.get('tkt', 0), 'non': mm.get('non', 0),
                'h': mh,
                'off_days': max(0, 30 - mdays),
                'max1': max(mm['p1d'].values()) if mm['p1d'] else 0,
                'max2': max(mm['p2d'].values()) if mm['p2d'] else 0,
            }
            month_stats[m]['vs1'] = round(month_stats[m]['p1'] - base, 2)
            month_stats[m]['vs2'] = round(month_stats[m]['p2'] - base, 2)
            month_stats[m]['st'] = 'above' if month_stats[m]['vs1'] >= 0 else ('below' if month_stats[m]['vs1'] < -0.5 else 'near')

        avg_month_days = round(sum(month_work_days_list)/len(month_work_days_list), 1) if month_work_days_list else 0
        avg_month_off_days = round(sum(max(0, 30 - d) for d in month_work_days_list)/len(month_work_days_list), 1) if month_work_days_list else 0

        ts.append(dict(
            id=tid, type=tm['type'], reg=tm['reg'], prov=tm['prov'],
            pn=PROV_THAI.get(tm['prov'],tm['prov']),
            p1=p1, p2=p2, tot1=tot1, tot2=tot2,
            h=h, days=days, max1=max1, max2=max2,
            avgMonthDays=avg_month_days, avgMonthOffDays=avg_month_off_days,
            base=base, vs1=vs1, vs2=vs2, st=st, tkt=tm['tkt'], non=tm['non'],
            monthStats=month_stats
        ))

        rd = dict(id=tid, type=tm['type'], reg=tm['reg'], prov=tm['prov'],
                  p1_avg=p1, p2_avg=p2, wd_avg=days)
        for m in sorted_months:
            mm = tm['monthly'].get(m)
            if mm and mm['dates']:
                md  = len(mm['dates'])
                mp1 = round(sum(mm['p1d'].values())/md, 2) if mm['p1d'] else 0
                mp2 = round(sum(mm['p2d'].values())/md, 2) if mm['p2d'] else 0
            else:
                mp1=mp2=md=0
            rd[f'p1_{m}']=mp1; rd[f'p2_{m}']=mp2; rd[f'wd_{m}']=md
        rank_data.append(rd)

    tr_map = {}
    for t in ts:
        tm = teams[t['id']]
        for m in sorted_months:
            mm = tm['monthly'].get(m)
            if not mm or not mm['dates']: continue
            md   = len(mm['dates'])
            avg1 = round(sum(mm['p1d'].values())/md, 2) if mm['p1d'] else 0
            avg2 = round(sum(mm['p2d'].values())/md, 2) if mm['p2d'] else 0
            k = f"{m}||{t['reg']}||{t['type']}"
            if k not in tr_map: tr_map[k] = {'s1':0,'s2':0,'cnt':0}
            tr_map[k]['s1']+=avg1; tr_map[k]['s2']+=avg2; tr_map[k]['cnt']+=1
    tr = [dict(m=k.split('||')[0], reg=k.split('||')[1], type=k.split('||')[2],
               avg=round(v['s2']/v['cnt'],2), avg_p1=round(v['s1']/v['cnt'],2))
          for k,v in tr_map.items()]

    heat_map = {}
    for t in ts:
        tm = teams[t['id']]
        for m, mm in tm['monthly'].items():
            if not mm['dates']:
                continue
            md  = len(mm['dates'])
            avg = round(sum(mm['p1d'].values())/md, 2) if mm['p1d'] else 0
            hk  = f"{m}||{t['prov']}||{t['reg']}||{t['type']}"
            if hk not in heat_map:
                heat_map[hk] = {'sum':0,'cnt':0,'tkt':0}
            heat_map[hk]['sum'] += avg
            heat_map[hk]['cnt'] += 1
            heat_map[hk]['tkt'] += mm.get('tkt', 0)
    heat = [
        dict(
            m=k.split('||')[0], pv=k.split('||')[1], reg=k.split('||')[2], type=k.split('||')[3],
            avg=round(v['sum']/v['cnt'],2), tot=v['cnt'], tkt=v.get('tkt',0)
        )
        for k,v in heat_map.items()
    ]

    def finalize_plan_trend(bucket):
        out = []
        for key, team_map in bucket.items():
            label, month_key, reg, ttype = key.split('||')
            cnt = len(team_map)
            if cnt == 0:
                continue
            s1 = sum(v['p1'] for v in team_map.values())
            s2 = sum(v['p2'] for v in team_map.values())
            out.append(dict(label=label, m=month_key, reg=reg, type=ttype,
                            avg_p1=round(s1/cnt, 2), avg=round(s2/cnt, 2), teams=cnt))
        return out

    def finalize_week_trend(bucket):
        out = []
        for key, payload in bucket.items():
            sort_key, label, month_key, reg, ttype = key.split('||')
            meta = payload.get('meta', {})
            team_map = payload.get('teams', {})
            team_vals_p1 = []
            team_vals_p2 = []
            for v in team_map.values():
                dcount = len(v.get('dates', set()))
                if dcount <= 0:
                    continue
                team_vals_p1.append(v['p1'] / dcount)
                team_vals_p2.append(v['p2'] / dcount)
            cnt = len(team_vals_p1)
            if cnt == 0:
                continue
            out.append(dict(
                sort=sort_key, label=label, m=month_key, reg=reg, type=ttype,
                avg_p1=round(sum(team_vals_p1)/cnt, 2),
                avg=round(sum(team_vals_p2)/cnt, 2), teams=cnt,
                start=meta.get('start'), end=meta.get('end'),
                iso_year=meta.get('iso_year'), iso_week=meta.get('iso_week')
            ))
        return out

    # รายสัปดาห์: avg PDT/วัน ต่อทีม ภายใน Wk ของเดือน โดยอิงวันที่ในคอลัมน์ Plan
    tr_week = finalize_week_trend(plan_weekly)
    tr_day = finalize_plan_trend(plan_daily)

    team_tr_month = {}
    for t in ts:
        rows = []
        for m in sorted_months:
            mm = t.get('monthStats', {}).get(m)
            if not mm:
                continue
            rows.append(dict(m=m, label=m, reg=t['reg'], type=t['type'], avg_p1=mm.get('p1', 0), avg=mm.get('p2', 0)))
        team_tr_month[t['id']] = rows

    team_tr_day = {}
    for tid, day_map in team_plan_daily.items():
        rows = []
        for label, v in sorted(day_map.items(), key=lambda x: x[0]):
            rows.append(dict(label=label, m=v.get('m'), reg=v.get('reg'), type=v.get('type'), avg_p1=round(v['p1'], 2), avg=round(v['p2'], 2), teams=1))
        team_tr_day[tid] = rows

    team_tr_week = {}
    for tid, wk_map in team_plan_weekly.items():
        rows = []
        for _, v in sorted(wk_map.items(), key=lambda x: x[0]):
            dcount = len(v.get('dates', set()))
            if dcount <= 0:
                continue
            meta = v.get('meta', {})
            rows.append(dict(sort=meta.get('sort'), label=meta.get('label'), m=v.get('m'), reg=v.get('reg'), type=v.get('type'), avg_p1=round(v['p1']/dcount, 2), avg=round(v['p2']/dcount, 2), teams=1, start=meta.get('start'), end=meta.get('end'), iso_year=meta.get('iso_year'), iso_week=meta.get('iso_week')))
        team_tr_week[tid] = rows

    prov_names = {p:PROV_THAI.get(p,p) for p in set(t['prov'] for t in ts)}
    nor1_list  = list(set(t['prov'] for t in ts if t['reg']=='NOR1'))

    # ── Build homeCoords จาก Update พิกัด ────────────────────
    # ใช้เฉพาะพิกัดของ 'งานแรกของวัน' เพื่อให้ได้ home base ตามที่ต้องการ
    home_coords = {}
    for tid, tm in teams.items():
        first_job_coords = [v['coord'] for _, v in sorted(tm.get('day_first_coord', {}).items()) if v.get('coord')]
        result = find_home_coords(first_job_coords)
        if result:
            home_coords[tid] = {
                'lat': result[0], 'lon': result[1],
                'count': result[2], 'total': result[3],
                'method': 'first-job-checkin-cluster'
            }
    log.info(f'homeCoords: {len(home_coords)} teams (from first-job check-in)')

    # ── Build boundary จาก team_boundary sheet ───────────────
    boundary = build_boundary(gc)

    elapsed = round(time.time()-t0,1)
    log.info(f'Done: {len(ts)} teams, {len(sorted_months)} months, {elapsed}s')
    sample = [(t['id'],t['p1'],t['vs1']) for t in sorted(ts,key=lambda x:x['p1'])[:3]]
    log.info(f'Sample lowest p1: {sample}')
    log.info(f'gstats: tkt={sum(t["tkt"] for t in ts)} non={sum(t["non"] for t in ts)}')

    # Build sum per team (for analysis card and team detail)
    def summarize_logs(logs):
        joined = ' '.join(logs).lower()
        items = []
        if 'spare' in joined or 'อะไหล่' in joined:
            items.append({'type': 'warn', 'text': 'พบประเด็นอะไหล่ / spare part ใน log'})
        if 'ฝน' in joined or 'น้ำท่วม' in joined or 'weather' in joined:
            items.append({'type': 'warn', 'text': 'พบผลกระทบสภาพอากาศ'})
        if 'permission' in joined or 'ขออนุญาต' in joined:
            items.append({'type': 'warn', 'text': 'พบประเด็น site permission / ขออนุญาต'})
        if 'link up' in joined:
            items.append({'type': 'success', 'text': 'มีงานที่ปิดด้วย Link Up ตาม log'})
        if not items and logs:
            items.append({'type': 'info', 'text': 'มี log หน้างานให้ตรวจสอบเพิ่มเติม'})
        return items[:4]

    def build_sum_bucket(raw_bucket, team_obj=None):
        if not raw_bucket:
            return None
        qh = {}
        for q, cnt in raw_bucket.get('que_rows', {}).items():
            if cnt:
                qh[q] = round(raw_bucket.get('que_hours', {}).get(q, 0) / cnt, 2)
        hold = raw_bucket.get('hold', 0)
        tot = raw_bucket.get('tot', 0)
        hold_pct = round((hold / tot) * 100, 1) if tot else 0
        logs = raw_bucket.get('logs', [])[-8:][::-1]
        c1 = sorted(raw_bucket.get('c1', {}).items(), key=lambda x: (-x[1], x[0])) if isinstance(raw_bucket.get('c1'), dict) else None
        if c1 is None:
            c1_counts = {}
            for item in raw_bucket.get('c1', []):
                c1_counts[item] = c1_counts.get(item, 0) + 1
            c1 = sorted(c1_counts.items(), key=lambda x: (-x[1], x[0]))
        hr_counts = {}
        for item in raw_bucket.get('hr', []):
            hr_counts[item] = hr_counts.get(item, 0) + 1
        c1_list = [k for k, _ in c1[:5]]
        hr_list = [f"{k} ({v})" for k, v in sorted(hr_counts.items(), key=lambda x: (-x[1], x[0]))[:5]]
        days = len(raw_bucket.get('days_set', set()))
        return {
            'tot': tot,
            'tkt': raw_bucket.get('tkt', 0),
            'non': raw_bucket.get('non', 0),
            'days': days,
            'hold': hold,
            'hold_pct': hold_pct,
            'inc_work': raw_bucket.get('inc_work', 0),
            'sw': raw_bucket.get('sw', {}),
            'st': raw_bucket.get('st', {}),
            'z': 0,
            'qh': qh,
            'hr': hr_list,
            'c1': c1_list,
            'logs': logs,
            'sla': raw_bucket.get('sla', {}),
            'done_p1': raw_bucket.get('done_p1', 0),
            'done_p2': raw_bucket.get('done_p2', 0),
        }

    def build_sla_bucket(sum_bucket):
        if not sum_bucket:
            return None
        sla_counts = sum_bucket.get('sla', {}) or {}
        total = sum(sla_counts.values())
        if total <= 0:
            return {
                'hard_pct': 0, 'medium_pct': 0, 'normal_pct': 0, 'easy_pct': 0,
                'top_sla': [], 'log_summary': summarize_logs(sum_bucket.get('logs', [])),
                'total_logs': len(sum_bucket.get('logs', []))
            }
        def diff_of(sla_name):
            s = str(sla_name).upper().strip()
            if s in ('NSA1', 'NSA2', 'SA1', 'SA2', 'HSP1 = SA1 4H', 'HSP2 = SA2 4H'):
                return 'hard'
            if s in ('NSA3', 'SA3', 'HSP3 = SA3 4H'):
                return 'medium'
            if s in ('NSA4', 'SA4', 'CSA'):
                return 'normal'
            if s in ('NSA5', 'PSA5'):
                return 'easy'
            return 'unknown'
        diff_counts = {'hard': 0, 'medium': 0, 'normal': 0, 'easy': 0, 'unknown': 0}
        top_sla = []
        for sla_name, cnt in sorted(sla_counts.items(), key=lambda x: (-x[1], x[0])):
            diff = diff_of(sla_name)
            diff_counts[diff] += cnt
            top_sla.append({'sla': sla_name, 'cnt': cnt, 'pct': round((cnt / total) * 100, 1), 'diff': diff})
        return {
            'hard_pct': round((diff_counts['hard'] / total) * 100, 1),
            'medium_pct': round((diff_counts['medium'] / total) * 100, 1),
            'normal_pct': round((diff_counts['normal'] / total) * 100, 1),
            'easy_pct': round((diff_counts['easy'] / total) * 100, 1),
            'top_sla': top_sla[:5],
            'log_summary': summarize_logs(sum_bucket.get('logs', [])),
            'total_logs': len(sum_bucket.get('logs', []))
        }

    sum_data = {}
    sla_data = {}
    for t in ts:
        raw_all = teams[t['id']]['summary'].get('all') or {}
        team_sum = build_sum_bucket(raw_all, t) or {'tot': 0, 'tkt': 0, 'non': 0, 'days': 0, 'hold': 0, 'hold_pct': 0, 'inc_work': 0, 'sw': {}, 'st': {}, 'z': 0, 'qh': {}, 'hr': [], 'c1': [], 'logs': [], 'sla': {}, 'done_p1': 0, 'done_p2': 0}
        team_sum['by_month'] = {}
        team_sla = build_sla_bucket(team_sum)
        team_sla['by_month'] = {}
        for m in sorted_months:
            raw_month = teams[t['id']]['summary']['by_month'].get(m)
            month_sum = build_sum_bucket(raw_month, t)
            if month_sum:
                team_sum['by_month'][m] = month_sum
                team_sla['by_month'][m] = build_sla_bucket(month_sum)
        sum_data[t['id']] = team_sum
        sla_data[t['id']] = team_sla

    summary_daily = []
    for dt in sorted(daily_team_stats.keys()):
        teams_rows = []
        for _, rec in sorted(daily_team_stats[dt].items(), key=lambda x: (x[1]['type'], x[1]['prov'], x[0])):
            teams_rows.append({
                'id': rec['id'],
                'type': rec['type'],
                'reg': rec['reg'],
                'prov': rec['prov'],
                'p1': rec['p1'],
                'p2': rec['p2'],
                'rows': rec['rows'],
                'is_active': bool(rec.get('is_active')),
            })
        summary_daily.append({'date': dt, 'teams': teams_rows})

    return dict(
        ts=ts, tr=tr, tr_week=tr_week, tr_day=tr_day, team_tr_month=team_tr_month, team_tr_week=team_tr_week, team_tr_day=team_tr_day, heat=heat, wk=[],
        prov=prov_names, nor1=nor1_list,
        months=sorted_months, ml=ml, sum=sum_data,
        gstats=dict(
            total_tkt=sum(t['tkt'] for t in ts),
            total_non=sum(t['non'] for t in ts),
            total_rows=sum(t['tkt']+t['non'] for t in ts)
        ),
        rankData=rank_data, boundary=boundary, homeCoords=home_coords,
        drill=drill, slaData=sla_data,
        summaryDaily=summary_daily,
        baseConfig=dict(CM=CM_BASE, OFC=OFC_BASE),
        cached_at=datetime.now().isoformat()
    )

def rebuild_cache():
    global _cache, _building
    if _building: return
    _building = True
    try:
        data = build_data()
        with _cache_lock: _cache = data
        log.info('Cache updated ✓')
    except Exception as e:
        log.error(f'rebuild_cache error: {e}')
        import traceback; traceback.print_exc()
    finally:
        _building = False



REALTIME_SHEET_IDS = {
    'NOR1': '1t8DErfQLBRXkoaorDFMdAxtLZf-RA_hzQ-xFQI6EgBo',
    'NOR2': '1q5xC5lQv2-FhjM-h_o4xqw1dpmei8oB5xo7lSyQm8us',
}
REALTIME_SHEET_NAMES = ['ชีต 1', 'Sheet1', 'sheet1', 'ชีต1']


def _rt_open_worksheet(gc, sheet_id):
    sh = gc.open_by_key(sheet_id)
    for name in REALTIME_SHEET_NAMES:
        try:
            return sh.worksheet(name)
        except Exception:
            pass
    return sh.get_worksheet(0)


def _rt_norm_header(v):
    return ' '.join(str(v or '').split()).strip()


def _rt_get(row, idx):
    if idx is None or idx >= len(row):
        return ''
    return str(row[idx]).strip()


def _rt_parse_plan_date(v):
    dt = parse_dt(v)
    if dt:
        return to_by_date(dt)
    s = str(v or '').strip()
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
    if m:
        d, mo, y = map(int, m.groups())
        if y > 2100:
            y -= 543
        try:
            return to_by_date(datetime(y, mo, d))
        except Exception:
            return s
    return s


def _rt_stage_flags(status_value, dt_travel=None, dt_start=None, dt_hold=None, dt_linkup=None):
    status = str(status_value or '').strip()
    status_l = status.lower()
    is_travel = (dt_travel is not None) or ('เดินทาง' in status) or ('travel' in status_l)
    is_start = (dt_start is not None) or ('เริ่มซ่อม' in status) or ('start' in status_l) or ('repair' in status_l)
    is_done = (dt_linkup is not None) or (dt_hold is not None) or ('link up' in status_l) or ('แล้วเสร็จ' in status) or ('เสร็จ' in status) or ('hold' in status_l)
    is_active = is_real_active_team_row(status, dt_travel, dt_start)
    return is_travel, is_start, is_done, is_active


def build_realtime_monitoring():
    gc = get_client()
    regions = {'NOR1': {'by_date': {}}, 'NOR2': {'by_date': {}}}
    all_dates = set()

    for region_hint, sheet_id in REALTIME_SHEET_IDS.items():
        ws = _rt_open_worksheet(gc, sheet_id)
        rows = ws.get_all_values()
        if not rows:
            continue
        headers = [_rt_norm_header(h) for h in rows[0]]
        col = {h: i for i, h in enumerate(headers) if h}

        def fc(*names):
            low = {k.lower(): v for k, v in col.items()}
            for n in names:
                if n in col:
                    return col[n]
                if n.lower() in low:
                    return low[n.lower()]
            return None

        C = {
            'ticket': fc('Ticket'),
            'sla': fc('SLA'),
            'subject': fc('Subject'),
            'site': fc('Site'),
            'region': fc('Region'),
            'province': fc('Province'),
            'type_team': fc('Type Team'),
            'team_id': fc('Team ID'),
            'que': fc('Que'),
            'plan': fc('Plan', 'PLAN'),
            'travel': fc('เวลาเดินทาง'),
            'start': fc('เวลาเริ่มซ่อม'),
            'hold': fc('Hold'),
            'linkup': fc('Link Up'),
            'status': fc('Status Team'),
            'verify': fc('Team Verify'),
            'ticket_refer': fc('จำนวน Ticket Refer'),
        }

        for row_idx, row in enumerate(rows[1:], start=2):
            plan_date = _rt_parse_plan_date(_rt_get(row, C['plan']))
            if not plan_date:
                continue
            region = (_rt_get(row, C['region']) or region_hint).strip().upper()
            if region not in ('NOR1', 'NOR2'):
                region = region_hint
            all_dates.add(plan_date)

            team_id = _rt_get(row, C['team_id']) or f'NO_TEAM_{row_idx}'
            ticket_raw = _rt_get(row, C['ticket'])
            ticket_key = dedupe_ticket_key(ticket_raw, row_idx)
            dedupe_key = f'{team_id}::{ticket_key}'
            province = _rt_get(row, C['province'])
            type_team = _rt_get(row, C['type_team'])
            que = _rt_get(row, C['que'])
            status_val = _rt_get(row, C['status'])
            dt_travel = parse_dt(_rt_get(row, C['travel']))
            dt_start = parse_dt(_rt_get(row, C['start']))
            dt_hold = parse_dt(_rt_get(row, C['hold']))
            dt_linkup = parse_dt(_rt_get(row, C['linkup']))
            is_travel, is_start, is_done, is_active = _rt_stage_flags(status_val, dt_travel, dt_start, dt_hold, dt_linkup)

            bucket = regions.setdefault(region, {'by_date': {}})['by_date'].setdefault(plan_date, {
                'planned_tickets': set(),
                'planned_teams': set(),
                'active_teams': set(),
                'travel_teams': set(),
                'travel_tickets': set(),
                'start_teams': set(),
                'start_tickets': set(),
                'done_teams': set(),
                'done_tickets': set(),
                'team_map': {}
            })
            bucket['planned_tickets'].add(dedupe_key)
            bucket['planned_teams'].add(team_id)
            if is_active:
                bucket['active_teams'].add(team_id)
            if is_travel:
                bucket['travel_teams'].add(team_id)
                bucket['travel_tickets'].add(dedupe_key)
            if is_start:
                bucket['start_teams'].add(team_id)
                bucket['start_tickets'].add(dedupe_key)
            if is_done:
                bucket['done_teams'].add(team_id)
                bucket['done_tickets'].add(dedupe_key)

            tb = bucket['team_map'].setdefault(team_id, {
                'team_id': team_id,
                'region': region,
                'province': province,
                'type_team': type_team,
                'planned_tickets': set(),
                'travel_tickets': set(),
                'start_tickets': set(),
                'done_tickets': set(),
                'que_set': set(),
                'latest_status': '',
            })
            tb['planned_tickets'].add(dedupe_key)
            if que:
                tb['que_set'].add(que)
            if status_val:
                tb['latest_status'] = status_val
            if is_travel:
                tb['travel_tickets'].add(dedupe_key)
            if is_start:
                tb['start_tickets'].add(dedupe_key)
            if is_done:
                tb['done_tickets'].add(dedupe_key)

    out = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'dates': sorted(all_dates),
        'default_date': '',
        'regions': {}
    }
    today_be = to_by_date(datetime.now())
    out['default_date'] = today_be if today_be in all_dates else (max(all_dates) if all_dates else '')

    for region in ('NOR1', 'NOR2'):
        reg_out = {'by_date': {}}
        for date_key in sorted(regions.get(region, {}).get('by_date', {}).keys()):
            b = regions[region]['by_date'][date_key]
            teams = []
            planned_not_departed = []
            travel_not_started = []
            for team_id, tb in b['team_map'].items():
                stage = 'planned'
                if tb['travel_tickets']:
                    stage = 'travel'
                if tb['start_tickets']:
                    stage = 'start'
                if tb['done_tickets']:
                    stage = 'done'
                row = {
                    'team_id': team_id,
                    'region': region,
                    'province': tb['province'],
                    'type_team': tb['type_team'],
                    'planned_tickets': len(tb['planned_tickets']),
                    'travel_tickets': len(tb['travel_tickets']),
                    'start_tickets': len(tb['start_tickets']),
                    'done_tickets': len(tb['done_tickets']),
                    'que_count': len(tb['que_set']),
                    'latest_status': tb['latest_status'],
                    'stage': stage,
                }
                teams.append(row)
                if stage == 'planned':
                    planned_not_departed.append(row)
                elif stage == 'travel':
                    travel_not_started.append(row)
            teams.sort(key=lambda x: (-x['done_tickets'], -x['start_tickets'], -x['travel_tickets'], -x['planned_tickets'], x['team_id']))
            planned_not_departed.sort(key=lambda x: (-x['planned_tickets'], x['team_id']))
            travel_not_started.sort(key=lambda x: (-x['travel_tickets'], -x['planned_tickets'], x['team_id']))
            planned_teams_n = len(b['planned_teams'])
            planned_not_departed_n = len(planned_not_departed)
            travel_not_started_n = len(travel_not_started)
            plan_stall_pct = round((planned_not_departed_n / planned_teams_n) * 100, 1) if planned_teams_n else 0.0
            travel_stall_pct = round((travel_not_started_n / planned_teams_n) * 100, 1) if planned_teams_n else 0.0
            insight = []
            if planned_not_departed_n:
                insight.append(f"ยังไม่ออกเดินทาง {planned_not_departed_n} ทีม ({plan_stall_pct:.0f}% ของทีมตามแผน)")
            if travel_not_started_n:
                insight.append(f"เดินทางแล้วแต่ยังไม่เริ่มซ่อม {travel_not_started_n} ทีม ({travel_stall_pct:.0f}% ของทีมตามแผน)")
            if not insight:
                insight.append('ทุกทีมเริ่มขยับงานตามแผนแล้ว')
            reg_out['by_date'][date_key] = {
                'summary': {
                    'planned_tickets': len(b['planned_tickets']),
                    'planned_teams': planned_teams_n,
                    'active_teams': len(b['active_teams']),
                    'travel_teams': len(b['travel_teams']),
                    'travel_tickets': len(b['travel_tickets']),
                    'start_teams': len(b['start_teams']),
                    'start_tickets': len(b['start_tickets']),
                    'done_teams': len(b['done_teams']),
                    'done_tickets': len(b['done_tickets']),
                    'planned_not_departed_teams': planned_not_departed_n,
                    'travel_not_started_teams': travel_not_started_n,
                    'plan_stall_pct': plan_stall_pct,
                    'travel_stall_pct': travel_stall_pct,
                },
                'alerts': {
                    'planned_not_departed': planned_not_departed,
                    'travel_not_started': travel_not_started,
                    'insight': insight,
                },
                'teams': teams
            }
        out['regions'][region] = reg_out
    return out


@app.route('/api/realtime')
def api_realtime():
    try:
        return jsonify(build_realtime_monitoring())
    except Exception as e:
        log.exception('api_realtime error')
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard')
def api_dashboard():
    with _cache_lock:
        if _cache is None:
            return jsonify({'error':'Cache building, retry in 60s'}), 503
        return jsonify(_cache)

@app.route('/api/status')
def api_status():
    with _cache_lock:
        if _cache is None: return jsonify({'status':'building'})
        return jsonify(dict(
            status='ready',
            teams=len(_cache.get('ts',[])),
            months=_cache.get('months',[]),
            tickets=_cache.get('gstats',{}).get('total_tkt',0),
            home_coords=len(_cache.get('homeCoords',{})),
            cached_at=_cache.get('cached_at')
        ))

@app.route('/api/rebuild', methods=['POST'])
def api_rebuild():
    threading.Thread(target=rebuild_cache, daemon=True).start()
    return jsonify({'status':'rebuilding'})

@app.route('/')
@app.route('/dashboard')
@app.route('/dashboard.html')
def index():
    template_file = os.path.join(TEMPLATE_DIR, 'dashboard.html')
    local_file = os.path.join(BASE_DIR, 'dashboard.html')

    if os.path.exists(template_file):
        return render_template('dashboard.html')
    if os.path.exists(local_file):
        return send_from_directory(BASE_DIR, 'dashboard.html')

    return '<h3>NOC Dashboard API</h3><p>ไม่พบไฟล์ dashboard.html</p><p>วางไฟล์ไว้ที่ <code>templates/dashboard.html</code> หรือข้างๆ <code>app.py</code></p><p><a href="/api/status">/api/status</a></p>', 404


@app.after_request
def add_no_cache_headers(resp):
    if resp.content_type and ('text/html' in resp.content_type or 'application/json' in resp.content_type):
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
    return resp

def start():
    threading.Thread(target=rebuild_cache, daemon=True).start()
    s = BackgroundScheduler()
    s.add_job(rebuild_cache, 'interval', hours=REBUILD_HOURS)
    s.start()

start()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',5000)))
