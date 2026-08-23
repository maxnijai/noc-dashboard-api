import os, json, logging, threading, time, re, math, secrets
from datetime import datetime, timedelta, date
from flask import Flask, jsonify, render_template, send_from_directory, request, session, redirect, url_for, Response
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler

from pending_trend import get_drive_and_sheets_clients, bangkok_now
from realtime_monitor import (
    build_realtime_response,
    get_insert_time,
    BOOKMARK_VIEWS,
)
from pending_ticket import (
    build_pending_ticket_response,
    build_exclusive_pending_response,
    save_work_log_entry,
    rename_group_problem_value,
    build_p0_snapshot_comparison,
    build_pending_ticket_xlsx,
)
import oncall
import oncall_escalation
import flood_nan
import summary_nan
import auth

SHEET_ID      = '1_l5UAj1etjGgLCR4DSG6qDoK8c1unFnO6NVHVwvmbAU'
SHEET_NAME    = 'Sheet1'
CM_BASE       = 3
OFC_BASE      = 2
REBUILD_HOURS = 6
EXCLUDE       = {'PS_CMI_ofc_011','PS_CMI_ofc_012'}
VALID_YEAR    = '2569'   # กรองเฉพาะปี พ.ศ. นี้
FIREBURN_SHEET_ID = '1UV54tO8-COcW6GN3oSfbm1POu0V09UrkAdcs9lp1Dqw'
FIREBURN_SHEET_NAME = 'Data'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
app = Flask(__name__, template_folder=TEMPLATE_DIR)
app.secret_key = os.environ.get('FLASK_SECRET_KEY') or secrets.token_hex(32)
CORS(app)

# ---------------------------------------------------------------------------
# Presence: lightweight in-memory "who's online" tracking, updated on every
# authenticated request (no Google Sheets writes - keeps quota untouched).
# Lives only in process memory, so it resets on redeploy and (per the
# single-worker Procfile note elsewhere) is consistent across every request
# since there's only one process. "Online" = seen within the last 5 minutes.
# ---------------------------------------------------------------------------
ONLINE_WINDOW_SECONDS = 5 * 60
_online_users = {}  # email -> {"name": ..., "last_seen": epoch_seconds}
_online_lock = threading.Lock()

def _mark_online(email, name):
    if not email:
        return
    with _online_lock:
        _online_users[email] = {"name": name or email, "last_seen": time.time()}

def _get_online_users():
    now = time.time()
    with _online_lock:
        active = [
            {"email": email, "name": info["name"], "last_seen": info["last_seen"]}
            for email, info in _online_users.items()
            if now - info["last_seen"] <= ONLINE_WINDOW_SECONDS
        ]
    active.sort(key=lambda u: -u["last_seen"])
    return active

# ---------------------------------------------------------------------------
# Auth: session-based login required for the entire site. A handful of paths
# are exempt (the login/change-password pages themselves, static assets, and
# the one-time bulk user-seed endpoint used to bootstrap accounts).
# ---------------------------------------------------------------------------
AUTH_EXEMPT_PATHS = {'/login', '/change-password', '/api/auth/seed-users', '/api/auth/reset-passwords', '/api/oncall/seed', '/api/oncall-schedule/add-district-column', '/api/oncall-escalation/seed', '/favicon.ico'}
AUTH_EXEMPT_PREFIXES = ('/static/',)

@app.before_request
def require_login():
    if request.path in AUTH_EXEMPT_PATHS or request.path.startswith(AUTH_EXEMPT_PREFIXES):
        return
    if not session.get('user_email'):
        if request.path.startswith('/api/'):
            return jsonify({'error': 'login required'}), 401
        return redirect(url_for('login_page', next=request.path))
    if session.get('must_change_password') and request.path != '/logout':
        return redirect(url_for('change_password_page'))
    _mark_online(session.get('user_email'), session.get('user_name'))

@app.route('/api/online-users')
def api_online_users():
    return jsonify({'users': _get_online_users()})

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'GET':
        return render_template('login.html', error=None)
    email = request.form.get('email', '')
    password = request.form.get('password', '')
    try:
        _, gs_client = get_drive_and_sheets_clients()
        user = auth.verify_login(gs_client, email, password)
    except Exception:
        log.exception("login check failed")
        return render_template('login.html', error='ระบบขัดข้อง กรุณาลองใหม่'), 500
    if not user:
        return render_template('login.html', error='อีเมลหรือรหัสผ่านไม่ถูกต้อง')
    session['user_email'] = auth.normalize_email(user['email'])
    session['user_name'] = user['name']
    session['must_change_password'] = user['must_change_password']
    session.permanent = True
    if user['must_change_password']:
        return redirect(url_for('change_password_page'))
    return redirect(request.args.get('next') or url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))

@app.route('/change-password', methods=['GET', 'POST'])
def change_password_page():
    if not session.get('user_email'):
        return redirect(url_for('login_page'))
    if request.method == 'GET':
        return render_template('change_password.html', name=session.get('user_name', ''),
                                must_change=session.get('must_change_password', False), error=None)
    current_pw = request.form.get('current_password', '')
    new_pw = request.form.get('new_password', '')
    confirm_pw = request.form.get('confirm_password', '')
    try:
        _, gs_client = get_drive_and_sheets_clients()
        user = auth.verify_login(gs_client, session['user_email'], current_pw)
        if not user:
            return render_template('change_password.html', name=session.get('user_name', ''),
                                    must_change=session.get('must_change_password', False),
                                    error='รหัสผ่านปัจจุบันไม่ถูกต้อง')
        if new_pw != confirm_pw:
            return render_template('change_password.html', name=session.get('user_name', ''),
                                    must_change=session.get('must_change_password', False),
                                    error='รหัสผ่านใหม่ไม่ตรงกัน')
        if len(new_pw) < 4:
            return render_template('change_password.html', name=session.get('user_name', ''),
                                    must_change=session.get('must_change_password', False),
                                    error='รหัสผ่านสั้นเกินไป (อย่างน้อย 4 ตัวอักษร)')
        auth.set_password(gs_client, session['user_email'], new_pw)
        session['must_change_password'] = False
        return redirect(url_for('index'))
    except Exception:
        log.exception("change password failed")
        return render_template('change_password.html', name=session.get('user_name', ''),
                                must_change=session.get('must_change_password', False),
                                error='ระบบขัดข้อง กรุณาลองใหม่'), 500

@app.route('/api/auth/seed-users', methods=['POST'])
def api_seed_users():
    """One-time bootstrap: POST a JSON array of {email, phone, name, company,
    department, password} to create accounts. If password is omitted,
    default password = last 4 phone digits; forced change on first login
    either way. Skips any email that already has an account, so it's safe
    to call more than once (e.g. to add new hires later). Not committed
    with any real data - the caller supplies it.
    Requires ?token=<SEED_USERS_TOKEN env var> since this endpoint has to be
    exempt from login (nobody can log in before the first account exists)."""
    expected_token = os.environ.get('SEED_USERS_TOKEN')
    if not expected_token or request.args.get('token') != expected_token:
        return jsonify({'error': 'missing or invalid token'}), 403
    payload = request.get_json(silent=True)
    if not isinstance(payload, list):
        return jsonify({'error': 'expected a JSON array of {email, phone, name, company, department, password}'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        created, skipped = auth.batch_seed_users(gs_client, payload)
        return jsonify({'created': created, 'skipped_existing': skipped})
    except Exception as e:
        log.exception("seed-users failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/auth/reset-passwords', methods=['POST'])
def api_reset_passwords():
    """One-time-use admin fixup: POST a JSON array of {email, password} to
    force-set that account's password (and re-flag must_change_password),
    regardless of whatever it currently is. Same token gate and login-exempt
    reasoning as seed-users. Unknown emails are silently skipped."""
    expected_token = os.environ.get('SEED_USERS_TOKEN')
    if not expected_token or request.args.get('token') != expected_token:
        return jsonify({'error': 'missing or invalid token'}), 403
    payload = request.get_json(silent=True)
    if not isinstance(payload, list):
        return jsonify({'error': 'expected a JSON array of {email, password}'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        updated = auth.batch_reset_passwords(gs_client, payload)
        return jsonify({'updated': updated})
    except Exception as e:
        log.exception("reset-passwords failed")
        return jsonify({'error': str(e)}), 500

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

def parse_coord(v, row_idx=None, debug_list=None):
    if not v:
        return None
    raw = str(v).strip()
    if not raw or raw.lower() == 'nan':
        return None

    cleaned = raw.replace('\u200b', '').replace(' ', '')
    cleaned = cleaned.replace(';', ',').replace('|', ',')
    cleaned = re.sub(r'\.{2,}', '.', cleaned)
    cleaned = re.sub(r',+', ',', cleaned)
    cleaned = cleaned.strip(', ')

    m = re.match(r'^(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)$', cleaned)
    if not m:
        if debug_list is not None:
            debug_list.append({'row': row_idx, 'raw': raw, 'cleaned': cleaned, 'type': 'format_error'})
        return None
    try:
        lat = float(m.group(1))
        lon = float(m.group(2))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            if debug_list is not None:
                debug_list.append({'row': row_idx, 'raw': raw, 'cleaned': cleaned, 'type': 'out_of_range'})
            return None
        return (lat, lon)
    except Exception as e:
        if debug_list is not None:
            debug_list.append({'row': row_idx, 'raw': raw, 'cleaned': cleaned, 'type': 'convert_error', 'error': str(e)})
        return None

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


def productivity_event_key(team_id, dt_travel=None, dt_date=None, row_index=None):
    """ทีมเดียวกัน + เวลาเดินทางเดียวกัน + วันเดียวกัน = นับ Productivity 1 งาน"""
    if not team_id or dt_travel is None:
        return f'NOPROD_{row_index if row_index is not None else "X"}'
    day_key = to_by_date(dt_date or dt_travel)
    travel_key = dt_travel.strftime('%H:%M')
    return f'{team_id}|{day_key}|{travel_key}'

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
        has_prod = (dt_travel is not None) and ((dt_start is not None) or (dt_linkup is not None))

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
        prod_event_key = productivity_event_key(team_id, dt_travel, dt_linkup or dt_travel or dt_start or dt_hold, row_idx)
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
                    'prod_p1_keys': set(), 'prod_p2_keys': set(),
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
                if has_prod and prod_event_key not in sb['prod_p1_keys']:
                    sb['prod_p1_keys'].add(prod_event_key)
                    sb['done_p1'] += 1
                if has_prod and prod_event_key not in sb['prod_p2_keys']:
                    sb['prod_p2_keys'].add(prod_event_key)
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
            if has_prod:
                tm['pdt1_keys'].setdefault(date_str, set()).add(prod_event_key)
                tm['pdt1_dates'][date_str] = len(tm['pdt1_keys'][date_str])
            if has_prod:
                tm['pdt2_keys'].setdefault(date_str, set()).add(prod_event_key)
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
                if has_prod:
                    mm['p1keys'].setdefault(date_str, set()).add(prod_event_key)
                    mm['p1d'][date_str] = len(mm['p1keys'][date_str])
                if has_prod:
                    mm['p2keys'].setdefault(date_str, set()).add(prod_event_key)
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
            if has_prod:
                plan_daily[day_key][team_id]['p1keys'].add(prod_event_key)
                plan_daily[day_key][team_id]['p1'] = len(plan_daily[day_key][team_id]['p1keys'])
            if has_prod:
                plan_daily[day_key][team_id]['p2keys'].add(prod_event_key)
                plan_daily[day_key][team_id]['p2'] = len(plan_daily[day_key][team_id]['p2keys'])

            if team_id not in team_plan_daily:
                team_plan_daily[team_id] = {}
            if plan_date not in team_plan_daily[team_id]:
                team_plan_daily[team_id][plan_date] = {'p1': 0, 'p2': 0, 'p1keys': set(), 'p2keys': set(), 'm': plan_month, 'reg': reg, 'type': type_team}
            if has_prod:
                team_plan_daily[team_id][plan_date]['p1keys'].add(prod_event_key)
                team_plan_daily[team_id][plan_date]['p1'] = len(team_plan_daily[team_id][plan_date]['p1keys'])
            if has_prod:
                team_plan_daily[team_id][plan_date]['p2keys'].add(prod_event_key)
                team_plan_daily[team_id][plan_date]['p2'] = len(team_plan_daily[team_id][plan_date]['p2keys'])

            # รายสัปดาห์: ISO week ตามปฏิทินสากล
            wk = week_bucket_label(dt_plan)
            wk_key  = f"{wk['sort']}||{wk['label']}||{plan_month}||{reg}||{type_team}"
            if wk_key not in plan_weekly:
                plan_weekly[wk_key] = {'meta': wk, 'teams': {}}
            if team_id not in plan_weekly[wk_key]['teams']:
                plan_weekly[wk_key]['teams'][team_id] = {'p1': 0, 'p2': 0, 'p1keys': set(), 'p2keys': set(), 'dates': set()}
            plan_weekly[wk_key]['teams'][team_id]['dates'].add(plan_date)
            if has_prod:
                plan_weekly[wk_key]['teams'][team_id]['p1keys'].add(prod_event_key)
                plan_weekly[wk_key]['teams'][team_id]['p1'] = len(plan_weekly[wk_key]['teams'][team_id]['p1keys'])
            if has_prod:
                plan_weekly[wk_key]['teams'][team_id]['p2keys'].add(prod_event_key)
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
            if has_prod:
                daily_team_stats[plan_date][team_id]['p1keys'].add(prod_event_key)
                daily_team_stats[plan_date][team_id]['p1'] = len(daily_team_stats[plan_date][team_id]['p1keys'])
            if has_prod:
                daily_team_stats[plan_date][team_id]['p2keys'].add(prod_event_key)
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
            if has_prod and ticket_key not in drill_seen_keys[team_id][drill_date_str]['p1']:
                drill_seen_keys[team_id][drill_date_str]['p1'].add(prod_event_key)
                row_p1 = 1
            if has_prod and ticket_key not in drill_seen_keys[team_id][drill_date_str]['p2']:
                drill_seen_keys[team_id][drill_date_str]['p2'].add(prod_event_key)
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

FOCUS_SOURCE_SHEET_ID = '1AEQSsiLUbr5p6HYh36WNGF9TkUDVeW2xN-vDvDkjy1k'
FOCUS_SOURCE_BOOKMARKS = {
    '3. All NW Incident NSA1-2',
    '4.FBB with SA1-4',
    '7.MB with SA1-4',
}
FOCUS_PLAN_SHEET_IDS = {
    'NOR1': '1t8DErfQLBRXkoaorDFMdAxtLZf-RA_hzQ-xFQI6EgBo',
    'NOR2': '1q5xC5lQv2-FhjM-h_o4xqw1dpmei8oB5xo7lSyQm8us',
}


def _fp_norm_header(v):
    return ' '.join(str(v or '').replace('\n', ' ').split()).strip()


def _fp_fc(col, *names):
    low = {k.lower(): v for k, v in col.items()}
    for n in names:
        if n in col:
            return col[n]
        if n.lower() in low:
            return low[n.lower()]
    return None


def _fp_get(row, idx):
    if idx is None or idx >= len(row):
        return ''
    return str(row[idx]).strip()


def _fp_parse_num(v):
    s = str(v or '').replace(',', '').strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _fp_fmt_dt(dt):
    if not dt:
        return ''
    return f"{dt.day}/{dt.month}/{dt.year + 543} {dt:%H:%M}"


def _fp_time_only(dt):
    if not dt:
        return ''
    return dt.strftime('%H:%M')


def _fp_best_plan_row(rows):
    def score(r):
        last_candidates = [r.get('dt_log'), r.get('dt_done'), r.get('dt_hold'), r.get('dt_start'), r.get('dt_go')]
        last_ts = max([d for d in last_candidates if d is not None], default=datetime.min)
        filled = sum(1 for k in ('team_id', 'que', 'go', 'start', 'done', 'hold', 'status_sccd', 'log_update') if r.get(k))
        return (last_ts, filled)
    return max(rows, key=score) if rows else None


def _fp_load_plan_sheet(gc, region):
    ws = _rt_open_worksheet(gc, FOCUS_PLAN_SHEET_IDS[region])
    rows = ws.get_all_values()
    out = {}
    if not rows:
        return out
    headers = [_fp_norm_header(h) for h in rows[0]]
    col = {h: i for i, h in enumerate(headers) if h}
    C = {
        'ticket': _fp_fc(col, 'TICKETID', 'Ticket', 'Ticket ID'),
        'ciname': _fp_fc(col, 'CINAME', 'CI Name', 'CI NAME'),
        'subject': _fp_fc(col, 'SUBJECT', 'Subject'),
        'que': _fp_fc(col, 'Que', 'QUE'),
        'team_id': _fp_fc(col, 'Team ID', 'TEAM ID'),
        'go': _fp_fc(col, 'Go', 'เวลาเดินทาง'),
        'start': _fp_fc(col, 'Start', 'เวลาเริ่มซ่อม'),
        'done': _fp_fc(col, 'Done', 'Link Up'),
        'hold': _fp_fc(col, 'Hold'),
        'status_sccd': _fp_fc(col, 'Status SCCD', 'STATUS SCCD'),
        'log_update': _fp_fc(col, 'Log Update', 'Update Log', 'LOG UPDATE'),
        'inoc_name': _fp_fc(col, 'INOC Name', 'INOCNAME'),
        'priority_src': _fp_fc(col, 'Priority'),
        'trueownergroup': _fp_fc(col, 'TRUEOWNERGROUP', 'TrueOwnerGroup', 'TRUE OWNER GROUP'),
    }
    for idx, row in enumerate(rows[1:], start=2):
        ticket = _fp_get(row, C['ticket'])
        if not ticket:
            continue
        rec = {
            'ticketid': ticket,
            'ciname': _fp_get(row, C['ciname']),
            'subject': _fp_get(row, C['subject']),
            'que': _fp_get(row, C['que']),
            'team_id': _fp_get(row, C['team_id']),
            'go': _fp_get(row, C['go']),
            'start': _fp_get(row, C['start']),
            'done': _fp_get(row, C['done']),
            'hold': _fp_get(row, C['hold']),
            'status_sccd': _fp_get(row, C['status_sccd']),
            'log_update': _fp_get(row, C['log_update']),
            'inoc_name': _fp_get(row, C['inoc_name']),
            'priority_src': _fp_get(row, C['priority_src']),
            'trueownergroup': _fp_get(row, C['trueownergroup']),
            'region': region,
            'dt_go': parse_dt(_fp_get(row, C['go'])),
            'dt_start': parse_dt(_fp_get(row, C['start'])),
            'dt_done': parse_dt(_fp_get(row, C['done'])),
            'dt_hold': parse_dt(_fp_get(row, C['hold'])),
            'dt_log': parse_dt(_fp_get(row, C['log_update'])),
            '_row_idx': idx,
        }
        out.setdefault(ticket, []).append(rec)
    return {k: _fp_best_plan_row(v) for k, v in out.items()}


def _fp_extract_region(trueownergroup_value, fallback=''):
    s = str(trueownergroup_value or '').strip().upper()
    m = re.search(r'-(NOR[12])-', s)
    if m:
        return m.group(1)
    s2 = str(fallback or '').strip().upper()
    return s2 if s2 in ('NOR1', 'NOR2') else ''


def _fp_trueowner_sort_key(v):
    s = str(v or '').strip().upper()
    return s or 'ZZZ'


def build_focus_priority():
    gc = get_client()
    source_ws = gc.open_by_key(FOCUS_SOURCE_SHEET_ID).get_worksheet(0)
    source_rows = source_ws.get_all_values()
    if not source_rows:
        return {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'bookmarks': {}, 'bookmark_order': list(FOCUS_SOURCE_BOOKMARKS)}

    headers = [_fp_norm_header(h) for h in source_rows[0]]
    col = {h: i for i, h in enumerate(headers) if h}
    C = {
        'region': _fp_fc(col, 'Region', 'REGION'),
        'bookmark': _fp_fc(col, 'Bookmark', 'BOOKMARK'),
        'ticket': _fp_fc(col, 'TICKETID', 'Ticket', 'Ticket ID'),
        'targetfinish': _fp_fc(col, 'TARGETFINISH', 'Target Finish', 'Target date'),
        'subject': _fp_fc(col, 'SUBJECT', 'Subject'),
        'ciname': _fp_fc(col, 'CINAME', 'CI Name', 'CI NAME'),
        'status_sccd': _fp_fc(col, 'Status SCCD', 'STATUS SCCD'),
        'penalty': _fp_fc(col, 'ยอดค่าปรับ ณ เวลานี้', 'PENALTYBAHT_TRACKB', 'Penalty'),
        'inoc_name': _fp_fc(col, 'INOC Name', 'INOCNAME'),
        'priority_src': _fp_fc(col, 'Priority'),
        'trueownergroup': _fp_fc(col, 'TRUEOWNERGROUP', 'TrueOwnerGroup', 'TRUE OWNER GROUP'),
        'latitude': _fp_fc(col, 'LATITUDE', 'Latitude', 'LAT'),
        'longitude': _fp_fc(col, 'LONGITUDE', 'Longitude', 'LON', 'LONG'),
    }

    plan_maps = {r: _fp_load_plan_sheet(gc, r) for r in ('NOR1', 'NOR2')}
    now_dt = datetime.now()
    ref_dt = (now_dt + timedelta(days=1)).replace(hour=1, minute=15, second=0, microsecond=0)
    bookmark_order = ['3. All NW Incident NSA1-2', '4.FBB with SA1-4', '7.MB with SA1-4']
    out = {
        'generated_at': now_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'reference_cutoff': _fp_fmt_dt(ref_dt),
        'now': _fp_fmt_dt(now_dt),
        'bookmark_order': bookmark_order,
        'bookmarks': {},
        'summary': {'total': 0, 'planned': 0, 'unplanned': 0, 'Priority0': 0, 'Priority1': 0, 'Priority2': 0},
    }
    for bm in bookmark_order:
        out['bookmarks'][bm] = {'summary': {'total': 0, 'planned': 0, 'unplanned': 0, 'Priority0': 0, 'Priority1': 0, 'Priority2': 0}, 'priorities': {'Priority0': [], 'Priority1': [], 'Priority2': []}}

    for idx, row in enumerate(source_rows[1:], start=2):
        bookmark = _fp_get(row, C['bookmark'])
        if bookmark not in FOCUS_SOURCE_BOOKMARKS:
            continue
        ticket = _fp_get(row, C['ticket'])
        if not ticket:
            continue
        targetfinish_raw = _fp_get(row, C['targetfinish'])
        dt_target = parse_dt(targetfinish_raw)
        if not dt_target:
            continue

        trueownergroup = _fp_get(row, C['trueownergroup'])
        region = _fp_extract_region(trueownergroup, _fp_get(row, C['region']))
        if region not in ('NOR1', 'NOR2'):
            continue
        plan = None
        if region in ('NOR1', 'NOR2'):
            plan = plan_maps.get(region, {}).get(ticket)
        if plan is None:
            plan = plan_maps.get('NOR1', {}).get(ticket) or plan_maps.get('NOR2', {}).get(ticket)

        diff_hours = round((ref_dt - dt_target).total_seconds() / 3600.0, 2)
        if diff_hours > 24:
            pr = 'Priority0'
        elif diff_hours >= 0:
            pr = 'Priority1'
        else:
            pr = 'Priority2'

        lat_raw = _fp_get(row, C['latitude'])
        lon_raw = _fp_get(row, C['longitude'])
        try:
            lat_val = float(str(lat_raw).replace(',', '').strip()) if str(lat_raw).strip() else None
        except Exception:
            lat_val = None
        try:
            lon_val = float(str(lon_raw).replace(',', '').strip()) if str(lon_raw).strip() else None
        except Exception:
            lon_val = None
        if lat_val is not None and not (-90 <= lat_val <= 90):
            lat_val = None
        if lon_val is not None and not (-180 <= lon_val <= 180):
            lon_val = None

        plan_found = plan is not None
        rec = {
            'row_no': idx,
            'bookmark': bookmark,
            'region': region,
            'trueownergroup': trueownergroup or (plan.get('trueownergroup') if plan else ''),
            'priority_bucket': pr,
            'priority_src': _fp_get(row, C['priority_src']) or (plan.get('priority_src') if plan else ''),
            'ciname': _fp_get(row, C['ciname']) or (plan.get('ciname') if plan else ''),
            'subject': _fp_get(row, C['subject']) or (plan.get('subject') if plan else ''),
            'ticketid': ticket,
            'targetfinish': _fp_fmt_dt(dt_target),
            'hours_vs_cutoff': diff_hours,
            'summary_hours': diff_hours,
            'plan_found': plan_found,
            'latitude': lat_val,
            'longitude': lon_val,
            'que': plan.get('que', '') if plan else '',
            'team_id': plan.get('team_id', '') if plan else '',
            'go': plan.get('go', '') if plan else '',
            'start': plan.get('start', '') if plan else '',
            'done': plan.get('done', '') if plan else '',
            'hold': plan.get('hold', '') if plan else '',
            'status_sccd': _fp_get(row, C['status_sccd']) or (plan.get('status_sccd') if plan else ''),
            'penalty': _fp_get(row, C['penalty']),
            'penalty_num': _fp_parse_num(_fp_get(row, C['penalty'])),
            'inoc_name': _fp_get(row, C['inoc_name']) or (plan.get('inoc_name') if plan else ''),
            'log_update': plan.get('log_update', '') if plan else '',
            'go_time': _fp_time_only(plan.get('dt_go')) if plan else '',
            'start_time': _fp_time_only(plan.get('dt_start')) if plan else '',
            'done_time': _fp_time_only(plan.get('dt_done')) if plan else '',
            'hold_time': _fp_time_only(plan.get('dt_hold')) if plan else '',
        }
        out['bookmarks'][bookmark]['priorities'][pr].append(rec)

    def _sort_rows(rows):
        rows.sort(key=lambda r: (
            _fp_trueowner_sort_key(r.get('trueownergroup')),
            0 if r.get('plan_found') else 1,
            -(r.get('penalty_num') or 0),
            r.get('ticketid') or ''
        ))

    for bm in bookmark_order:
        payload = out['bookmarks'][bm]
        rows_all = []
        for pr in ('Priority0', 'Priority1', 'Priority2'):
            _sort_rows(payload['priorities'][pr])
            rows_all.extend(payload['priorities'][pr])
        payload['summary'] = {
            'total': len(rows_all),
            'planned': sum(1 for r in rows_all if r['plan_found']),
            'unplanned': sum(1 for r in rows_all if not r['plan_found']),
            'Priority0': len(payload['priorities']['Priority0']),
            'Priority1': len(payload['priorities']['Priority1']),
            'Priority2': len(payload['priorities']['Priority2']),
        }
        for k in ('total','planned','unplanned','Priority0','Priority1','Priority2'):
            out['summary'][k] += payload['summary'][k]
    return out



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
    is_productive_done = (dt_travel is not None) and ((dt_start is not None) or (dt_linkup is not None))
    is_active = is_real_active_team_row(status, dt_travel, dt_start)
    return is_travel, is_start, is_done, is_productive_done, is_active


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
            has_ticket_plan = bool(str(ticket_raw or '').strip())
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
            is_travel, is_start, is_done, is_productive_done, is_active = _rt_stage_flags(status_val, dt_travel, dt_start, dt_hold, dt_linkup)

            bucket = regions.setdefault(region, {'by_date': {}})['by_date'].setdefault(plan_date, {
                'planned_tickets': set(),
                'planned_teams': set(),
                'active_teams': set(),
                'travel_teams': set(),
                'travel_tickets': set(),
                'start_teams': set(),
                'start_tickets': set(),
                'done_teams': set(),
                'done_tickets': set(), 'done_events': set(),
                'off_teams': set(),
                'team_map': {}
            })

            tb = bucket['team_map'].setdefault(team_id, {
                'team_id': team_id,
                'region': region,
                'province': province,
                'type_team': type_team,
                'planned_tickets': set(),
                'travel_tickets': set(),
                'start_tickets': set(),
                'done_tickets': set(), 'done_events': set(),
                'que_set': set(),
                'latest_status': '',
                'has_ticket_plan': False,
                'off_rows': 0,
            })
            if que:
                tb['que_set'].add(que)
            if status_val:
                tb['latest_status'] = status_val

            if not has_ticket_plan:
                tb['off_rows'] += 1
                if not tb['has_ticket_plan']:
                    bucket['off_teams'].add(team_id)
                continue

            tb['has_ticket_plan'] = True
            bucket['off_teams'].discard(team_id)
            bucket['planned_tickets'].add(dedupe_key)
            bucket['planned_teams'].add(team_id)
            tb['planned_tickets'].add(dedupe_key)
            if is_active:
                bucket['active_teams'].add(team_id)
            if is_travel:
                bucket['travel_teams'].add(team_id)
                bucket['travel_tickets'].add(dedupe_key)
                tb['travel_tickets'].add(dedupe_key)
            if is_start:
                bucket['start_teams'].add(team_id)
                bucket['start_tickets'].add(dedupe_key)
                tb['start_tickets'].add(dedupe_key)
            if is_productive_done:
                rt_prod_key = productivity_event_key(team_id, dt_travel, dt_linkup or dt_travel or dt_start or dt_hold, row_idx)
                bucket['done_teams'].add(team_id)
                bucket['done_events'].add(rt_prod_key)
                tb['done_events'].add(rt_prod_key)

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
            off_teams = []
            off_teams_cm = []
            off_teams_ofc = []
            for team_id, tb in b['team_map'].items():
                if not tb.get('has_ticket_plan'):
                    row = {
                        'team_id': team_id,
                        'region': region,
                        'province': tb['province'],
                        'type_team': tb['type_team'],
                        'planned_tickets': 0,
                        'travel_tickets': 0,
                        'start_tickets': 0,
                        'done_tickets': 0,
                        'que_count': len(tb['que_set']),
                        'latest_status': tb['latest_status'] or 'หยุด',
                        'stage': 'off',
                    }
                    teams.append(row)
                    off_teams.append(row)
                    if str(tb['type_team'] or '').strip().upper() == 'CM':
                        off_teams_cm.append(row)
                    elif str(tb['type_team'] or '').strip().upper() == 'OFC':
                        off_teams_ofc.append(row)
                    continue
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
                    'done_tickets': len(tb.get('done_events', set())),
                    'que_count': len(tb['que_set']),
                    'latest_status': tb['latest_status'],
                    'stage': stage,
                }
                teams.append(row)
                if stage == 'planned':
                    planned_not_departed.append(row)
                elif stage == 'travel':
                    travel_not_started.append(row)
            teams.sort(key=lambda x: (0 if x['stage'] != 'off' else 1, -x['done_tickets'], -x['start_tickets'], -x['travel_tickets'], -x['planned_tickets'], x['team_id']))
            planned_not_departed.sort(key=lambda x: (-x['planned_tickets'], x['team_id']))
            travel_not_started.sort(key=lambda x: (-x['travel_tickets'], -x['planned_tickets'], x['team_id']))
            planned_teams_n = len(b['planned_teams'])
            planned_not_departed_n = len(planned_not_departed)
            travel_not_started_n = len(travel_not_started)
            plan_stall_pct = round((planned_not_departed_n / planned_teams_n) * 100, 1) if planned_teams_n else 0.0
            travel_stall_pct = round((travel_not_started_n / planned_teams_n) * 100, 1) if planned_teams_n else 0.0
            insight = []
            if off_teams:
                insight.append(f"ทีมหยุด {len(off_teams)} ทีม (CM {len(off_teams_cm)} · OFC {len(off_teams_ofc)}) ไม่มี Ticket ในแผนของวันนั้น")
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
                    'off_teams': len(off_teams),
                    'off_teams_cm': len(off_teams_cm),
                    'off_teams_ofc': len(off_teams_ofc),
                },
                'alerts': {
                    'planned_not_departed': planned_not_departed,
                    'travel_not_started': travel_not_started,
                    'off_teams': off_teams,
                    'off_teams_cm': off_teams_cm,
                    'off_teams_ofc': off_teams_ofc,
                    'insight': insight,
                },
                'teams': teams
            }
        out['regions'][region] = reg_out
    return out






def _find_col_idx(headers, *names):
    norm_headers = [' '.join(str(h or '').split()).strip().lower() for h in headers]
    for name in names:
        name_n = ' '.join(str(name or '').split()).strip().lower()
        if name_n in norm_headers:
            return norm_headers.index(name_n)
    return None

def _row_get_by_idx(row, idx, default=''):
    if idx is None:
        return default
    if 0 <= idx < len(row):
        return str(row[idx]).strip()
    return default


def _infer_fireburn_region_province(region_value='', province_value='', section_value='', team_value='', subject_value=''):
    # Infer NOR region and province/group robustly from multiple columns.
    region_raw = str(region_value or '').strip()
    province_raw = str(province_value or '').strip()
    section_raw = str(section_value or '').strip()
    team_raw = str(team_value or '').strip()
    subject_raw = str(subject_value or '').strip()

    joined = ' | '.join([region_raw, province_raw, section_raw, team_raw, subject_raw]).upper()

    region_norm = ''
    if 'NOR1' in joined:
        region_norm = 'NOR1'
    elif 'NOR2' in joined:
        region_norm = 'NOR2'

    province_label = province_raw or section_raw or '-'

    province_code = ''
    if province_raw in PROV_MAP:
        province_code = PROV_MAP.get(province_raw, '')
    elif section_raw in PROV_MAP:
        province_code = PROV_MAP.get(section_raw, '')
    else:
        for k, v in PROV_MAP.items():
            if k in joined:
                province_code = v
                if not province_raw:
                    province_label = k
                break

    if not province_code:
        for code in PROV_MAP.values():
            if re.search(rf'(?<![A-Z]){re.escape(code)}(?![A-Z])', joined):
                province_code = code
                break

    if not region_norm and province_code:
        region_norm = 'NOR1' if province_code in NOR1 else 'NOR2'

    is_nor = bool(region_norm in ('NOR1', 'NOR2') or province_code)
    return region_norm or '-', province_label or '-', is_nor, province_code or ''

def _fireburn_debug_payload(updated_at, headers=None, detail_rows=None, points=None, summary_rows=None, weekly_rows=None, provinces=None, debug=None, insight=None, error=None):
    return {
        'updated_at': updated_at,
        'points': points or [],
        'weekly': weekly_rows or [],
        'summary_by_province': summary_rows or [],
        'detail_rows': detail_rows or [],
        'provinces': provinces or [],
        'stats': {
            'total': len(detail_rows or []),
            'with_coords': len(points or []),
            'without_coords': max(len(detail_rows or []) - len(points or []), 0)
        },
        'debug': debug or {'total_rows': len(detail_rows or []), 'valid_coords': len(points or []), 'invalid_coords': 0, 'error_samples': [], 'headers': []},
        'insight': insight or [],
        'error': error
    }


def build_fireburn_2026():
    """Fireburn NOR 2026 from separate GGS source. Robust against dirty coordinates and weak column naming."""
    updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    gc = get_client()
    ws = gc.open_by_key(FIREBURN_SHEET_ID).worksheet(FIREBURN_SHEET_NAME)
    rows = ws.get_all_values()

    if not rows:
        return _fireburn_debug_payload(updated_at, headers=[], detail_rows=[], points=[], summary_rows=[], weekly_rows=[], provinces=[],
            debug={'total_rows': 0, 'valid_coords': 0, 'invalid_coords': 0, 'error_samples': [], 'headers': []},
            insight=['ไม่พบข้อมูลในชีต Data'])

    headers = [str(h).strip() for h in rows[0]]
    idx_ticket = _find_col_idx(headers, 'Ticket ID', 'Ticket', 'TICKETID')
    idx_region = _find_col_idx(headers, 'Region', 'REGION')
    idx_province = _find_col_idx(headers, 'Province', 'PROVINCE', 'Trueownergroup', 'TRUEOWNERGROUP')
    idx_section = _find_col_idx(headers, 'Section', 'SECTION')
    idx_team_id = _find_col_idx(headers, 'Team ID', 'TEAM ID')
    idx_type_ofc = _find_col_idx(headers, 'Type ชนิด OFC', 'Type OFC', 'TYPE ชนิด OFC', 'TYPE OFC', 'Type')
    idx_subject = _find_col_idx(headers, 'Subject', 'SUBJECT')
    idx_subproject = _find_col_idx(headers, 'Subproject', 'Sub Project', 'Sub Project ', 'Bookmark')
    idx_wk_create = _find_col_idx(headers, 'Wk Create', 'WK Create', 'WK CREATE', 'Wk')
    idx_point1 = _find_col_idx(headers, 'จุดซ่อมที่1', 'จุดซ่อมที่ 1', 'จุดซ่อมจุดที่ 1', 'พิกัดจุดซ่อมจุดที่ 1', 'Repair Point 1')

    if idx_wk_create is None:
        idx_wk_create = 4
    if idx_point1 is None:
        idx_point1 = 19

    points = []
    summary = {}
    weekly = {}
    detail_rows = []
    debug_coords = []
    valid_coords = 0
    invalid_coords = 0

    for i, row in enumerate(rows[1:], start=2):
        ticket_id = _row_get_by_idx(row, idx_ticket, f'ROW_{i}')
        region_raw = _row_get_by_idx(row, idx_region)
        province_raw = _row_get_by_idx(row, idx_province)
        section = _row_get_by_idx(row, idx_section)
        team_id = _row_get_by_idx(row, idx_team_id)
        type_ofc = _row_get_by_idx(row, idx_type_ofc)
        subject = _row_get_by_idx(row, idx_subject)
        subproject = _row_get_by_idx(row, idx_subproject)
        wk_create = _row_get_by_idx(row, idx_wk_create)
        point1_raw = _row_get_by_idx(row, idx_point1)

        region_norm, province_label, is_nor, province_code = _infer_fireburn_region_province(
            region_raw, province_raw, section, team_id, subject
        )

        if not is_nor:
            # fallback: keep row anyway if it has coord or week, so page does not go blank
            if not str(point1_raw or '').strip() and not str(wk_create or '').strip():
                continue
            region_norm = region_norm or '-'
            province_label = province_label or province_raw or '-'

        rec = {
            'ticket_id': ticket_id or '-',
            'region': region_norm or '-',
            'province': province_label or '-',
            'province_code': province_code or '-',
            'section': section or '-',
            'team_id': team_id or '-',
            'type_ofc': type_ofc or '-',
            'subject': subject or '-',
            'subproject': subproject or '-',
            'wk_create': wk_create or '-',
            'coord_raw': point1_raw or '-',
            'has_coord': False,
            'row_no': i
        }
        detail_rows.append(rec)

        coord = parse_coord(point1_raw, i, debug_coords)
        if coord:
            valid_coords += 1
            rec['has_coord'] = True
            points.append({**rec, 'latitude': coord[0], 'longitude': coord[1]})
        else:
            if str(point1_raw or '').strip():
                invalid_coords += 1

        pkey = (region_norm or '-', province_label or '-')
        summary[pkey] = summary.get(pkey, 0) + 1

        wk_key = (wk_create or '-').strip() or '-'
        if wk_key not in weekly:
            weekly[wk_key] = {}
        weekly[wk_key][province_label or '-'] = weekly[wk_key].get(province_label or '-', 0) + 1

    summary_rows = [
        {'region': k[0], 'province': k[1], 'record_count': v}
        for k, v in sorted(summary.items(), key=lambda kv: (str(kv[0][0]), -kv[1], str(kv[0][1])))
    ]

    def _wk_sort_key(s):
        s = str(s or '').strip()
        m = re.search(r'(\d+)$', s)
        return int(m.group(1)) if m else 9999

    weekly_rows = []
    for wk in sorted(weekly.keys(), key=_wk_sort_key):
        pc = weekly[wk]
        weekly_rows.append({'week': wk, 'total': sum(pc.values()), 'province_counts': pc})

    provinces = sorted({r['province'] for r in summary_rows if r['province'] and r['province'] != '-'})

    err_type_counts = {}
    for e in debug_coords:
        t = e.get('type', 'unknown')
        err_type_counts[t] = err_type_counts.get(t, 0) + 1

    insight = [f'อ่านข้อมูลได้ {len(detail_rows)} แถว']
    if valid_coords:
        insight.append(f'พิกัดใช้งานได้ {valid_coords} รายการ')
    if invalid_coords:
        insight.append(f'พิกัดใช้ไม่ได้หรือรูปแบบผิด {invalid_coords} รายการ')
    if err_type_counts.get('format_error'):
        insight.append(f'format พิกัดผิด {err_type_counts.get("format_error")} รายการ')
    if err_type_counts.get('out_of_range'):
        insight.append(f'lat/lon เกินช่วง {err_type_counts.get("out_of_range")} รายการ')
    if not provinces:
        insight.append('ไม่พบจังหวัดที่อ่านได้จากฟิลด์ Province/Section แต่ยังแสดงข้อมูลดิบในตาราง')

    return _fireburn_debug_payload(
        updated_at, headers=headers, detail_rows=detail_rows, points=points, summary_rows=summary_rows, weekly_rows=weekly_rows, provinces=provinces,
        debug={
            'total_rows': len(detail_rows),
            'valid_coords': valid_coords,
            'invalid_coords': invalid_coords,
            'error_samples': debug_coords[:50],
            'headers': headers,
            'index_map': {
                'ticket': idx_ticket, 'region': idx_region, 'province': idx_province, 'section': idx_section,
                'team_id': idx_team_id, 'type_ofc': idx_type_ofc, 'subject': idx_subject,
                'subproject': idx_subproject, 'wk_create': idx_wk_create, 'point1': idx_point1
            }
        },
        insight=insight
    )

@app.route('/api/fireburn-2026')
def api_fireburn_2026():
    try:
        return jsonify(build_fireburn_2026())
    except Exception as e:
        log.exception('api_fireburn_2026 failed')
        return jsonify(_fireburn_debug_payload(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            headers=[],
            debug={'total_rows': 0, 'valid_coords': 0, 'invalid_coords': 0, 'error_samples': [{'row': '-', 'raw': '-', 'cleaned': '-', 'type': 'api_error', 'error': str(e)}], 'headers': []},
            insight=['Fireburn API ล้ม แต่ระบบกันพังไว้แล้ว'],
            error='fireburn_failed'
        )), 200

MAXMA_SHEET_ID = '1t8DErfQLBRXkoaorDFMdAxtLZf-RA_hzQ-xFQI6EgBo'
MAXMA_SHEET_NAME = 'ชีต1'

def build_team_locations():
    """ดึงพิกัดทีมล่าสุด (เวลาเดินทางล่าสุดต่อทีม) จาก MAXMA Sheet"""
    try:
        gc = get_client()
        ws = gc.open_by_key(MAXMA_SHEET_ID).worksheet(MAXMA_SHEET_NAME)
        rows = ws.get_all_values()
        if not rows:
            return {'teams': [], 'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        headers = [h.strip() for h in rows[0]]
        col = {h: i for i, h in enumerate(headers) if h}

        def g(row, name):
            i = col.get(name)
            return str(row[i]).strip() if i is not None and i < len(row) else ''

        # หา column Update พิกัด และ เวลาเดินทาง
        pikat_col = col.get('Update พิกัด')
        travel_col = col.get('เวลาเดินทาง')
        team_col = col.get('Team ID')
        type_col = col.get('Type Team')
        que_col = col.get('Que')
        verify_col = col.get('Team Verify')

        if pikat_col is None or travel_col is None or team_col is None:
            log.warning(f'Missing columns. Found: {list(col.keys())[:20]}')
            return {'teams': [], 'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        # หา row ที่มีเวลาเดินทางล่าสุดต่อทีม
        team_latest = {}
        for row_idx, row in enumerate(rows[1:], start=2):
            team_id = g(row, 'Team ID')
            if not team_id:
                continue
            travel_raw = g(row, 'เวลาเดินทาง')
            pikat_raw = row[pikat_col].strip() if pikat_col < len(row) else ''
            if not travel_raw or not pikat_raw:
                continue
            dt_travel = parse_dt(travel_raw)
            if not dt_travel:
                continue
            coord = parse_coord(pikat_raw)
            if not coord:
                continue
            if team_id not in team_latest or dt_travel > team_latest[team_id]['dt']:
                team_latest[team_id] = {
                    'dt': dt_travel,
                    'lat': coord[0],
                    'lon': coord[1],
                    'team_id': team_id,
                    'type_team': row[type_col].strip() if type_col is not None and type_col < len(row) else '',
                    'que': row[que_col].strip() if que_col is not None and que_col < len(row) else '',
                    'team_verify': row[verify_col].strip() if verify_col is not None and verify_col < len(row) else '',
                    'travel_time': travel_raw,
                }

        teams = []
        for team_id, d in team_latest.items():
            teams.append({
                'team_id': d['team_id'],
                'type_team': d['type_team'],
                'que': d['que'],
                'team_verify': d['team_verify'],
                'travel_time': d['travel_time'],
                'latitude': d['lat'],
                'longitude': d['lon'],
            })

        log.info(f'Team locations: {len(teams)} teams')
        return {
            'teams': teams,
            'total': len(teams),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        log.error(f'build_team_locations error: {e}')
        return {'teams': [], 'error': str(e), 'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

@app.route('/api/team-locations')
def api_team_locations():
    try:
        return jsonify(build_team_locations())
    except Exception as e:
        log.exception('api_team_locations error')
        return jsonify({'error': str(e), 'teams': []}), 500

@app.route('/api/focus-priority')
def api_focus_priority():
    try:
        return jsonify(build_focus_priority())
    except Exception as e:
        log.exception('api_focus_priority error')
        return jsonify({'error': str(e)}), 500

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

@app.route('/api/realtime-monitor')
def api_realtime_monitor():
    view = request.args.get('view', default='FBB')
    if view not in BOOKMARK_VIEWS:
        return jsonify({'error': f'Unknown view {view!r}. Valid: {list(BOOKMARK_VIEWS)}'}), 400
    region_param = request.args.get('region') or None
    region_filter = [r.strip() for r in region_param.split(',') if r.strip()] if region_param else None
    trueowner = request.args.get('trueowner') or None
    aging_param = request.args.get('aging') or None
    aging_filter = [a.strip() for a in aging_param.split(',') if a.strip()] if aging_param else None
    district = request.args.get('district') or None
    try:
        _, gs_client = get_drive_and_sheets_clients()
        data = build_realtime_response(
            gs_client, view_key=view, region_filter=region_filter,
            trueowner_filter=trueowner, aging_filter=aging_filter, district_filter=district,
        )
        return jsonify(data)
    except Exception as e:
        log.exception("realtime-monitor API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/realtime-monitor/insert-time')
def api_realtime_monitor_insert_time():
    try:
        _, gs_client = get_drive_and_sheets_clients()
        return jsonify({'insert_time': get_insert_time(gs_client)})
    except Exception as e:
        log.exception("realtime-monitor insert-time check failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/pending-ticket')
def api_pending_ticket():
    def multi(name):
        val = request.args.get(name) or None
        return [v.strip() for v in val.split(',') if v.strip()] if val else None

    bookmark = multi('bookmark')
    trueowner = multi('trueowner')
    severity = multi('severity')
    district = multi('district')
    group_problem = multi('group_problem')
    aging = multi('aging')
    try:
        _, gs_client = get_drive_and_sheets_clients()
        data = build_pending_ticket_response(
            gs_client, bookmark_filter=bookmark, trueowner_filter=trueowner,
            severity_filter=severity, district_filter=district, group_problem_filter=group_problem,
            aging_filter=aging,
        )
        return jsonify(data)
    except Exception as e:
        log.exception("pending-ticket API failed")
        return jsonify({'error': str(e)}), 500

def _pending_ticket_filters_from_request():
    def multi(name):
        val = request.args.get(name) or None
        return [v.strip() for v in val.split(',') if v.strip()] if val else None
    return dict(
        bookmark_filter=multi('bookmark'), trueowner_filter=multi('trueowner'),
        severity_filter=multi('severity'), district_filter=multi('district'),
        group_problem_filter=multi('group_problem'), aging_filter=multi('aging'),
    )

@app.route('/api/pending-ticket/export-excel')
def api_pending_ticket_export_excel():
    """Downloads the currently-filtered Pending Ticket table as .xlsx -
    same filters (bookmark/trueowner/severity/district/group_problem/aging)
    as the main table, same columns as the mirror sheet export."""
    try:
        _, gs_client = get_drive_and_sheets_clients()
        data = build_pending_ticket_response(gs_client, **_pending_ticket_filters_from_request())
        xlsx_bytes = build_pending_ticket_xlsx(data.get('tickets', []))
        filename = f"pending-ticket-{bangkok_now().strftime('%Y-%m-%d_%H%M')}.xlsx"
        return Response(
            xlsx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        log.exception("pending-ticket export-excel failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/pending-ticket/update', methods=['POST'])
def api_pending_ticket_update():
    payload = request.get_json(silent=True) or {}
    ticket_id = payload.get('ticket_id')
    if not ticket_id:
        return jsonify({'error': 'ticket_id is required'}), 400
    fields = {
        k: payload.get(k, '')
        for k in ('group_problem', 'action_team', 'detail', 'image_link', 'plan_closed_date')
    }
    # ต้องกรอกครบทั้ง 4 ช่องเสมอ ห้ามว่างช่องใดช่องหนึ่ง - เช็คซ้ำฝั่ง server
    # เผื่อมีการเรียก API ตรงๆ ข้ามหน้าเว็บ (ฝั่งหน้าเว็บเช็คไว้แล้วเช่นกัน)
    group_problem = (fields.get('group_problem') or '').strip()
    if not group_problem:
        return jsonify({'error': 'Group Problem ห้ามว่าง'}), 400
    action_team = (fields.get('action_team') or '').strip()
    if not action_team:
        return jsonify({'error': 'Action Team ห้ามว่าง'}), 400

    # รายละเอียดต้องกรอกทุกครั้งและต้องอยู่ในรูปแบบ "ปัญหาจากการตรวจสอบ/วิธีแก้ไข"
    detail = (fields.get('detail') or '').strip()
    if not detail or '/' not in detail:
        return jsonify({'error': 'detail ต้องกรอกตามรูปแบบ "ปัญหาจากการตรวจสอบ/วิธีแก้ไข" (ต้องมี /)'}), 400
    fields['detail'] = detail

    # Plan Closed Date ห้ามว่าง และห้ามเป็นวันที่ย้อนหลังวันปัจจุบัน (เทียบเวลาไทย)
    plan_closed_date = (fields.get('plan_closed_date') or '').strip()
    if not plan_closed_date:
        return jsonify({'error': 'Plan Closed Date ห้ามว่าง'}), 400
    today_str = bangkok_now().strftime('%Y-%m-%d')
    if plan_closed_date < today_str:
        return jsonify({'error': 'Plan Closed Date ต้องไม่ใช่วันที่ย้อนหลังวันปัจจุบัน'}), 400

    updated_by = session.get('user_name') or 'unknown'
    try:
        _, gs_client = get_drive_and_sheets_clients()
        saved = save_work_log_entry(gs_client, ticket_id, fields, updated_by=updated_by)
        return jsonify({'status': 'saved', 'row': saved})
    except Exception as e:
        log.exception("pending-ticket update failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/exclusive-pending')
def api_exclusive_pending():
    try:
        _, gs_client = get_drive_and_sheets_clients()
        priority_filter = request.args.get('priority') or None
        restrict_to_over_sla = request.args.get('all_aging') != '1'  # ?all_aging=1 disables the over-SLA-only restriction
        data = build_exclusive_pending_response(gs_client, priority_filter=priority_filter, restrict_to_over_sla=restrict_to_over_sla)
        return jsonify(data)
    except Exception as e:
        log.exception("exclusive-pending API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flood-nan')
def api_flood_nan():
    """Every known Nan-province site plotted on a map, color-coded by the
    worst open ticket severity matched to that site (LOCATION ID ==
    CINAME), plus a Nan-only ticket detail table and a severity x Bookmark
    classification matrix."""
    try:
        _, gs_client = get_drive_and_sheets_clients()
        data = flood_nan.build_flood_nan_response(gs_client)
        return jsonify(data)
    except Exception as e:
        log.exception("flood-nan API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flood-nan/markers', methods=['GET', 'POST'])
def api_flood_nan_markers():
    """GET: list manually-placed remark pins. POST: add one (lat, lon,
    remark required) - anyone logged in can add, same as other freeform
    notes elsewhere in the app."""
    try:
        _, gs_client = get_drive_and_sheets_clients()
        if request.method == 'GET':
            return jsonify({'markers': flood_nan.list_manual_markers(gs_client)})
        payload = request.get_json(silent=True) or {}
        lat = payload.get('lat')
        lon = payload.get('lon')
        remark = (payload.get('remark') or '').strip()
        if lat is None or lon is None or not remark:
            return jsonify({'error': 'lat, lon, remark are required'}), 400
        created_by = session.get('user_name') or session.get('user_email') or ''
        marker = flood_nan.add_manual_marker(gs_client, float(lat), float(lon), remark, created_by)
        return jsonify({'marker': marker})
    except Exception as e:
        log.exception("flood-nan markers API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flood-nan/markers/<marker_id>', methods=['DELETE'])
def api_flood_nan_delete_marker(marker_id):
    try:
        _, gs_client = get_drive_and_sheets_clients()
        deleted = flood_nan.delete_manual_marker(gs_client, marker_id)
        if not deleted:
            return jsonify({'error': 'not found'}), 404
        return jsonify({'ok': True})
    except Exception as e:
        log.exception("flood-nan delete marker failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flood-nan/site-remark', methods=['POST'])
def api_flood_nan_site_remark():
    """Sets (overwriting any previous) the current-action remark for a DN
    site - shown pinned in that site's map popup."""
    try:
        payload = request.get_json(silent=True) or {}
        location_id = (payload.get('location_id') or '').strip()
        remark = (payload.get('remark') or '').strip()
        if not location_id or not remark:
            return jsonify({'error': 'location_id and remark are required'}), 400
        _, gs_client = get_drive_and_sheets_clients()
        updated_by = session.get('user_name') or session.get('user_email') or ''
        result = flood_nan.set_site_remark(gs_client, location_id, remark, updated_by)
        return jsonify({'remark': result})
    except Exception as e:
        log.exception("flood-nan site-remark failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flood-nan/site-remark/<location_id>', methods=['DELETE'])
def api_flood_nan_delete_site_remark(location_id):
    try:
        _, gs_client = get_drive_and_sheets_clients()
        deleted = flood_nan.delete_site_remark(gs_client, location_id)
        if not deleted:
            return jsonify({'error': 'not found'}), 404
        return jsonify({'ok': True})
    except Exception as e:
        log.exception("flood-nan delete site-remark failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flood-nan/trends')
def api_flood_nan_trends():
    """24-hour Nan ticket-count trend (SA Mobile/SA Online/NSA1-2/NSA3/NSA4)
    plus a 24-hour SA-Mobile-only District trend, both built from the
    hourly ~:29-past Drive backups (the current hour uses live data
    instead). Can be slow on a cold cache - each not-yet-cached hour means
    downloading and parsing a full backup file, up to 24 times."""
    try:
        drive_service, gs_client = get_drive_and_sheets_clients()
        data = flood_nan.build_nan_trends(gs_client, drive_service, hours=24)
        return jsonify(data)
    except Exception as e:
        log.exception("flood-nan trends API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary-nan')
def api_summary_nan():
    """Static snapshot report (from the uploaded closed-ticket workbook,
    not a live Google Sheet) - site-level root cause + per-ticket repair
    detail for the 19-22 Aug Nan flood event."""
    try:
        data = summary_nan.build_summary_nan_response()
        return jsonify(data)
    except Exception as e:
        log.exception("summary-nan API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/p0-snapshot-comparison')
def api_p0_snapshot_comparison():
    """P0 count right now vs P0 count at ~01:15 today (from the Drive backup
    closest to that time), per group (Mobile SA1-4, FBB SA1-4, NSA1-2,
    NSA3/4). Slow on a cache miss (downloads a ~5MB xlsx from Drive), so
    it's cached for 15 min and meant to be triggered on demand from the
    frontend rather than loaded automatically on every page view."""
    try:
        drive_service, gs_client = get_drive_and_sheets_clients()
        use_cache = request.args.get('refresh') != '1'
        data = build_p0_snapshot_comparison(gs_client, drive_service, use_cache=use_cache)
        return jsonify(data)
    except Exception as e:
        log.exception("p0-snapshot-comparison API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule')
def api_oncall_schedule():
    try:
        _, gs_client = get_drive_and_sheets_clients()
        data = oncall.load_oncall_schedule(gs_client)
        if data is None:
            return jsonify({'error': 'not seeded yet'}), 404
        return jsonify(data)
    except Exception as e:
        log.exception("oncall-schedule API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/toggle', methods=['POST'])
def api_oncall_schedule_toggle():
    payload = request.get_json(silent=True) or {}
    team_id1 = payload.get('team_id1')
    date_str = payload.get('date')
    new_status = payload.get('status')
    if not team_id1 or not date_str or new_status not in ('on', 'off', 'blank'):
        return jsonify({'error': 'team_id1, date, and status (on/off/blank) are required'}), 400
    updated_by = session.get('user_name') or 'unknown'
    try:
        _, gs_client = get_drive_and_sheets_clients()
        result = oncall.toggle_oncall_cell(gs_client, team_id1, date_str, new_status, updated_by=updated_by)
        return jsonify(result)
    except Exception as e:
        log.exception("oncall-schedule toggle failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/add-month', methods=['POST'])
def api_oncall_schedule_add_month():
    payload = request.get_json(silent=True) or {}
    year_month = payload.get('year_month')  # "YYYY-MM"
    if not year_month:
        return jsonify({'error': 'year_month ("YYYY-MM") is required'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        added = oncall.add_month_columns(gs_client, year_month)
        return jsonify({'added_dates': added})
    except Exception as e:
        log.exception("oncall-schedule add-month failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/reset-default-on', methods=['POST'])
def api_oncall_reset_default_on():
    """One-time cleanup: clears every "on" cell (the old blanket default
    from the Excel seed) to "blank", so Oncall becomes something people
    pick explicitly instead of counting everyone by default."""
    try:
        _, gs_client = get_drive_and_sheets_clients()
        cleared = oncall.reset_default_on_to_blank(gs_client)
        return jsonify({'cleared': cleared})
    except Exception as e:
        log.exception("oncall-schedule reset-default-on failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/add-district-column', methods=['POST'])
def api_oncall_add_district_column():
    """One-time migration: POST {team_district_map: {team_id1: "district, district"}}
    to insert the District identity column, without touching any date-cell
    (Oncall/Day Off) data already entered. Token-gated (same pattern as
    seed-users/oncall-seed) since this is meant to run from a standalone
    tool file outside a logged-in browser session."""
    expected_token = os.environ.get('SEED_USERS_TOKEN')
    if not expected_token or request.args.get('token') != expected_token:
        return jsonify({'error': 'missing or invalid token'}), 403
    payload = request.get_json(silent=True) or {}
    team_district_map = payload.get('team_district_map')
    if not isinstance(team_district_map, dict):
        return jsonify({'error': 'expected {team_district_map: "districts"}'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        filled = oncall.add_district_column(gs_client, team_district_map)
        return jsonify({'filled': filled})
    except Exception as e:
        log.exception("oncall-schedule add-district-column failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/add-note-column', methods=['POST'])
def api_oncall_add_note_column():
    """One-time schema migration (admin-only): adds the Note column to
    OncallSchedule without touching any existing Oncall/Day Off data.
    Session-authenticated (runs from inside the logged-in dashboard, not a
    standalone tool), restricted to the admin account."""
    if session.get('user_email') != 'saridphong_n@bbtec.co.th':
        return jsonify({'error': 'forbidden'}), 403
    try:
        _, gs_client = get_drive_and_sheets_clients()
        count = oncall.add_note_column(gs_client)
        return jsonify({'rows': count})
    except Exception as e:
        log.exception("oncall-schedule add-note-column failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/add-note-column', methods=['POST'])
def api_oncall_escalation_add_note_column():
    if session.get('user_email') != 'saridphong_n@bbtec.co.th':
        return jsonify({'error': 'forbidden'}), 403
    try:
        _, gs_client = get_drive_and_sheets_clients()
        count = oncall_escalation.add_note_column(gs_client)
        return jsonify({'rows': count})
    except Exception as e:
        log.exception("oncall-escalation add-note-column failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/repair-columns', methods=['POST'])
def api_oncall_repair_columns():
    if session.get('user_email') != 'saridphong_n@bbtec.co.th':
        return jsonify({'error': 'forbidden'}), 403
    try:
        _, gs_client = get_drive_and_sheets_clients()
        count = oncall.repair_column_alignment(gs_client)
        return jsonify({'rows': count})
    except Exception as e:
        log.exception("oncall-schedule repair-columns failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/repair-columns', methods=['POST'])
def api_oncall_escalation_repair_columns():
    if session.get('user_email') != 'saridphong_n@bbtec.co.th':
        return jsonify({'error': 'forbidden'}), 403
    try:
        _, gs_client = get_drive_and_sheets_clients()
        count = oncall_escalation.repair_column_alignment(gs_client)
        return jsonify({'rows': count})
    except Exception as e:
        log.exception("oncall-escalation repair-columns failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/duplicate-province', methods=['POST'])
def api_oncall_escalation_duplicate_province():
    """Admin-only: copies every Sup/Eng Zone contact from one province to
    another (same person, e.g. a shared Eng Zone covering two provinces),
    so they show up under both. POST {source_province, target_province}."""
    if session.get('user_email') != 'saridphong_n@bbtec.co.th':
        return jsonify({'error': 'forbidden'}), 403
    payload = request.get_json(silent=True) or {}
    source_province = payload.get('source_province')
    target_province = payload.get('target_province')
    if not source_province or not target_province:
        return jsonify({'error': 'source_province and target_province are required'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        added = oncall_escalation.duplicate_province_contacts(gs_client, source_province, target_province)
        return jsonify({'added': added})
    except Exception as e:
        log.exception("oncall-escalation duplicate-province failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-schedule/note', methods=['POST'])
def api_oncall_schedule_note():
    payload = request.get_json(silent=True) or {}
    team_id1 = payload.get('team_id1')
    note_text = payload.get('note', '')
    if not team_id1:
        return jsonify({'error': 'team_id1 is required'}), 400
    updated_by = session.get('user_name') or 'unknown'
    try:
        _, gs_client = get_drive_and_sheets_clients()
        result = oncall.update_note(gs_client, team_id1, note_text, updated_by=updated_by)
        return jsonify(result)
    except Exception as e:
        log.exception("oncall-schedule note update failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/note', methods=['POST'])
def api_oncall_escalation_note():
    payload = request.get_json(silent=True) or {}
    row_key = payload.get('row_key')
    note_text = payload.get('note', '')
    if not row_key:
        return jsonify({'error': 'row_key is required'}), 400
    updated_by = session.get('user_name') or 'unknown'
    try:
        _, gs_client = get_drive_and_sheets_clients()
        result = oncall_escalation.update_note(gs_client, row_key, note_text, updated_by=updated_by)
        return jsonify(result)
    except Exception as e:
        log.exception("oncall-escalation note update failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation')
def api_oncall_escalation():
    try:
        _, gs_client = get_drive_and_sheets_clients()
        data = oncall_escalation.load_escalation_contacts(gs_client)
        if data is None:
            return jsonify({'error': 'not seeded yet'}), 404
        return jsonify(data)
    except Exception as e:
        log.exception("oncall-escalation API failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/seed', methods=['POST'])
def api_oncall_escalation_seed():
    """One-time bootstrap: POST {rows: [{province, position, name, tel, type}, ...],
    dates: ["YYYY-MM-DD", ...]} to (re)populate the OncallEscalation tab
    with a full date grid, all defaulted to blank. Token-gated like the
    other seed tools since it's meant to run from a standalone file."""
    expected_token = os.environ.get('SEED_USERS_TOKEN')
    if not expected_token or request.args.get('token') != expected_token:
        return jsonify({'error': 'missing or invalid token'}), 403
    payload = request.get_json(silent=True) or {}
    rows = payload.get('rows')
    dates = payload.get('dates')
    if not isinstance(rows, list) or not isinstance(dates, list):
        return jsonify({'error': 'expected {rows: [...], dates: [...]}'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        count = oncall_escalation.seed_escalation_contacts(gs_client, rows, dates)
        return jsonify({'seeded_rows': count, 'dates': len(dates)})
    except Exception as e:
        log.exception("oncall-escalation seed failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/toggle', methods=['POST'])
def api_oncall_escalation_toggle():
    payload = request.get_json(silent=True) or {}
    row_key = payload.get('row_key')
    date_str = payload.get('date')
    new_status = payload.get('status')
    if not row_key or not date_str or new_status not in ('on', 'off', 'blank'):
        return jsonify({'error': 'row_key, date, and status (on/off/blank) are required'}), 400
    updated_by = session.get('user_name') or 'unknown'
    try:
        _, gs_client = get_drive_and_sheets_clients()
        result = oncall_escalation.toggle_escalation_cell(gs_client, row_key, date_str, new_status, updated_by=updated_by)
        return jsonify(result)
    except Exception as e:
        log.exception("oncall-escalation toggle failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall-escalation/add-month', methods=['POST'])
def api_oncall_escalation_add_month():
    payload = request.get_json(silent=True) or {}
    year_month = payload.get('year_month')
    if not year_month:
        return jsonify({'error': 'year_month ("YYYY-MM") is required'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        added = oncall_escalation.add_month_columns(gs_client, year_month)
        return jsonify({'added_dates': added})
    except Exception as e:
        log.exception("oncall-escalation add-month failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/oncall/seed', methods=['POST'])
def api_oncall_seed():
    """One-time bootstrap (same token-gate pattern as seed-users): POST
    {dates: [...], rows: [...]} to (re)populate the OncallSchedule tab."""
    expected_token = os.environ.get('SEED_USERS_TOKEN')
    if not expected_token or request.args.get('token') != expected_token:
        return jsonify({'error': 'missing or invalid token'}), 403
    payload = request.get_json(silent=True) or {}
    dates = payload.get('dates')
    rows = payload.get('rows')
    if not isinstance(dates, list) or not isinstance(rows, list):
        return jsonify({'error': 'expected {dates: [...], rows: [...]}'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        count = oncall.seed_oncall_schedule(gs_client, dates, rows)
        return jsonify({'seeded_rows': count, 'dates': len(dates)})
    except Exception as e:
        log.exception("oncall seed failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/admin/add-user', methods=['POST'])
def api_add_user():
    """Adds a single login account without needing the SEED_USERS_TOKEN
    (that token lives in Railway env vars, out of reach for normal admin
    use) - session-gated to the same admin instead. Default password is
    the last 4 digits of the phone number; the account is flagged to force
    a password change on first login, same as every other seeding path."""
    if session.get('user_email') != 'saridphong_n@bbtec.co.th':
        return jsonify({'error': 'forbidden'}), 403
    payload = request.get_json(silent=True) or {}
    email = (payload.get('email') or '').strip()
    name = (payload.get('name') or '').strip()
    phone = (payload.get('phone') or '').strip()
    if not email:
        return jsonify({'error': 'กรุณากรอกอีเมล'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        created = auth.seed_user(gs_client, email, phone, name=name)
        if not created:
            return jsonify({'error': 'มีบัญชีนี้อยู่แล้ว (อีเมลซ้ำ)'}), 400
        return jsonify({'ok': True, 'email': email})
    except Exception as e:
        log.exception("add-user failed")
        return jsonify({'error': str(e)}), 500

@app.route('/api/pending-ticket/rename-group-problem', methods=['POST'])
def api_rename_group_problem():
    """One-time cleanup: renames every existing group_problem value in
    TicketWorkLog from one string to another (e.g. after consolidating two
    similar dropdown options into one), so already-saved tickets don't show
    blank once the old option disappears from the list."""
    payload = request.get_json(silent=True) or {}
    old_value = (payload.get('old_value') or '').strip()
    new_value = (payload.get('new_value') or '').strip()
    if not old_value or not new_value:
        return jsonify({'error': 'old_value and new_value are required'}), 400
    try:
        _, gs_client = get_drive_and_sheets_clients()
        changed = rename_group_problem_value(gs_client, old_value, new_value)
        return jsonify({'changed': changed})
    except Exception as e:
        log.exception("rename-group-problem failed")
        return jsonify({'error': str(e)}), 500

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
