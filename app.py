import os, json, logging, threading, time, re, math
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler

# ================= CONFIG =================
SHEET_ID      = '1_l5UAj1etjGgLCR4DSG6qDoK8c1unFnO6NVHVwvmbAU'
SHEET_NAME    = 'Sheet1'
CM_BASE       = 3
OFC_BASE      = 2
REBUILD_HOURS = 6
VALID_YEAR    = '2569'
EXCLUDE       = {'PS_CMI_ofc_011','PS_CMI_ofc_012'}

# ================= APP =================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

_cache = None
_cache_lock = threading.Lock()
_building = False

# ================= MAP =================
PROV_MAP = {
    'TRUE-TH-BBT-NOR1-CMI1-NOP':'CMI','TRUE-TH-BBT-NOR1-CRI-NOP':'CRI',
    'TRUE-TH-BBT-NOR2-PSN-NOP':'PSN','TRUE-TH-BBT-NOR2-PCB-NOP':'PCB',
    'TRUE-TH-BBT-NOR2-TAK-NOP':'TAK','TRUE-TH-BBT-NOR1-MHS-NOP':'MHS',
    'TRUE-TH-BBT-NOR1-NAN-NOP':'NAN','TRUE-TH-BBT-NOR2-PCT-NOP':'PCT',
    'TRUE-TH-BBT-NOR1-LPG-NOP':'LPG','TRUE-TH-BBT-NOR2-UTR-NOP':'UTR',
    'TRUE-TH-BBT-NOR2-KPP-NOP':'KPP','TRUE-TH-BBT-NOR2-SKT-NOP':'SKT',
    'TRUE-TH-BBT-NOR1-PHE-NOP':'PHE','TRUE-TH-BBT-NOR1-LPN-NOP':'LPN',
    'TRUE-TH-BBT-NOR1-PYO-NOP':'PYO'
}

NOR1 = {'CMI','CRI','MHS','NAN','LPG','PHE','LPN','PYO'}

# ================= GOOGLE =================
def get_client():
    creds_json = os.environ['GOOGLE_CREDENTIALS_JSON']
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        info,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    return gspread.authorize(creds)

# ================= PARSE =================
def parse_dt(v):
    if not v: return None
    m = re.match(r'(\d+)/(\d+)/(\d+)\s+(\d+):(\d+)', str(v))
    if not m: return None
    d,mn,y,h,mi = map(int,m.groups())
    if y > 2100: y -= 543
    return datetime(y,mn,d,h,mi)

def to_month(dt):
    return f'{dt.year+543}-{dt.month:02d}' if dt else None

def is_valid(m):
    return m and m.startswith(VALID_YEAR)

# ================= CORE =================
def build_data():
    log.info("BUILD START")

    gc = get_client()
    ws = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    rows = ws.get_all_values()

    headers = rows[0]
    col = {h:i for i,h in enumerate(headers)}

    def g(r,name):
        return r[col[name]] if name in col and col[name] < len(r) else ""

    teams = {}
    months = set()

    for r in rows[1:]:
        tid = g(r,'Team ID')
        ttype = g(r,'Type Team')
        prov_raw = g(r,'Province')

        if not tid or ttype not in ['CM','OFC']: continue
        if tid in EXCLUDE: continue

        prov = PROV_MAP.get(prov_raw,'')
        reg = 'NOR1' if prov in NOR1 else 'NOR2'

        dt = parse_dt(g(r,'Link Up')) or parse_dt(g(r,'เวลาเดินทาง'))
        m = to_month(dt)

        if not is_valid(m): continue
        months.add(m)

        if tid not in teams:
            teams[tid] = {
                'type':ttype,
                'prov':prov,
                'reg':reg,
                'p1':0,
                'p2':0,
                'days':set(),
                'tkt':0,
                'non':0
            }

        tm = teams[tid]
        tm['days'].add(m)

        if g(r,'Ticket').startswith(('TT','INC')):
            tm['tkt'] += 1
        else:
            tm['non'] += 1

        tm['p1'] += 1
        tm['p2'] += 1

    ts = []
    for tid,tm in teams.items():
        days = len(tm['days']) or 1
        p1 = round(tm['p1']/days,2)
        base = CM_BASE if tm['type']=='CM' else OFC_BASE
        st = 'above' if p1>=base else 'below'

        ts.append({
            'id':tid,
            'type':tm['type'],
            'reg':tm['reg'],
            'prov':tm['prov'],
            'p1':p1,
            'p2':p1,
            'base':base,
            'vs1':round(p1-base,2),
            'st':st,
            'days':days,
            'tkt':tm['tkt'],
            'non':tm['non']
        })

    log.info("BUILD DONE")

    return {
        'ts':ts,
        'months':sorted(months),
        'nor1':list(NOR1),
        'gstats':{
            'total_tkt':sum(t['tkt'] for t in ts),
            'total_non':sum(t['non'] for t in ts)
        },
        'cached_at':datetime.now().isoformat()
    }

# ================= CACHE =================
def rebuild_cache():
    global _cache,_building
    if _building: return
    _building=True
    try:
        data = build_data()
        with _cache_lock:
            _cache = data
        log.info("CACHE READY")
    except Exception as e:
        log.error(e)
    finally:
        _building=False

# ================= API =================
@app.route('/api/dashboard')
def api_dashboard():
    with _cache_lock:
        if _cache is None:
            return jsonify({'status':'building'}),503
        return jsonify(_cache)

@app.route('/api/status')
def status():
    return jsonify({'status':'ok'})

# ================= FRONTEND =================
@app.route('/')
def home():
    return render_template('dashboard.html')

# ================= START =================
def start():
    threading.Thread(target=rebuild_cache, daemon=True).start()
    s = BackgroundScheduler()
    s.add_job(rebuild_cache, 'interval', hours=REBUILD_HOURS)
    s.start()

start()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',8080)))
