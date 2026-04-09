import os, json, logging, threading, time
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler

# ── Config ────────────────────────────────────────────────────
SHEET_ID   = '1_l5UAj1etjGgLCR4DSG6qDoK8c1unFnO6NVHVwvmbAU'
SHEET_NAME = 'Sheet1'
CM_BASE    = 3
OFC_BASE   = 2
REBUILD_HOURS = 6  # rebuild cache ทุก 6 ชั่วโมง

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

app   = Flask(__name__)
CORS(app)

# ── Global cache ───────────────────────────────────────────────
_cache      = None
_cache_lock = threading.Lock()
_building   = False

# ── Google Sheets client ───────────────────────────────────────
def get_client():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        raise RuntimeError('GOOGLE_CREDENTIALS_JSON env var not set')
    info   = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds  = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# ── Build dashboard data ───────────────────────────────────────
def build_data():
    log.info('Building dashboard data...')
    t0 = time.time()

    gc    = get_client()
    ws    = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    rows  = ws.get_all_values()

    headers = rows[0]
    col     = {h.strip(): i for i, h in enumerate(headers) if h.strip()}

    def g(row, name):
        i = col.get(name)
        return row[i].strip() if i is not None and i < len(row) else ''

    def to_min(v):
        if not v: return 0
        import re
        m = re.match(r'(\d+):(\d+)', str(v).strip())
        return int(m.group(1)) * 60 + int(m.group(2)) if m else 0

    def fmt_time(v):
        return str(v).strip() if v else ''

    # ── Parse ──────────────────────────────────────────────────
    team_map  = {}
    heat_map  = {}
    months    = set()
    prov_names = {}
    drill_map = {}
    cutoff    = datetime.now() - timedelta(days=90)

    thai_months = ['','ม.ค.','ก.พ.','มี.ค.','เม.ย.','พ.ค.','มิ.ย.',
                   'ก.ค.','ส.ค.','ก.ย.','ต.ค.','พ.ย.','ธ.ค.']

    log.info(f'Processing {len(rows)-1} rows...')

    for row in rows[1:]:
        team_id   = g(row, 'Team ID')
        type_team = g(row, 'Type Team')
        region    = g(row, 'Region')
        prov      = g(row, 'Province')

        if not team_id or type_team not in ('CM', 'OFC'):
            continue
        if prov:
            prov_names[prov] = prov

        hold_min   = to_min(g(row, 'Hold'))
        linkup_min = to_min(g(row, 'Link Up'))
        work_min   = to_min(g(row, 'เวลาเดินทาง')) + to_min(g(row, 'เวลาเริ่มซ่อม'))
        pdt1 = linkup_min / 60 if linkup_min > 0 else 0
        pdt2 = (linkup_min + hold_min) / 60

        # Date
        month = date_str = ''
        lu_raw = g(row, 'Link Up')
        if lu_raw:
            try:
                from dateutil import parser as dparser
                d = dparser.parse(lu_raw, dayfirst=False)
                by = d.year + 543
                month    = f'{by}-{d.month:02d}'
                date_str = f'{by}-{d.month:02d}-{d.day:02d}'
                months.add(month)
            except:
                pass

        is_ticket = bool(g(row, 'Ticket')) and g(row, 'Categories') != 'Non-Ticket'

        # Team aggregate
        if team_id not in team_map:
            team_map[team_id] = dict(
                id=team_id, type=type_team, reg=region, prov=prov,
                sp1=0, sp2=0, sh=0, cnt=0,
                max1=0, max2=0, tkt=0, non=0,
                days=set(), mth={}
            )
        tm = team_map[team_id]
        if pdt1 > 0:
            tm['sp1'] += pdt1; tm['sp2'] += pdt2; tm['cnt'] += 1
            if pdt1 > tm['max1']: tm['max1'] = pdt1
            if pdt2 > tm['max2']: tm['max2'] = pdt2
        tm['sh'] += work_min / 60
        if is_ticket: tm['tkt'] += 1
        else:         tm['non'] += 1
        if date_str:
            tm['days'].add(date_str)
        if month and pdt1 > 0:
            if month not in tm['mth']:
                tm['mth'][month] = dict(sp=0, cnt=0, days=set())
            tm['mth'][month]['sp']  += pdt1
            tm['mth'][month]['cnt'] += 1
            if date_str:
                tm['mth'][month]['days'].add(date_str)

        # Heatmap
        if month and prov and pdt1 > 0:
            hk = f'{month}||{prov}'
            if hk not in heat_map:
                heat_map[hk] = dict(sum=0, cnt=0)
            heat_map[hk]['sum'] += pdt1
            heat_map[hk]['cnt'] += 1

        # Drill (3 months)
        if date_str and lu_raw:
            try:
                from dateutil import parser as dparser
                d = dparser.parse(lu_raw, dayfirst=False)
                if d >= cutoff:
                    if team_id not in drill_map:
                        drill_map[team_id] = {}
                    if date_str not in drill_map[team_id]:
                        drill_map[team_id][date_str] = []
                    if len(drill_map[team_id][date_str]) < 30:
                        drill_map[team_id][date_str].append(dict(
                            tkt=g(row,'Ticket'),
                            type='Ticket' if is_ticket else 'Non-Ticket',
                            sla=g(row,'SLA'),
                            subj=g(row,'Subject')[:80],
                            que=g(row,'Que'),
                            travel=fmt_time(g(row,'เวลาเดินทาง')),
                            start=fmt_time(g(row,'เวลาเริ่มซ่อม')),
                            hold=fmt_time(g(row,'Hold')),
                            linkup=fmt_time(g(row,'Link Up')),
                            status=g(row,'Status Team'),
                            holdCause=g(row,'สาเหตุการ Hold'),
                            log=g(row,'Update Log')[:150],
                            cause1=g(row,'สาเหตุ 1'),
                            fix1=g(row,'วิธีแก้ไข')
                        ))
            except:
                pass

    # ── Build ts[] ────────────────────────────────────────────
    sorted_months = sorted(months)
    ml = {}
    for m in sorted_months:
        parts = m.split('-')
        ml[m] = f'{thai_months[int(parts[1])]} {parts[0][2:]}'

    ts = []
    rank_data = []
    for tm in team_map.values():
        if tm['cnt'] == 0:
            continue
        p1   = round(tm['sp1'] / tm['cnt'], 2)
        p2   = round(tm['sp2'] / tm['cnt'], 2)
        base = CM_BASE if tm['type'] == 'CM' else OFC_BASE
        vs1  = round(p1 - base, 2)
        days = len(tm['days'])
        st   = 'above' if vs1 >= 0 else ('below' if vs1 < -0.5 else 'near')

        ts.append(dict(
            id=tm['id'], type=tm['type'], reg=tm['reg'], prov=tm['prov'], pn=tm['prov'],
            p1=p1, p2=p2, tot1=tm['cnt'], tot2=tm['cnt'],
            h=round(tm['sh'] / tm['cnt'], 2), days=days,
            max1=round(tm['max1'],2), max2=round(tm['max2'],2),
            base=base, vs1=vs1, vs2=vs1, st=st,
            tkt=tm['tkt'], non=tm['non']
        ))

        rd = dict(id=tm['id'], type=tm['type'], reg=tm['reg'], prov=tm['prov'],
                  p1_avg=p1, p2_avg=p2, wd_avg=days)
        for m in sorted_months:
            md = tm['mth'].get(m)
            rd[f'p1_{m}'] = round(md['sp']/md['cnt'],2) if md else 0
            rd[f'p2_{m}'] = rd[f'p1_{m}']
            rd[f'wd_{m}'] = len(md['days']) if md else 0
        rank_data.append(rd)

    # Trend
    tr_map = {}
    for t in ts:
        for m in sorted_months:
            md = team_map[t['id']]['mth'].get(m)
            if not md or md['cnt'] == 0:
                continue
            k = f"{m}||{t['reg']}||{t['type']}"
            if k not in tr_map:
                tr_map[k] = dict(sum=0, cnt=0)
            tr_map[k]['sum'] += md['sp'] / md['cnt']
            tr_map[k]['cnt'] += 1
    tr = []
    for k, v in tr_map.items():
        m, reg, tp = k.split('||')
        avg = round(v['sum']/v['cnt'], 2)
        tr.append(dict(m=m, reg=reg, type=tp, avg=avg, avg_p1=avg))

    heat = []
    for k, v in heat_map.items():
        m, pv = k.split('||')
        heat.append(dict(m=m, pv=pv, avg=round(v['sum']/v['cnt'],2), tot=v['cnt']))

    nor1 = list(set(t['prov'] for t in ts if t['reg'] == 'NOR1'))

    result = dict(
        ts=ts, tr=tr, heat=heat, wk=[],
        prov=prov_names, nor1=nor1,
        months=sorted_months, ml=ml, sum={},
        gstats=dict(
            total_tkt=sum(t['tkt'] for t in ts),
            total_non=sum(t['non'] for t in ts),
            total_rows=sum(t['tkt']+t['non'] for t in ts)
        ),
        rankData=rank_data,
        boundary=[], homeCoords={},
        drill=drill_map,
        slaData={},
        cached_at=datetime.now().isoformat()
    )

    elapsed = round(time.time() - t0, 1)
    log.info(f'Done: {len(ts)} teams, {len(sorted_months)} months, {elapsed}s')
    return result

# ── Cache builder ─────────────────────────────────────────────
def rebuild_cache():
    global _cache, _building
    if _building:
        log.info('Already building, skip')
        return
    _building = True
    try:
        data = build_data()
        with _cache_lock:
            _cache = data
        log.info('Cache updated')
    except Exception as e:
        log.error(f'rebuild_cache error: {e}')
    finally:
        _building = False

# ── Routes ────────────────────────────────────────────────────
@app.route('/api/dashboard')
def api_dashboard():
    with _cache_lock:
        if _cache is None:
            return jsonify({'error': 'Cache building, retry in 60s'}), 503
        return jsonify(_cache)

@app.route('/api/status')
def api_status():
    with _cache_lock:
        if _cache is None:
            return jsonify({'status': 'building'})
        return jsonify(dict(
            status='ready',
            teams=len(_cache.get('ts', [])),
            months=_cache.get('months', []),
            tickets=_cache.get('gstats', {}).get('total_tkt', 0),
            cached_at=_cache.get('cached_at')
        ))

@app.route('/api/rebuild', methods=['POST'])
def api_rebuild():
    t = threading.Thread(target=rebuild_cache, daemon=True)
    t.start()
    return jsonify({'status': 'rebuilding'})

@app.route('/')
def index():
    return '<h3>NOC Dashboard API</h3><a href="/api/status">/api/status</a>'

# ── Startup ───────────────────────────────────────────────────
def start():
    # Build cache ตอนเริ่มต้น
    t = threading.Thread(target=rebuild_cache, daemon=True)
    t.start()

    # Schedule rebuild ทุก 6 ชั่วโมง
    scheduler = BackgroundScheduler()
    scheduler.add_job(rebuild_cache, 'interval', hours=REBUILD_HOURS)
    scheduler.start()

start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
