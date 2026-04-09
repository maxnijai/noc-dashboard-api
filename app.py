import os, json, logging, threading, time, re
from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler

SHEET_ID      = '1_l5UAj1etjGgLCR4DSG6qDoK8c1unFnO6NVHVwvmbAU'
SHEET_NAME    = 'Sheet1'
CM_BASE       = 3
OFC_BASE      = 2
REBUILD_HOURS = 6

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

_cache = None
_cache_lock = threading.Lock()
_building   = False

def get_client():
    info  = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
    creds = Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    return gspread.authorize(creds)

# ── Date parser: D/M/YYYY HH:MM[:SS] (พ.ศ. หรือ ค.ศ.) ───────
def parse_dt(v):
    if not v: return None
    s = str(v).strip()
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})', s)
    if not m: return None
    d,mo,y,h,mi = int(m.group(1)),int(m.group(2)),int(m.group(3)),int(m.group(4)),int(m.group(5))
    if y > 2100: y -= 543
    try: return datetime(y, mo, d, h, mi)
    except: return None

def to_month(dt):
    if not dt: return ''
    return f'{dt.year+543}-{dt.month:02d}'

def to_date_str(dt):
    if not dt: return ''
    return f'{dt.year+543}-{dt.month:02d}-{dt.day:02d}'

def fmt_time(v):
    """แสดงเวลา HH:MM จาก datetime string"""
    if not v: return ''
    s = str(v).strip()
    m = re.search(r'(\d{1,2}:\d{2})', s)
    return m.group(1) if m else s

# ── Main builder ──────────────────────────────────────────────
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
                if k.lower() == n.lower(): return v
        return None

    C = {
        'team_id':    fc('Team ID'),
        'type_team':  fc('Type Team'),
        'region':     fc('Region'),
        'province':   fc('Province'),
        'ticket':     fc('Ticket'),
        'sla':        fc('SLA'),
        'subject':    fc('Subject'),
        'categories': fc('Categories'),
        'que':        fc('Que'),
        'travel':     fc('เวลาเดินทาง'),
        'start':      fc('เวลาเริ่มซ่อม'),
        'hold':       fc('Hold'),
        'linkup':     fc('Link Up'),
        'status':     fc('Status Team'),
        'holdcause':  fc('สาเหตุการ Hold'),
        'log':        fc('Update Log'),
        'cause1':     fc('สาเหตุ 1'),
        'fix1':       fc('วิธีแก้ไข'),
    }
    log.info(f'Cols: team_id={C["team_id"]} type_team={C["type_team"]} travel={C["travel"]} linkup={C["linkup"]}')

    def g(row, key):
        i = C.get(key)
        return str(row[i]).strip() if i is not None and i < len(row) else ''

    thai_months = ['','ม.ค.','ก.พ.','มี.ค.','เม.ย.','พ.ค.','มิ.ย.',
                   'ก.ค.','ส.ค.','ก.ย.','ต.ค.','พ.ย.','ธ.ค.']

    team_map = {}; heat_map = {}; months = set()
    prov_names = {}; drill_map = {}
    cutoff = datetime.now() - timedelta(days=90)
    ok = skip = 0

    for row in rows[1:]:
        team_id   = g(row,'team_id')
        type_team = g(row,'type_team')
        region    = g(row,'region')
        prov      = g(row,'province')
        if not team_id or type_team not in ('CM','OFC'):
            skip += 1; continue
        ok += 1
        if prov: prov_names[prov] = prov

        # parse datetime
        dt_travel = parse_dt(g(row,'travel'))
        dt_linkup = parse_dt(g(row,'linkup'))
        dt_hold   = parse_dt(g(row,'hold'))

        # ── PDT calculation ──────────────────────────────────
        # p1 = Link Up - เวลาเดินทาง (hours)
        # p2 = p1 + hold duration (hours)
        p1 = p2 = 0.0
        if dt_travel and dt_linkup:
            diff = (dt_linkup - dt_travel).total_seconds() / 3600
            if 0 < diff < 24:  # กรอง outlier
                p1 = round(diff, 2)
                p2 = p1
                if dt_hold and dt_hold < dt_linkup:
                    hold_dur = (dt_linkup - dt_hold).total_seconds() / 3600
                    if 0 < hold_dur < 24:
                        p2 = round(p1 + hold_dur, 2)

        # work hours = LinkUp - Travel (same as p1)
        work_hrs = p1

        month    = to_month(dt_linkup)
        date_str = to_date_str(dt_linkup)
        if month: months.add(month)

        is_ticket = bool(g(row,'ticket')) and g(row,'categories') != 'Non-Ticket'

        if team_id not in team_map:
            team_map[team_id] = dict(
                id=team_id, type=type_team, reg=region, prov=prov,
                sp1=0.0, sp2=0.0, sh=0.0, cnt=0,
                max1=0.0, max2=0.0, tkt=0, non=0,
                days=set(), mth={})
        tm = team_map[team_id]

        if is_ticket: tm['tkt'] += 1
        else:         tm['non'] += 1

        if p1 > 0:
            tm['sp1'] += p1; tm['sp2'] += p2; tm['cnt'] += 1
            tm['sh']  += work_hrs
            if p1 > tm['max1']: tm['max1'] = p1
            if p2 > tm['max2']: tm['max2'] = p2
        if date_str: tm['days'].add(date_str)
        if month and p1 > 0:
            if month not in tm['mth']:
                tm['mth'][month] = dict(sp=0.0, cnt=0, days=set())
            tm['mth'][month]['sp']  += p1
            tm['mth'][month]['cnt'] += 1
            if date_str: tm['mth'][month]['days'].add(date_str)

        if month and prov and p1 > 0:
            hk = f'{month}||{prov}'
            if hk not in heat_map: heat_map[hk] = dict(sum=0.0, cnt=0)
            heat_map[hk]['sum'] += p1; heat_map[hk]['cnt'] += 1

        if date_str and dt_linkup and dt_linkup >= cutoff:
            if team_id not in drill_map: drill_map[team_id] = {}
            if date_str not in drill_map[team_id]: drill_map[team_id][date_str] = []
            if len(drill_map[team_id][date_str]) < 30:
                drill_map[team_id][date_str].append(dict(
                    tkt=g(row,'ticket'), type='Ticket' if is_ticket else 'Non-Ticket',
                    sla=g(row,'sla'), subj=g(row,'subject')[:80], que=g(row,'que'),
                    travel=fmt_time(g(row,'travel')), start=fmt_time(g(row,'start')),
                    hold=fmt_time(g(row,'hold')),     linkup=fmt_time(g(row,'linkup')),
                    status=g(row,'status'), holdCause=g(row,'holdcause'),
                    log=g(row,'log')[:150], cause1=g(row,'cause1'), fix1=g(row,'fix1')
                ))

    log.info(f'Rows ok:{ok} skip:{skip} teams:{len(team_map)}')

    sorted_months = sorted(months)
    ml = {}
    for m in sorted_months:
        p = m.split('-')
        ml[m] = f'{thai_months[int(p[1])]} {p[0][2:]}'

    ts = []; rank_data = []
    for tm in team_map.values():
        if tm['cnt'] == 0: continue
        p1   = round(tm['sp1']/tm['cnt'], 2)
        p2   = round(tm['sp2']/tm['cnt'], 2)
        base = CM_BASE if tm['type']=='CM' else OFC_BASE
        vs1  = round(p1 - base, 2)
        days = len(tm['days'])
        st   = 'above' if vs1 >= 0 else ('below' if vs1 < -0.5 else 'near')
        ts.append(dict(
            id=tm['id'], type=tm['type'], reg=tm['reg'], prov=tm['prov'], pn=tm['prov'],
            p1=p1, p2=p2, tot1=tm['cnt'], tot2=tm['cnt'],
            h=round(tm['sh']/tm['cnt'], 2), days=days,
            max1=round(tm['max1'],2), max2=round(tm['max2'],2),
            base=base, vs1=vs1, vs2=vs1, st=st, tkt=tm['tkt'], non=tm['non']
        ))
        rd = dict(id=tm['id'], type=tm['type'], reg=tm['reg'], prov=tm['prov'],
                  p1_avg=p1, p2_avg=p2, wd_avg=days)
        for m in sorted_months:
            md = tm['mth'].get(m)
            rd[f'p1_{m}'] = round(md['sp']/md['cnt'],2) if md else 0
            rd[f'p2_{m}'] = rd[f'p1_{m}']
            rd[f'wd_{m}'] = len(md['days']) if md else 0
        rank_data.append(rd)

    tr_map = {}
    for t in ts:
        for m in sorted_months:
            md = team_map[t['id']]['mth'].get(m)
            if not md or md['cnt']==0: continue
            k = f"{m}||{t['reg']}||{t['type']}"
            if k not in tr_map: tr_map[k] = dict(sum=0.0, cnt=0)
            tr_map[k]['sum'] += md['sp']/md['cnt']; tr_map[k]['cnt'] += 1
    tr = [dict(m=k.split('||')[0], reg=k.split('||')[1], type=k.split('||')[2],
               avg=round(v['sum']/v['cnt'],2), avg_p1=round(v['sum']/v['cnt'],2))
          for k,v in tr_map.items()]

    heat = [dict(m=k.split('||')[0], pv=k.split('||')[1],
                 avg=round(v['sum']/v['cnt'],2), tot=v['cnt'])
            for k,v in heat_map.items()]

    nor1 = list(set(t['prov'] for t in ts if t['reg']=='NOR1'))
    elapsed = round(time.time()-t0, 1)
    log.info(f'Done: {len(ts)} teams, {len(sorted_months)} months, {elapsed}s')
    log.info(f'Sample PDT: {[(t["id"],t["p1"]) for t in sorted(ts, key=lambda x:x["p1"])[:3]]}')

    return dict(
        ts=ts, tr=tr, heat=heat, wk=[],
        prov=prov_names, nor1=nor1,
        months=sorted_months, ml=ml, sum={},
        gstats=dict(
            total_tkt=sum(t['tkt'] for t in ts),
            total_non=sum(t['non'] for t in ts),
            total_rows=sum(t['tkt']+t['non'] for t in ts)
        ),
        rankData=rank_data, boundary=[], homeCoords={},
        drill=drill_map, slaData={},
        cached_at=datetime.now().isoformat()
    )

def rebuild_cache():
    global _cache, _building
    if _building: return
    _building = True
    try:
        data = build_data()
        with _cache_lock: _cache = data
        log.info('Cache updated')
    except Exception as e:
        log.error(f'rebuild_cache error: {e}')
        import traceback; traceback.print_exc()
    finally:
        _building = False

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
            cached_at=_cache.get('cached_at')
        ))

@app.route('/api/rebuild', methods=['POST'])
def api_rebuild():
    threading.Thread(target=rebuild_cache, daemon=True).start()
    return jsonify({'status':'rebuilding'})

@app.route('/')
def index():
    return '<h3>NOC Dashboard API</h3><a href="/api/status">/api/status</a>'

def start():
    threading.Thread(target=rebuild_cache, daemon=True).start()
    s = BackgroundScheduler()
    s.add_job(rebuild_cache, 'interval', hours=REBUILD_HOURS)
    s.start()

start()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',5000)))
