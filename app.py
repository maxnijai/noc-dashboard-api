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
EXCLUDE       = {'PS_CMI_ofc_011','PS_CMI_ofc_012'}
VALID_YEAR    = '2569'   # กรองเฉพาะปี พ.ศ. นี้

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
app = Flask(__name__)
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

def parse_dt(v):
    """D/M/YYYY HH:MM (พ.ศ.) → datetime ค.ศ."""
    if not v: return None
    s = str(v).strip()
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})', s)
    if not m: return None
    d,mo,y,h,mi = int(m.group(1)),int(m.group(2)),int(m.group(3)),int(m.group(4)),int(m.group(5))
    if y > 2100: y -= 543   # พ.ศ. → ค.ศ.
    try: return datetime(y, mo, d, h, mi)
    except: return None

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
        'team_id':   fc('Team ID'),
        'type_team': fc('Type Team'),
        'province':  fc('Province'),
        'ticket':    fc('Ticket'),
        'sla':       fc('SLA'),
        'subject':   fc('Subject'),
        'que':       fc('Que'),
        'travel':    fc('เวลาเดินทาง'),
        'start':     fc('เวลาเริ่มซ่อม'),
        'hold':      fc('Hold'),
        'linkup':    fc('Link Up'),
        'status':    fc('Status Team'),
        'holdcause': fc('สาเหตุการ Hold'),
        'log':       fc('Update Log'),
        'cause1':    fc('สาเหตุ 1'),
        'fix1':      fc('วิธีแก้ไข'),
    }

    def g(row, key):
        i = C.get(key)
        return str(row[i]).strip() if i is not None and i < len(row) else ''

    teams  = {}
    months = set()
    drill  = {}
    cutoff = datetime.now() - timedelta(days=90)

    for row in rows[1:]:
        team_id   = g(row,'team_id')
        type_team = g(row,'type_team')
        prov_full = g(row,'province')
        if not team_id or type_team not in ('CM','OFC'): continue
        if team_id in EXCLUDE: continue

        prov = PROV_MAP.get(prov_full,'')
        reg  = 'NOR1' if prov in NOR1 else 'NOR2'

        dt_linkup = parse_dt(g(row,'linkup'))
        dt_travel = parse_dt(g(row,'travel'))
        dt_hold   = parse_dt(g(row,'hold'))

        has_lu   = dt_linkup is not None
        has_hold = dt_hold   is not None

        # date จาก Link Up ?? เวลาเดินทาง
        dt_date   = dt_linkup or dt_travel
        month_str = to_by_month(dt_date)
        date_str  = to_by_date(dt_date)

        # กรองเฉพาะ valid year (2569) — ตัด outlier
        row_valid = is_valid_month(month_str)
        if month_str: months.add(month_str) if row_valid else None

        # work_hrs: Link Up - เวลาเดินทาง
        work_hrs = None
        if dt_linkup and dt_travel:
            wh = (dt_linkup - dt_travel).total_seconds() / 3600
            if 0 < wh < 24: work_hrs = wh

        # is_ticket: TT หรือ INC (ตาม v17 label "TT + INC pattern")
        # แต่นับเฉพาะ row ที่ year valid
        tkt_val   = g(row,'ticket')
        is_ticket = tkt_val.startswith('TT') or tkt_val.startswith('INC')

        if team_id not in teams:
            teams[team_id] = dict(
                type=type_team, prov=prov, reg=reg,
                pdt1_dates={}, pdt2_dates={},
                all_dates=set(), work_hrs_daily={},
                tkt=0, non=0, monthly={}
            )
        tm = teams[team_id]

        # นับ tkt/non เฉพาะ valid year rows
        if row_valid:
            if is_ticket: tm['tkt'] += 1
            else:         tm['non'] += 1

        # PDT counts เฉพาะ valid rows
        if row_valid and date_str:
            tm['all_dates'].add(date_str)
            if work_hrs:
                tm['work_hrs_daily'][date_str] = tm['work_hrs_daily'].get(date_str,0) + work_hrs
            if has_lu:
                tm['pdt1_dates'][date_str] = tm['pdt1_dates'].get(date_str,0) + 1
            if has_lu or has_hold:
                tm['pdt2_dates'][date_str] = tm['pdt2_dates'].get(date_str,0) + 1
            if month_str:
                if month_str not in tm['monthly']:
                    tm['monthly'][month_str] = {'p1d':{},'p2d':{},'dates':set()}
                mm = tm['monthly'][month_str]
                mm['dates'].add(date_str)
                if has_lu:
                    mm['p1d'][date_str] = mm['p1d'].get(date_str,0)+1
                if has_lu or has_hold:
                    mm['p2d'][date_str] = mm['p2d'].get(date_str,0)+1

        # drill (3 months, valid only)
        if row_valid and dt_linkup and dt_linkup >= cutoff and date_str:
            if team_id not in drill: drill[team_id] = {}
            if date_str not in drill[team_id]: drill[team_id][date_str] = []
            if len(drill[team_id][date_str]) < 30:
                drill[team_id][date_str].append(dict(
                    tkt=tkt_val, type='Ticket' if is_ticket else 'Non-Ticket',
                    sla=g(row,'sla'), subj=g(row,'subject')[:80], que=g(row,'que'),
                    travel=fmt_time(g(row,'travel')), start=fmt_time(g(row,'start')),
                    hold=fmt_time(g(row,'hold')),     linkup=fmt_time(g(row,'linkup')),
                    status=g(row,'status'), holdCause=g(row,'holdcause'),
                    log=g(row,'log')[:150], cause1=g(row,'cause1'), fix1=g(row,'fix1')
                ))

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
        daily_hrs = list(tm['work_hrs_daily'].values())
        h = round(sum(daily_hrs)/len(daily_hrs), 2) if daily_hrs else 0

        ts.append(dict(
            id=tid, type=tm['type'], reg=tm['reg'], prov=tm['prov'],
            pn=PROV_THAI.get(tm['prov'],tm['prov']),
            p1=p1, p2=p2, tot1=tot1, tot2=tot2,
            h=h, days=days, max1=max1, max2=max2,
            base=base, vs1=vs1, vs2=vs2, st=st, tkt=tm['tkt'], non=tm['non']
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
            if not mm['dates']: continue
            md  = len(mm['dates'])
            avg = round(sum(mm['p1d'].values())/md, 2) if mm['p1d'] else 0
            hk  = f"{m}||{t['prov']}"
            if hk not in heat_map: heat_map[hk] = {'sum':0,'cnt':0}
            heat_map[hk]['sum']+=avg; heat_map[hk]['cnt']+=1
    heat = [dict(m=k.split('||')[0], pv=k.split('||')[1],
                 avg=round(v['sum']/v['cnt'],2), tot=v['cnt'])
            for k,v in heat_map.items()]

    prov_names = {p:PROV_THAI.get(p,p) for p in set(t['prov'] for t in ts)}
    nor1_list  = list(set(t['prov'] for t in ts if t['reg']=='NOR1'))

    elapsed = round(time.time()-t0,1)
    log.info(f'Done: {len(ts)} teams, {len(sorted_months)} months, {elapsed}s')
    sample = [(t['id'],t['p1'],t['vs1']) for t in sorted(ts,key=lambda x:x['p1'])[:3]]
    log.info(f'Sample lowest p1: {sample}')
    log.info(f'gstats: tkt={sum(t["tkt"] for t in ts)} non={sum(t["non"] for t in ts)}')

    return dict(
        ts=ts, tr=tr, heat=heat, wk=[],
        prov=prov_names, nor1=nor1_list,
        months=sorted_months, ml=ml, sum={},
        gstats=dict(
            total_tkt=sum(t['tkt'] for t in ts),
            total_non=sum(t['non'] for t in ts),
            total_rows=sum(t['tkt']+t['non'] for t in ts)
        ),
        rankData=rank_data, boundary=[], homeCoords={},
        drill=drill, slaData={},
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
