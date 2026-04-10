import os, json, logging, threading, time, re, math
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template_string
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

DASHBOARD_HTML = r"""
<!doctype html>
<html lang="th">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>NOC Productivity Dashboard</title>
  <style>
    :root{
      --bg:#0b1220;
      --panel:#121a2b;
      --panel2:#172033;
      --border:#263247;
      --text:#e8eefc;
      --muted:#9fb0d0;
      --green:#2ecc71;
      --amber:#f1c40f;
      --red:#ff6b6b;
      --blue:#62a8ff;
      --chip:#1d2940;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family:Arial,Helvetica,sans-serif;
      background:linear-gradient(180deg,#09111d,#0c1424);
      color:var(--text);
    }
    .wrap{max-width:1400px;margin:0 auto;padding:20px}
    .topbar{
      display:flex;justify-content:space-between;align-items:center;gap:12px;
      flex-wrap:wrap;margin-bottom:18px;
    }
    .title{font-size:30px;font-weight:700}
    .sub{color:var(--muted);font-size:13px;margin-top:6px}
    .toolbar{display:flex;gap:8px;flex-wrap:wrap}
    input,select,button{
      background:var(--panel);
      color:var(--text);
      border:1px solid var(--border);
      border-radius:10px;
      padding:10px 12px;
      font-size:14px;
    }
    button{cursor:pointer}
    .grid{
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
      gap:14px;
      margin-bottom:18px;
    }
    .card{
      background:var(--panel);
      border:1px solid var(--border);
      border-radius:16px;
      padding:16px;
      box-shadow:0 8px 24px rgba(0,0,0,.18);
    }
    .label{
      font-size:12px;
      text-transform:uppercase;
      color:var(--muted);
      margin-bottom:8px;
      letter-spacing:.4px;
    }
    .value{font-size:34px;font-weight:700}
    .split{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:14px;
      margin-bottom:18px;
    }
    .section{
      background:var(--panel);
      border:1px solid var(--border);
      border-radius:16px;
      padding:16px;
    }
    .section h3{margin:0 0 14px 0;font-size:18px}
    .table-wrap{
      overflow:auto;
      border:1px solid var(--border);
      border-radius:12px;
      max-height:520px;
      background:var(--panel2);
    }
    table{
      width:100%;
      border-collapse:collapse;
      font-size:13px;
    }
    th,td{
      padding:10px 10px;
      border-bottom:1px solid var(--border);
      text-align:left;
      vertical-align:top;
      white-space:nowrap;
    }
    th{
      position:sticky;
      top:0;
      background:#182235;
      color:var(--muted);
      z-index:2;
    }
    tr:hover td{background:#162033}
    .pill{
      display:inline-block;
      padding:4px 10px;
      border-radius:999px;
      background:var(--chip);
      border:1px solid var(--border);
      font-size:12px;
    }
    .status-above{color:var(--green)}
    .status-near{color:var(--amber)}
    .status-below{color:var(--red)}
    .muted{color:var(--muted)}
    .loading{
      padding:24px;
      text-align:center;
      color:var(--muted);
    }
    .footer{
      margin-top:12px;
      color:var(--muted);
      font-size:12px;
    }
    .chips{
      display:flex;gap:8px;flex-wrap:wrap;margin-top:8px;
    }
    .chip{
      background:var(--chip);
      border:1px solid var(--border);
      border-radius:999px;
      padding:5px 10px;
      font-size:12px;
      color:var(--muted);
    }
    @media (max-width: 900px){
      .split{grid-template-columns:1fr}
      .title{font-size:24px}
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div>
        <div class="title">NOC Productivity Dashboard</div>
        <div class="sub" id="lastUpdate">กำลังโหลดข้อมูล...</div>
        <div class="chips" id="monthChips"></div>
      </div>
      <div class="toolbar">
        <input id="searchBox" placeholder="ค้นหา Team ID / จังหวัด / รหัสทีม">
        <select id="regionFilter">
          <option value="">ทุก Region</option>
        </select>
        <select id="typeFilter">
          <option value="">ทุก Type</option>
        </select>
        <select id="provinceFilter">
          <option value="">ทุก Province</option>
        </select>
        <button onclick="loadDashboard()">Refresh</button>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">Total Teams</div>
        <div class="value" id="kpiTeams">-</div>
      </div>
      <div class="card">
        <div class="label">Total Tickets</div>
        <div class="value" id="kpiTickets">-</div>
      </div>
      <div class="card">
        <div class="label">Total Non-Tickets</div>
        <div class="value" id="kpiNon">-</div>
      </div>
      <div class="card">
        <div class="label">Months</div>
        <div class="value" id="kpiMonths">-</div>
      </div>
    </div>

    <div class="split">
      <div class="section">
        <h3>สรุปตาม Region</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Region</th>
                <th>Teams</th>
                <th>Avg PDT1</th>
                <th>Avg PDT2</th>
              </tr>
            </thead>
            <tbody id="regionSummary">
              <tr><td colspan="4" class="loading">กำลังโหลด...</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="section">
        <h3>สรุปตาม Type</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Type</th>
                <th>Teams</th>
                <th>Avg PDT1</th>
                <th>Avg PDT2</th>
              </tr>
            </thead>
            <tbody id="typeSummary">
              <tr><td colspan="4" class="loading">กำลังโหลด...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="section">
      <h3>ตารางทีม</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Team ID</th>
              <th>Type</th>
              <th>Region</th>
              <th>Province</th>
              <th>จังหวัด</th>
              <th>PDT1</th>
              <th>PDT2</th>
              <th>Base</th>
              <th>vs Base</th>
              <th>Hours</th>
              <th>Days</th>
              <th>Tickets</th>
              <th>Non</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="teamTable">
            <tr><td colspan="14" class="loading">กำลังโหลดข้อมูล...</td></tr>
          </tbody>
        </table>
      </div>
      <div class="footer">หน้าเว็บนี้ดึงข้อมูลจาก <code>/api/dashboard</code> อัตโนมัติ</div>
    </div>
  </div>

  <script>
    let RAW = null;
    let FILTERED = [];

    function fmtNum(v){
      if (v === null || v === undefined || v === '') return '-';
      const n = Number(v);
      return Number.isFinite(n) ? n.toLocaleString('th-TH', {maximumFractionDigits: 2}) : v;
    }

    function esc(s){
      return String(s ?? '')
        .replace(/&/g,'&amp;')
        .replace(/</g,'&lt;')
        .replace(/>/g,'&gt;')
        .replace(/"/g,'&quot;')
        .replace(/'/g,'&#39;');
    }

    function fillFilters(rows){
      const regionSel = document.getElementById('regionFilter');
      const typeSel = document.getElementById('typeFilter');
      const provSel = document.getElementById('provinceFilter');

      const regions = [...new Set(rows.map(x => x.reg).filter(Boolean))].sort();
      const types = [...new Set(rows.map(x => x.type).filter(Boolean))].sort();
      const provinces = [...new Set(rows.map(x => x.prov).filter(Boolean))].sort();

      const curR = regionSel.value;
      const curT = typeSel.value;
      const curP = provSel.value;

      regionSel.innerHTML = '<option value="">ทุก Region</option>' +
        regions.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join('');
      typeSel.innerHTML = '<option value="">ทุก Type</option>' +
        types.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join('');
      provSel.innerHTML = '<option value="">ทุก Province</option>' +
        provinces.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join('');

      regionSel.value = regions.includes(curR) ? curR : '';
      typeSel.value = types.includes(curT) ? curT : '';
      provSel.value = provinces.includes(curP) ? curP : '';
    }

    function buildSummary(rows){
      const byRegion = {};
      const byType = {};

      rows.forEach(r => {
        if (!byRegion[r.reg]) byRegion[r.reg] = {cnt:0, p1:0, p2:0};
        if (!byType[r.type]) byType[r.type] = {cnt:0, p1:0, p2:0};

        byRegion[r.reg].cnt++;
        byRegion[r.reg].p1 += Number(r.p1 || 0);
        byRegion[r.reg].p2 += Number(r.p2 || 0);

        byType[r.type].cnt++;
        byType[r.type].p1 += Number(r.p1 || 0);
        byType[r.type].p2 += Number(r.p2 || 0);
      });

      document.getElementById('regionSummary').innerHTML =
        Object.entries(byRegion).sort().map(([k,v]) => `
          <tr>
            <td>${esc(k)}</td>
            <td>${fmtNum(v.cnt)}</td>
            <td>${fmtNum(v.p1 / v.cnt)}</td>
            <td>${fmtNum(v.p2 / v.cnt)}</td>
          </tr>
        `).join('') || '<tr><td colspan="4" class="loading">ไม่มีข้อมูล</td></tr>';

      document.getElementById('typeSummary').innerHTML =
        Object.entries(byType).sort().map(([k,v]) => `
          <tr>
            <td>${esc(k)}</td>
            <td>${fmtNum(v.cnt)}</td>
            <td>${fmtNum(v.p1 / v.cnt)}</td>
            <td>${fmtNum(v.p2 / v.cnt)}</td>
          </tr>
        `).join('') || '<tr><td colspan="4" class="loading">ไม่มีข้อมูล</td></tr>';
    }

    function renderTable(rows){
      const tbody = document.getElementById('teamTable');
      if (!rows.length){
        tbody.innerHTML = '<tr><td colspan="14" class="loading">ไม่พบข้อมูล</td></tr>';
        return;
      }

      tbody.innerHTML = rows.map(r => {
        const statusCls = r.st === 'above' ? 'status-above' : (r.st === 'near' ? 'status-near' : 'status-below');
        return `
          <tr>
            <td>${esc(r.id)}</td>
            <td>${esc(r.type)}</td>
            <td>${esc(r.reg)}</td>
            <td>${esc(r.prov)}</td>
            <td>${esc(r.pn || '')}</td>
            <td>${fmtNum(r.p1)}</td>
            <td>${fmtNum(r.p2)}</td>
            <td>${fmtNum(r.base)}</td>
            <td>${fmtNum(r.vs1)}</td>
            <td>${fmtNum(r.h)}</td>
            <td>${fmtNum(r.days)}</td>
            <td>${fmtNum(r.tkt)}</td>
            <td>${fmtNum(r.non)}</td>
            <td><span class="pill ${statusCls}">${esc(r.st)}</span></td>
          </tr>
        `;
      }).join('');
    }

    function applyFilters(){
      if (!RAW) return;

      const q = document.getElementById('searchBox').value.trim().toLowerCase();
      const region = document.getElementById('regionFilter').value;
      const type = document.getElementById('typeFilter').value;
      const province = document.getElementById('provinceFilter').value;

      FILTERED = (RAW.ts || []).filter(r => {
        const hitRegion = !region || r.reg === region;
        const hitType = !type || r.type === type;
        const hitProvince = !province || r.prov === province;

        const hay = [
          r.id, r.type, r.reg, r.prov, r.pn, r.st
        ].join(' ').toLowerCase();

        const hitQ = !q || hay.includes(q);
        return hitRegion && hitType && hitProvince && hitQ;
      });

      document.getElementById('kpiTeams').textContent = fmtNum(FILTERED.length);
      buildSummary(FILTERED);
      renderTable(FILTERED);
    }

    async function loadDashboard(){
      try{
        document.getElementById('lastUpdate').textContent = 'กำลังโหลดข้อมูล...';
        const res = await fetch('/api/dashboard', {cache:'no-store'});
        if (!res.ok) throw new Error('โหลดข้อมูลไม่สำเร็จ');
        RAW = await res.json();

        document.getElementById('kpiTickets').textContent = fmtNum(RAW.gstats?.total_tkt || 0);
        document.getElementById('kpiNon').textContent = fmtNum(RAW.gstats?.total_non || 0);
        document.getElementById('kpiMonths').textContent = fmtNum((RAW.months || []).length);

        document.getElementById('lastUpdate').textContent =
          'อัปเดตล่าสุด: ' + (RAW.cached_at || '-');

        document.getElementById('monthChips').innerHTML =
          (RAW.months || []).map(m => `<span class="chip">${esc(RAW.ml?.[m] || m)}</span>`).join('');

        fillFilters(RAW.ts || []);
        applyFilters();
      }catch(err){
        document.getElementById('lastUpdate').textContent = 'โหลดข้อมูลไม่สำเร็จ';
        document.getElementById('teamTable').innerHTML =
          `<tr><td colspan="14" class="loading">เกิดข้อผิดพลาด: ${esc(err.message)}</td></tr>`;
      }
    }

    document.addEventListener('DOMContentLoaded', () => {
      document.getElementById('searchBox').addEventListener('input', applyFilters);
      document.getElementById('regionFilter').addEventListener('change', applyFilters);
      document.getElementById('typeFilter').addEventListener('change', applyFilters);
      document.getElementById('provinceFilter').addEventListener('change', applyFilters);
      loadDashboard();
      setInterval(loadDashboard, 60000);
    });
  </script>
</body>
</html>
"""

def get_client():
    info  = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
    creds = Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    return gspread.authorize(creds)

BOUNDARY_SHEET = 'team_boudary'

def parse_coord(v):
    if not v:
        return None
    m = re.match(r'([-\d.]+),\s*([-\d.]+)', str(v).strip())
    return (float(m.group(1)), float(m.group(2))) if m else None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def find_home_coords(coords_list, radius_km=5):
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
                len(coords_list)
            )
    return best

def build_boundary(gc):
    """ดึง boundary data จาก team_boundary sheet"""
    try:
        ws = gc.open_by_key(SHEET_ID).worksheet(BOUNDARY_SHEET)
        rows = ws.get_all_values()
        if not rows:
            return []
        headers = [h.strip() for h in rows[0]]
        col = {h:i for i, h in enumerate(headers) if h}

        def g(row, name):
            i = col.get(name)
            return str(row[i]).strip() if i is not None and i < len(row) else ''

        boundary = []
        for row in rows[1:]:
            tid = g(row, 'Team ID')
            if not tid or tid == 'nan':
                continue
            type_team = g(row, 'Type Team')
            prov_code = g(row, 'Province')
            prov_name = g(row, 'Province1')
            home = g(row, 'อำเภอ home base')
            group = g(row, 'Group District')
            reg = 'NOR1' if prov_code in NOR1 else 'NOR2'
            resp = []
            for i in range(1, 10):
                v = g(row, f'อำเภอที่รับผิดชอบที่ {i}')
                if v and v != 'nan':
                    resp.append(v)
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
    """D/M/YYYY HH:MM (พ.ศ.) → datetime ค.ศ."""
    if not v:
        return None
    s = str(v).strip()
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})', s)
    if not m:
        return None
    d, mo, y, h, mi = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5))
    if y > 2100:
        y -= 543
    try:
        return datetime(y, mo, d, h, mi)
    except:
        return None

def to_by_month(dt):
    """datetime ค.ศ. → พ.ศ. month string เช่น 2569-01"""
    if not dt:
        return None
    return f'{dt.year+543}-{dt.month:02d}'

def to_by_date(dt):
    """datetime ค.ศ. → พ.ศ. date string เช่น 2569-01-15"""
    if not dt:
        return None
    return f'{dt.year+543}-{dt.month:02d}-{dt.day:02d}'

def fmt_time(v):
    if not v:
        return ''
    m = re.search(r'(\d{1,2}:\d{2})', str(v))
    return m.group(1) if m else str(v).strip()

def is_valid_month(month_str):
    return month_str and month_str.startswith(VALID_YEAR + '-')

def build_data():
    log.info('Building dashboard data...')
    t0 = time.time()

    gc = get_client()
    ws = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    rows = ws.get_all_values()
    if not rows:
        raise RuntimeError('Sheet is empty')

    headers = [' '.join(h.split()) for h in rows[0]]
    col = {h: i for i, h in enumerate(headers) if h}

    def fc(*names):
        for n in names:
            if n in col:
                return col[n]
            for k, v in col.items():
                if k.lower() == n.lower():
                    return v
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
    }

    def g(row, key):
        i = C.get(key)
        return str(row[i]).strip() if i is not None and i < len(row) else ''

    teams = {}
    months = set()
    drill = {}
    cutoff = datetime.now() - timedelta(days=90)

    for row in rows[1:]:
        team_id   = g(row, 'team_id')
        type_team = g(row, 'type_team')
        prov_full = g(row, 'province')
        if not team_id or type_team not in ('CM', 'OFC'):
            continue
        if team_id in EXCLUDE:
            continue

        prov = PROV_MAP.get(prov_full, '')
        reg  = 'NOR1' if prov in NOR1 else 'NOR2'

        dt_linkup = parse_dt(g(row, 'linkup'))
        dt_travel = parse_dt(g(row, 'travel'))
        dt_hold   = parse_dt(g(row, 'hold'))

        has_lu   = dt_linkup is not None
        has_hold = dt_hold is not None

        dt_date   = dt_linkup or dt_travel
        month_str = to_by_month(dt_date)
        date_str  = to_by_date(dt_date)

        row_valid = is_valid_month(month_str)
        if month_str and row_valid:
            months.add(month_str)

        tkt_val   = g(row, 'ticket')
        is_ticket = tkt_val.startswith('TT') or tkt_val.startswith('INC')

        coord_raw = g(row, 'update_pikat')
        last_ts = dt_linkup or dt_hold

        if team_id not in teams:
            teams[team_id] = dict(
                type=type_team, prov=prov, reg=reg,
                pdt1_dates={}, pdt2_dates={},
                all_dates=set(),
                day_first_travel={},
                day_last_ts={},
                tkt=0, non=0, monthly={},
                coords=[]
            )
        tm = teams[team_id]

        if coord_raw and row_valid:
            c = parse_coord(coord_raw)
            if c:
                tm['coords'].append(c)

        if row_valid:
            if is_ticket:
                tm['tkt'] += 1
            else:
                tm['non'] += 1

        if row_valid and date_str:
            tm['all_dates'].add(date_str)
            if dt_travel:
                prev = tm['day_first_travel'].get(date_str)
                if prev is None or dt_travel < prev:
                    tm['day_first_travel'][date_str] = dt_travel
            if last_ts:
                prev = tm['day_last_ts'].get(date_str)
                if prev is None or last_ts > prev:
                    tm['day_last_ts'][date_str] = last_ts
            if has_lu:
                tm['pdt1_dates'][date_str] = tm['pdt1_dates'].get(date_str, 0) + 1
            if has_lu or has_hold:
                tm['pdt2_dates'][date_str] = tm['pdt2_dates'].get(date_str, 0) + 1
            if month_str:
                if month_str not in tm['monthly']:
                    tm['monthly'][month_str] = {'p1d': {}, 'p2d': {}, 'dates': set()}
                mm = tm['monthly'][month_str]
                mm['dates'].add(date_str)
                if has_lu:
                    mm['p1d'][date_str] = mm['p1d'].get(date_str, 0) + 1
                if has_lu or has_hold:
                    mm['p2d'][date_str] = mm['p2d'].get(date_str, 0) + 1

        if row_valid and dt_linkup and dt_linkup >= cutoff and date_str:
            if team_id not in drill:
                drill[team_id] = {}
            if date_str not in drill[team_id]:
                drill[team_id][date_str] = []
            if len(drill[team_id][date_str]) < 30:
                drill[team_id][date_str].append([
                    tkt_val,
                    g(row, 'sla'),
                    g(row, 'subject')[:80],
                    g(row, 'que'),
                    fmt_time(g(row, 'travel')),
                    fmt_time(g(row, 'start')),
                    fmt_time(g(row, 'hold')),
                    fmt_time(g(row, 'linkup')),
                    g(row, 'status'),
                    g(row, 'holdcause'),
                    g(row, 'log')[:150],
                    g(row, 'cause1'),
                    g(row, 'fix1'),
                    '',
                    'Ticket' if is_ticket else 'Non-Ticket',
                    1 if (dt_linkup is not None) else 0,
                    1 if (dt_linkup is not None or dt_hold is not None) else 0,
                    0,
                    month_str or '',
                ])

    log.info(f'Parsed {len(teams)} teams, months={sorted(months)}')

    sorted_months = sorted(months)
    ml = {}
    for m in sorted_months:
        p = m.split('-')
        ml[m] = f'{THAI_MONTHS[int(p[1])]} {p[0][2:]}'

    ts = []
    rank_data = []
    for tid, tm in teams.items():
        days = len(tm['all_dates'])
        if days == 0:
            continue
        tot1 = sum(tm['pdt1_dates'].values())
        tot2 = sum(tm['pdt2_dates'].values())
        p1   = round(tot1 / days, 2)
        p2   = round(tot2 / days, 2)
        base = CM_BASE if tm['type'] == 'CM' else OFC_BASE
        vs1  = round(p1 - base, 2)
        vs2  = round(p2 - base, 2)
        st   = 'above' if vs1 >= 0 else ('below' if vs1 < -0.5 else 'near')
        max1 = max(tm['pdt1_dates'].values()) if tm['pdt1_dates'] else 0
        max2 = max(tm['pdt2_dates'].values()) if tm['pdt2_dates'] else 0
        daily_h_vals = [
            round((tm['day_last_ts'][d] - tm['day_first_travel'][d]).total_seconds() / 3600, 2)
            for d in tm['all_dates']
            if d in tm['day_first_travel']
            and d in tm['day_last_ts']
            and 0 < (tm['day_last_ts'][d] - tm['day_first_travel'][d]).total_seconds() / 3600 < 24
        ]
        h = round(sum(daily_h_vals) / len(daily_h_vals), 2) if daily_h_vals else 0

        ts.append(dict(
            id=tid, type=tm['type'], reg=tm['reg'], prov=tm['prov'],
            pn=PROV_THAI.get(tm['prov'], tm['prov']),
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
                mp1 = round(sum(mm['p1d'].values()) / md, 2) if mm['p1d'] else 0
                mp2 = round(sum(mm['p2d'].values()) / md, 2) if mm['p2d'] else 0
            else:
                mp1 = mp2 = md = 0
            rd[f'p1_{m}'] = mp1
            rd[f'p2_{m}'] = mp2
            rd[f'wd_{m}'] = md
        rank_data.append(rd)

    tr_map = {}
    for t in ts:
        tm = teams[t['id']]
        for m in sorted_months:
            mm = tm['monthly'].get(m)
            if not mm or not mm['dates']:
                continue
            md   = len(mm['dates'])
            avg1 = round(sum(mm['p1d'].values()) / md, 2) if mm['p1d'] else 0
            avg2 = round(sum(mm['p2d'].values()) / md, 2) if mm['p2d'] else 0
            k = f"{m}||{t['reg']}||{t['type']}"
            if k not in tr_map:
                tr_map[k] = {'s1': 0, 's2': 0, 'cnt': 0}
            tr_map[k]['s1'] += avg1
            tr_map[k]['s2'] += avg2
            tr_map[k]['cnt'] += 1

    tr = [dict(
            m=k.split('||')[0],
            reg=k.split('||')[1],
            type=k.split('||')[2],
            avg=round(v['s2'] / v['cnt'], 2),
            avg_p1=round(v['s1'] / v['cnt'], 2)
         ) for k, v in tr_map.items()]

    heat_map = {}
    for t in ts:
        tm = teams[t['id']]
        for m, mm in tm['monthly'].items():
            if not mm['dates']:
                continue
            md  = len(mm['dates'])
            avg = round(sum(mm['p1d'].values()) / md, 2) if mm['p1d'] else 0
            hk  = f"{m}||{t['prov']}"
            if hk not in heat_map:
                heat_map[hk] = {'sum': 0, 'cnt': 0}
            heat_map[hk]['sum'] += avg
            heat_map[hk]['cnt'] += 1

    heat = [dict(
            m=k.split('||')[0],
            pv=k.split('||')[1],
            avg=round(v['sum'] / v['cnt'], 2),
            tot=v['cnt']
        ) for k, v in heat_map.items()]

    prov_names = {p: PROV_THAI.get(p, p) for p in set(t['prov'] for t in ts)}
    nor1_list  = list(set(t['prov'] for t in ts if t['reg'] == 'NOR1'))

    home_coords = {}
    for tid, tm in teams.items():
        result = find_home_coords(tm['coords'])
        if result:
            home_coords[tid] = {
                'lat': result[0], 'lon': result[1],
                'count': result[2], 'total': result[3]
            }
    log.info(f'homeCoords: {len(home_coords)} teams')

    boundary = build_boundary(gc)

    elapsed = round(time.time() - t0, 1)
    log.info(f'Done: {len(ts)} teams, {len(sorted_months)} months, {elapsed}s')
    sample = [(t['id'], t['p1'], t['vs1']) for t in sorted(ts, key=lambda x: x['p1'])[:3]]
    log.info(f'Sample lowest p1: {sample}')
    log.info(f'gstats: tkt={sum(t["tkt"] for t in ts)} non={sum(t["non"] for t in ts)}')

    return dict(
        ts=ts, tr=tr, heat=heat, wk=[],
        prov=prov_names, nor1=nor1_list,
        months=sorted_months, ml=ml, sum={},
        gstats=dict(
            total_tkt=sum(t['tkt'] for t in ts),
            total_non=sum(t['non'] for t in ts),
            total_rows=sum(t['tkt'] + t['non'] for t in ts)
        ),
        rankData=rank_data, boundary=boundary, homeCoords=home_coords,
        drill=drill, slaData={},
        cached_at=datetime.now().isoformat()
    )

def rebuild_cache():
    global _cache, _building
    if _building:
        return
    _building = True
    try:
        data = build_data()
        with _cache_lock:
            _cache = data
        log.info('Cache updated ✓')
    except Exception as e:
        log.error(f'rebuild_cache error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        _building = False

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
    threading.Thread(target=rebuild_cache, daemon=True).start()
    return jsonify({'status': 'rebuilding'})

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML)

def start():
    threading.Thread(target=rebuild_cache, daemon=True).start()
    s = BackgroundScheduler()
    s.add_job(rebuild_cache, 'interval', hours=REBUILD_HOURS)
    s.start()

start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
