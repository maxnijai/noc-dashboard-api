import os, json, time, base64, hmac, hashlib, re
from datetime import datetime
from flask import jsonify, request
import gspread

SHEET_ID = os.environ.get('SMART_DEFENSE_SHEET_ID', '1RBWr-lKva_XOqmcKwEE-E7hqIodbWWK1XHzuV8QJ-7Q')
TICKET_SHEET = os.environ.get('SMART_DEFENSE_TICKET_SHEET', 'NOR_Penalty_Ticket')
USER_SHEET = os.environ.get('SMART_DEFENSE_USER_SHEET', 'USER_ACCOUNT')
AUDIT_SHEET = os.environ.get('SMART_DEFENSE_AUDIT_SHEET', 'SD_AUDIT_LOG')
SECRET = os.environ.get('SMART_DEFENSE_SECRET', 'bbtec-smart-defense-change-me')
TOKEN_HOURS = int(os.environ.get('SMART_DEFENSE_TOKEN_HOURS', '12'))

# Quota-safe cache settings: reduce Google Sheets read calls
CACHE_TTL_USERS = int(os.environ.get('SMART_DEFENSE_CACHE_USERS_SEC', '300'))
CACHE_TTL_TICKETS = int(os.environ.get('SMART_DEFENSE_CACHE_TICKETS_SEC', '120'))
CACHE_TTL_AUDIT = int(os.environ.get('SMART_DEFENSE_CACHE_AUDIT_SEC', '60'))
AUDIT_LOGIN = os.environ.get('SMART_DEFENSE_AUDIT_LOGIN', 'FALSE').upper() == 'TRUE'
_SD_CACHE = {
    'users': {'ts': 0, 'data': None},
    'tickets': {'ts': 0, 'data': None},
    'audit_ready': {'ts': 0, 'data': None},
}

def _cache_valid(name, ttl):
    c = _SD_CACHE.get(name) or {}
    return c.get('data') is not None and (time.time() - c.get('ts', 0)) < ttl

def _cache_get(name, ttl):
    return _SD_CACHE[name]['data'] if _cache_valid(name, ttl) else None

def _cache_set(name, data):
    _SD_CACHE[name] = {'ts': time.time(), 'data': data}

def _cache_clear(name=None):
    if name:
        _SD_CACHE[name] = {'ts': 0, 'data': None}
    else:
        for k in list(_SD_CACHE):
            _SD_CACHE[k] = {'ts': 0, 'data': None}

NOR1 = {'CMI','CRI','LPG','LPN','MHS','NAN','PHE','PYO'}
NOR2 = {'KPP','PCB','PCT','PSN','SKT','TAK','UTR'}
OWNER_PROV = {
 'TRUE-TH-BBT-NOR1-CMI1-NOP':'CMI','TRUE-TH-BBT-NOR1-CMI2-NOP':'CMI','TRUE-TH-BBT-NOR1-CRI-NOP':'CRI','TRUE-TH-BBT-NOR1-LPG-NOP':'LPG','TRUE-TH-BBT-NOR1-LPN-NOP':'LPN','TRUE-TH-BBT-NOR1-MHS-NOP':'MHS','TRUE-TH-BBT-NOR1-NAN-NOP':'NAN','TRUE-TH-BBT-NOR1-PHE-NOP':'PHE','TRUE-TH-BBT-NOR1-PYO-NOP':'PYO',
 'TRUE-TH-BBT-NOR2-KPP-NOP':'KPP','TRUE-TH-BBT-NOR2-PCB-NOP':'PCB','TRUE-TH-BBT-NOR2-PCT-NOP':'PCT','TRUE-TH-BBT-NOR2-PSN-NOP':'PSN','TRUE-TH-BBT-NOR2-SKT-NOP':'SKT','TRUE-TH-BBT-NOR2-TAK-NOP':'TAK','TRUE-TH-BBT-NOR2-UTR-NOP':'UTR'
}

CONTROL = [
 'SD_STEP','SD_STATUS',
 'SD_LOCK_STEP1','SD_LOCK_STEP2','SD_FINAL_LOCK',
 'SD_DEFEND_COUNT','SD_DEFEND_NOTE','SD_MANAGER_REMARK',
 'SD_STEP1_BY','SD_STEP1_TIME','SD_STEP2_BY','SD_STEP2_TIME',
 'SD_DEFEND_BY','SD_DEFEND_TIME','SD_ACCEPT_BY','SD_ACCEPT_TIME',
 'SD_MANAGER_BY','SD_MANAGER_TIME',
 'SD_LAST_ACTION','SD_LAST_UPDATE','SD_LAST_USER'
]
AUDIT_HEADERS = ['Timestamp','Action','TicketID','Row','User','Name','Group','Role','Region','Province','Before_JSON','After_JSON','Changed_JSON','Remark']

FALLBACK = {
 'TICKETID':'A','TRUESEVERITY_DESC':'C','CREATIONDATE':'E','TARGETFINISH':'F','SUBJECT':'H','EXTERNALSYSTEM_TICKETID':'Q','PENALTYBAHT_TRACKB':'AB',
 'Group problem':'AD','Sub Problem':'AE','Accident':'AF','Overdue Detail แนบLINK รูป':'AG','แนบ LINK รูป':'AG','แนบ LINK ชี้แจง':'AH',
 'FSO พิจารณา (ปรับ/ไม่ปรับ)':'AI','FSO approve (ลงชื่อ FSO)':'AJ','วันที่ FSO อนุมัติ':'AK','Remark FSO':'AL'
}

STEP_LABEL = {
 '1':'STEP1_ENGINEER_REVIEW',
 '2':'STEP2_FSO_DECISION',
 '3':'STEP3_DEFEND_PROCESS',
 '4':'STEP4_FINAL_RESULT',
 '5':'STEP5_MANAGER_APPROVED'
}

def now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def norm(v):
    return str(v or '').strip().upper()

def col_letter(s):
    n = 0
    for c in str(s).upper():
        if 'A' <= c <= 'Z':
            n = n * 26 + ord(c) - 64
    return n or None

def hmap(headers):
    d = {}
    for i, h in enumerate(headers, 1):
        h = str(h).strip()
        if h:
            d[h] = i
            d[h.lower()] = i
    return d

def find_col(m, name):
    return m.get(name) or m.get(str(name).lower()) or col_letter(FALLBACK.get(name, ''))

def split_set(v):
    s = str(v or '').strip()
    if not s or s.upper() == 'ALL':
        return {'ALL'}
    return {x.strip().upper() for x in re.split(r'[,;|\n]+', s) if x.strip()}

def role(user):
    return str(user.get('role', '')).upper().replace(' ', '_').replace('-', '_')

def group(user):
    return str(user.get('group', '')).upper().replace(' ', '_').replace('-', '_')

def sign(payload):
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    body = base64.urlsafe_b64encode(raw).decode().rstrip('=')
    sig = hmac.new(SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    return body + '.' + sig

def verify(token):
    if not token or '.' not in token:
        return None
    body, sig = token.rsplit('.', 1)
    good = hmac.new(SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(good, sig):
        return None
    try:
        data = json.loads(base64.urlsafe_b64decode(body + '=' * (-len(body) % 4)).decode())
        return data if data.get('exp', 0) >= time.time() else None
    except Exception:
        return None

def auth():
    token = (request.headers.get('Authorization', '').replace('Bearer ', '').strip() or request.args.get('token', ''))
    user = verify(token)
    if not user:
        return None, (jsonify({'ok': False, 'error': 'unauthorized'}), 401)
    return user, None

def open_sheet(get_client):
    return get_client().open_by_key(SHEET_ID)

def open_ws(get_client, name, create_users=False):
    sh = open_sheet(get_client)
    try:
        return sh.worksheet(name)
    except Exception:
        if create_users:
            ws = sh.add_worksheet(title=name, rows=100, cols=20)
            ws.update('A1:J2', [[
                'User','Pass','Name','Group','Role','Region','Province','Systems','Active','Remark'
            ], [
                'admin','admin123','Administrator','BBTEC','BBTEC_REGIONAL','ALL','ALL','SMART_DEFENSE,PDT','TRUE','Change password'
            ]])
            return ws
        return sh.sheet1

def get_or_create_ws(get_client, name, headers, rows=1000, cols=30):
    sh = open_sheet(get_client)
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows=rows, cols=cols)
        ws.update('1:1', [headers])
        return ws
    # Quota-safe: read only header row, not the whole sheet.
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if not first:
        ws.update('1:1', [headers])
    return ws

def ensure_headers(ws, vals=None):
    # vals can be the result of one ws.get_all_values() call.
    # This avoids an extra Google read every /tickets or /summary call.
    if vals and len(vals) > 0:
        headers = vals[0]
    else:
        try:
            headers = ws.row_values(1)
        except Exception:
            headers = []
    headers = headers or ['TICKETID']
    cur = list(headers)
    changed = False
    for h in CONTROL:
        if h not in cur:
            cur.append(h)
            changed = True
    if changed:
        ws.update('1:1', [cur])
    return cur

def ensure_audit_sheet(get_client):
    # Quota-safe: cache that the audit sheet/header is already ready.
    if _cache_get('audit_ready', CACHE_TTL_AUDIT):
        return open_ws(get_client, AUDIT_SHEET)
    ws = get_or_create_ws(get_client, AUDIT_SHEET, AUDIT_HEADERS, rows=5000, cols=len(AUDIT_HEADERS) + 2)
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if first != AUDIT_HEADERS:
        ws.update('1:1', [AUDIT_HEADERS])
    _cache_set('audit_ready', True)
    return ws

def load_users(get_client, force=False):
    if not force:
        cached = _cache_get('users', CACHE_TTL_USERS)
        if cached is not None:
            return cached
    ws = open_ws(get_client, USER_SHEET, True)
    records = ws.get_all_records()
    _cache_set('users', records)
    return records

def row_obj(row, idx, colmap):
    def v(h):
        c = find_col(colmap, h)
        return str(row[c-1]).strip() if c and c <= len(row) else ''
    owner = v('TRUEOWNERGROUP') or v('OWNERGROUP') or v('Province')
    prov = OWNER_PROV.get(owner, v('Province') or v('PROVINCE'))
    reg = 'NOR1' if prov in NOR1 or 'NOR1' in owner else ('NOR2' if prov in NOR2 or 'NOR2' in owner else '')
    amount_raw = v('PENALTYBAHT_TRACKB')
    try:
        amount = float(str(amount_raw).replace(',', '').replace('บาท', '').strip() or 0)
    except Exception:
        amount = 0
    sd_step = v('SD_STEP') or '1'
    return {
        'row': idx,
        'ticketid': v('TICKETID') or v('Ticket'),
        'severity': v('TRUESEVERITY_DESC'),
        'creationdate': v('CREATIONDATE'),
        'targetfinish': v('TARGETFINISH'),
        'subject': v('SUBJECT'),
        'external_ticket': v('EXTERNALSYSTEM_TICKETID'),
        'penalty_raw': amount_raw,
        'penalty': amount,
        'owner_group': owner,
        'region': reg,
        'province': prov,
        'group_problem': v('Group problem'),
        'sub_problem': v('Sub Problem'),
        'accident': v('Accident'),
        'overdue_link': v('Overdue Detail แนบLINK รูป') or v('แนบ LINK รูป'),
        'explain_link': v('แนบ LINK ชี้แจง'),
        'fso_decision': v('FSO พิจารณา (ปรับ/ไม่ปรับ)'),
        'fso_approve': v('FSO approve (ลงชื่อ FSO)'),
        'fso_date': v('วันที่ FSO อนุมัติ'),
        'remark_fso': v('Remark FSO'),
        'sd_step': sd_step,
        'step_label': STEP_LABEL.get(str(sd_step), 'UNKNOWN'),
        'sd_status': v('SD_STATUS') or 'OPEN',
        'lock_step1': v('SD_LOCK_STEP1') == 'Y',
        'lock_step2': v('SD_LOCK_STEP2') == 'Y',
        'final_lock': v('SD_FINAL_LOCK') == 'Y',
        'defend_count': int(v('SD_DEFEND_COUNT') or 0),
        'defend_note': v('SD_DEFEND_NOTE'),
        'manager_remark': v('SD_MANAGER_REMARK'),
        'step1_by': v('SD_STEP1_BY'), 'step1_time': v('SD_STEP1_TIME'),
        'step2_by': v('SD_STEP2_BY'), 'step2_time': v('SD_STEP2_TIME'),
        'defend_by': v('SD_DEFEND_BY'), 'defend_time': v('SD_DEFEND_TIME'),
        'accept_by': v('SD_ACCEPT_BY'), 'accept_time': v('SD_ACCEPT_TIME'),
        'manager_by': v('SD_MANAGER_BY'), 'manager_time': v('SD_MANAGER_TIME'),
        'last_action': v('SD_LAST_ACTION'),
        'last_update': v('SD_LAST_UPDATE'),
        'last_user': v('SD_LAST_USER')
    }

def can_view(user, reg='', prov='', owner_group=''):
    r = role(user)
    if 'REGIONAL' in r:
        return True
    regs, provs = split_set(user.get('region')), split_set(user.get('province'))
    owner = norm(owner_group)
    if 'ALL' not in regs and reg and reg.upper() not in regs:
        return False
    if 'ALL' not in provs:
        # Province can be either short code CMI or full owner group TRUE-TH-...
        allowed = prov.upper() in provs or owner in provs
        if not allowed:
            return False
    return True

def can_action(user, action, row):
    r = role(user)
    g = group(user)
    if not can_view(user, row.get('region'), row.get('province'), row.get('owner_group')):
        return False
    if row.get('final_lock') and action not in ('manager_approve',):
        return False
    if action in ('step1_save','step1_confirm'):
        return (g in {'ENGINEER','SITE'} or 'ENGINEER' in r or 'SITE' in r) and not row.get('lock_step1') and str(row.get('sd_step')) in ('1','')
    if action == 'step2_confirm':
        return g == 'FSO' or 'FSO' in r
    if action in ('defend','accept'):
        return (g in {'ENGINEER','SITE','BBTEC'} or any(x in r for x in ['ENGINEER','SITE','SUP','BBTEC_MANAGER'])) and str(row.get('sd_step')) in ('2','3')
    if action == 'manager_approve':
        return g == 'BBTEC' and ('BBTEC_MANAGER' in r or 'REGIONAL' in r)
    return False

def get_rows(get_client, force=False):
    # For read-only endpoints, serve from cache to avoid Sheets 429 quota.
    # For write actions, call get_rows(..., force=True) to read the latest row state.
    if not force:
        cached = _cache_get('tickets', CACHE_TTL_TICKETS)
        if cached is not None:
            headers, m, data = cached
            return None, headers, m, data

    ws = open_ws(get_client, TICKET_SHEET)
    vals = ws.get_all_values()  # only one full-sheet read here
    headers = ensure_headers(ws, vals)
    m = hmap(headers)
    rows = [row_obj(row, i, m) for i, row in enumerate(vals[1:], 2)] if vals else []
    data = [r for r in rows if r.get('ticketid')]
    _cache_set('tickets', (headers, m, data))
    return ws, headers, m, data

def update_cells(ws, headers, row_num, updates):
    m = hmap(headers)
    cells = []
    for h, v in updates.items():
        c = find_col(m, h)
        if c:
            cells.append(gspread.Cell(row_num, c, '' if v is None else str(v)))
    if cells:
        ws.update_cells(cells, value_input_option='USER_ENTERED')

def public_before_after(row):
    keys = ['sd_step','sd_status','lock_step1','lock_step2','final_lock','defend_count','group_problem','sub_problem','accident','overdue_link','explain_link','fso_decision','fso_approve','fso_date','remark_fso','defend_note','manager_remark','last_action','last_update','last_user']
    return {k: row.get(k, '') for k in keys}

def build_changed(before, after_updates):
    changed = {}
    for k, v in after_updates.items():
        changed[k] = v
    return changed

def append_audit(get_client, user, action, row, before, updates, remark=''):
    try:
        ws = ensure_audit_sheet(get_client)
        before_pub = public_before_after(before or {})
        changed = build_changed(before_pub, updates or {})
        after_pub = dict(before_pub)
        for k, v in (updates or {}).items():
            lk = str(k).lower()
            # keep raw header changes too; useful for audit troubleshooting
            after_pub[k] = v
            if k == 'SD_STEP': after_pub['sd_step'] = v
            if k == 'SD_STATUS': after_pub['sd_status'] = v
            if k == 'SD_LOCK_STEP1': after_pub['lock_step1'] = (v == 'Y')
            if k == 'SD_LOCK_STEP2': after_pub['lock_step2'] = (v == 'Y')
            if k == 'SD_FINAL_LOCK': after_pub['final_lock'] = (v == 'Y')
            if k == 'SD_DEFEND_COUNT': after_pub['defend_count'] = v
        ws.append_row([
            now(), action, (before or {}).get('ticketid',''), (before or {}).get('row',''),
            user.get('user',''), user.get('name',''), user.get('group',''), user.get('role',''),
            (before or {}).get('region',''), (before or {}).get('province',''),
            json.dumps(before_pub, ensure_ascii=False),
            json.dumps(after_pub, ensure_ascii=False),
            json.dumps(changed, ensure_ascii=False),
            remark
        ], value_input_option='USER_ENTERED')
    except Exception:
        # Never block production action because audit append failed.
        pass

def require_step(row, allowed_steps, action):
    if str(row.get('sd_step')) not in {str(x) for x in allowed_steps}:
        return False, {'ok': False, 'error': 'invalid_step_for_action', 'action': action, 'current_step': row.get('sd_step'), 'allowed_steps': list(allowed_steps)}
    return True, None

def register_smart_defense(app, get_client, log):
    @app.route('/api/smart-defense/login', methods=['POST'])
    def sd_login():
        b = request.get_json(silent=True) or {}
        u = str(b.get('user') or b.get('username') or '').strip()
        p = str(b.get('pass') or b.get('password') or '').strip()
        if not u or not p:
            return jsonify({'ok': False, 'error': 'missing_user_or_password'}), 400
        try:
            records = load_users(get_client)
            for r in records:
                if str(r.get('User', '')).strip().lower() == u.lower() and str(r.get('Active', 'TRUE')).upper() != 'FALSE':
                    if str(r.get('Pass', '')).strip() != p:
                        return jsonify({'ok': False, 'error': 'invalid_password'}), 401
                    payload = {
                        'user': str(r.get('User', '')).strip(),
                        'name': str(r.get('Name') or r.get('User')).strip(),
                        'group': str(r.get('Group', 'BBTEC')).strip(),
                        'role': str(r.get('Role', '')).strip(),
                        'region': str(r.get('Region', 'ALL')).strip(),
                        'province': str(r.get('Province', 'ALL')).strip(),
                        'systems': str(r.get('Systems', 'SMART_DEFENSE')).strip(),
                        'iat': int(time.time()),
                        'exp': int(time.time() + TOKEN_HOURS * 3600)
                    }
                    if AUDIT_LOGIN:
                        append_audit(get_client, payload, 'login', {}, {}, {}, 'user_login_success')
                    return jsonify({'ok': True, 'token': sign(payload), 'user': payload})
            return jsonify({'ok': False, 'error': 'user_not_found'}), 401
        except Exception as e:
            log.exception('sd login failed')
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/api/smart-defense/me')
    def sd_me():
        user, err = auth()
        if err: return err
        return jsonify({'ok': True, 'user': user})

    @app.route('/api/smart-defense/tickets')
    def sd_tickets():
        user, err = auth()
        if err: return err
        try:
            _, _, _, data = get_rows(get_client)
            q = str(request.args.get('q', '')).lower().strip()
            step = str(request.args.get('step', '')).strip()
            out = []
            for r in data:
                if not can_view(user, r.get('region'), r.get('province'), r.get('owner_group')):
                    continue
                if step and str(r.get('sd_step')) != step:
                    continue
                if q and q not in json.dumps(r, ensure_ascii=False).lower():
                    continue
                r['permissions'] = {
                    'step1_save': can_action(user, 'step1_save', r),
                    'step1_confirm': can_action(user, 'step1_confirm', r),
                    'step2_confirm': can_action(user, 'step2_confirm', r) and str(r.get('sd_step')) in ('2','3') and not r.get('final_lock'),
                    'defend': can_action(user, 'defend', r) and r.get('fso_decision') == 'ปรับ' and r.get('defend_count', 0) < 2 and str(r.get('sd_step')) in ('2','3') and not r.get('final_lock'),
                    'accept': can_action(user, 'accept', r) and r.get('fso_decision') == 'ปรับ' and str(r.get('sd_step')) in ('2','3') and not r.get('final_lock'),
                    'manager_approve': can_action(user, 'manager_approve', r) and str(r.get('sd_step')) == '4'
                }
                out.append(r)
            return jsonify({'ok': True, 'rows': out[:1000], 'total': len(out), 'total_amount': sum(r['penalty'] for r in out), 'user': user, 'updated_at': now()})
        except Exception as e:
            log.exception('sd tickets failed')
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/api/smart-defense/action', methods=['POST'])
    def sd_action():
        user, err = auth()
        if err: return err
        b = request.get_json(silent=True) or {}
        action = str(b.get('action', '')).strip()
        try:
            row_num = int(b.get('row') or 0)
        except Exception:
            row_num = 0
        fields = b.get('fields') or {}
        if row_num < 2 or not action:
            return jsonify({'ok': False, 'error': 'missing_row_or_action'}), 400
        try:
            ws, headers, _, data = get_rows(get_client, force=True)
            cur = next((x for x in data if x['row'] == row_num), None)
            if not cur:
                return jsonify({'ok': False, 'error': 'row_not_found'}), 404
            if not can_action(user, action, cur):
                return jsonify({'ok': False, 'error': 'permission_denied_or_locked'}), 403

            ts = now()
            upd = {'SD_LAST_ACTION': action, 'SD_LAST_UPDATE': ts, 'SD_LAST_USER': user.get('user', '')}
            audit_remark = str(fields.get('remark') or b.get('remark') or '')

            if action in ('step1_save', 'step1_confirm'):
                ok, msg = require_step(cur, {'1',''}, action)
                if not ok: return jsonify(msg), 409
                if cur.get('lock_step1'):
                    return jsonify({'ok': False, 'error': 'step1_locked'}), 409
                upd.update({
                    'Group problem': fields.get('group_problem', cur.get('group_problem', '')),
                    'Sub Problem': fields.get('sub_problem', cur.get('sub_problem', '')),
                    'Accident': fields.get('accident', cur.get('accident', '')),
                    'Overdue Detail แนบLINK รูป': fields.get('overdue_link', cur.get('overdue_link', '')),
                    'แนบ LINK ชี้แจง': fields.get('explain_link', cur.get('explain_link', ''))
                })
                if action == 'step1_confirm':
                    upd.update({'SD_STEP': '2', 'SD_STATUS': 'STEP1_DONE', 'SD_LOCK_STEP1': 'Y', 'SD_STEP1_BY': user.get('user', ''), 'SD_STEP1_TIME': ts})

            elif action == 'step2_confirm':
                ok, msg = require_step(cur, {'2','3'}, action)
                if not ok: return jsonify(msg), 409
                dec = fields.get('fso_decision') or b.get('fso_decision')
                if dec not in ('ปรับ', 'ไม่ปรับ'):
                    return jsonify({'ok': False, 'error': 'decision_required'}), 400
                # If FSO says no-penalty, ticket is final immediately. If penalty, engineer can accept or defend.
                next_step = '4' if dec == 'ไม่ปรับ' else '2'
                next_status = 'FINAL_NO_PENALTY' if dec == 'ไม่ปรับ' else 'FSO_DECIDED_PENALTY'
                final_lock = 'Y' if dec == 'ไม่ปรับ' else ''
                upd.update({
                    'FSO พิจารณา (ปรับ/ไม่ปรับ)': dec,
                    'FSO approve (ลงชื่อ FSO)': fields.get('fso_approve') or user.get('name') or user.get('user'),
                    'วันที่ FSO อนุมัติ': fields.get('fso_date') or datetime.now().strftime('%Y-%m-%d'),
                    'Remark FSO': fields.get('remark_fso', cur.get('remark_fso', '')),
                    'SD_STEP': next_step,
                    'SD_STATUS': next_status,
                    'SD_LOCK_STEP2': 'Y',
                    'SD_FINAL_LOCK': final_lock,
                    'SD_STEP2_BY': user.get('user', ''),
                    'SD_STEP2_TIME': ts
                })

            elif action == 'defend':
                ok, msg = require_step(cur, {'2','3'}, action)
                if not ok: return jsonify(msg), 409
                dc = int(cur.get('defend_count') or 0)
                if cur.get('fso_decision') != 'ปรับ':
                    return jsonify({'ok': False, 'error': 'defend_allowed_only_penalty'}), 400
                if dc >= 2:
                    return jsonify({'ok': False, 'error': 'defend_limit_reached'}), 409
                upd.update({
                    'SD_STEP': '3',
                    'SD_STATUS': f'DEFEND_REQUESTED_ROUND_{dc + 1}',
                    'SD_DEFEND_COUNT': str(dc + 1),
                    'SD_LOCK_STEP2': '',
                    'SD_DEFEND_NOTE': fields.get('defend_note') or b.get('defend_note') or cur.get('defend_note', ''),
                    'SD_DEFEND_BY': user.get('user', ''),
                    'SD_DEFEND_TIME': ts
                })

            elif action == 'accept':
                ok, msg = require_step(cur, {'2','3'}, action)
                if not ok: return jsonify(msg), 409
                upd.update({'SD_STEP': '4', 'SD_STATUS': 'FINAL_ACCEPT_PENALTY', 'SD_FINAL_LOCK': 'Y', 'SD_ACCEPT_BY': user.get('user', ''), 'SD_ACCEPT_TIME': ts})

            elif action == 'manager_approve':
                ok, msg = require_step(cur, {'4'}, action)
                if not ok: return jsonify(msg), 409
                upd.update({'SD_STEP': '5', 'SD_STATUS': 'APPROVED', 'SD_FINAL_LOCK': 'Y', 'SD_MANAGER_BY': user.get('user', ''), 'SD_MANAGER_TIME': ts, 'SD_MANAGER_REMARK': fields.get('manager_remark', cur.get('manager_remark', ''))})

            else:
                return jsonify({'ok': False, 'error': 'unknown_action'}), 400

            update_cells(ws, headers, row_num, upd)
            _cache_clear('tickets')
            append_audit(get_client, user, action, cur, public_before_after(cur), upd, audit_remark)
            return jsonify({'ok': True, 'row': row_num, 'action': action, 'updates': upd})
        except Exception as e:
            log.exception('sd action failed')
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/api/smart-defense/summary')
    def sd_summary():
        user, err = auth()
        if err: return err
        try:
            _, _, _, data = get_rows(get_client)
            buckets = {}
            for r in data:
                if not can_view(user, r.get('region'), r.get('province'), r.get('owner_group')):
                    continue
                for typ, key in [('ALL', 'ALL'), ('REGION', r.get('region') or 'Unknown'), ('PROVINCE', r.get('province') or 'Unknown')]:
                    b = buckets.setdefault((typ, key), {'type': typ, 'key': key, 'total': 0, 'amount': 0, 'step1_done': 0, 'fso_penalty': 0, 'fso_no_penalty': 0, 'defend': 0, 'final': 0, 'approved': 0, 'penalty_amount': 0, 'no_penalty_amount': 0})
                    b['total'] += 1
                    b['amount'] += r['penalty']
                    if str(r['sd_step']) in ('2','3','4','5'): b['step1_done'] += 1
                    if r['fso_decision'] == 'ปรับ':
                        b['fso_penalty'] += 1; b['penalty_amount'] += r['penalty']
                    if r['fso_decision'] == 'ไม่ปรับ':
                        b['fso_no_penalty'] += 1; b['no_penalty_amount'] += r['penalty']
                    if r['defend_count'] > 0: b['defend'] += 1
                    if str(r['sd_step']) in ('4','5'): b['final'] += 1
                    if str(r['sd_step']) == '5': b['approved'] += 1
            rows = list(buckets.values())
            for b in rows:
                b['actual_pct'] = round(b['step1_done'] / b['total'] * 100, 1) if b['total'] else 0
            return jsonify({'ok': True, 'rows': rows, 'updated_at': now()})
        except Exception as e:
            log.exception('sd summary failed')
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/api/smart-defense/audit')
    def sd_audit():
        user, err = auth()
        if err: return err
        try:
            ticketid = str(request.args.get('ticketid','')).strip()
            row_filter = str(request.args.get('row','')).strip()
            limit = int(request.args.get('limit') or 200)
            ws = ensure_audit_sheet(get_client)
            records = ws.get_all_records()
            out = []
            for rec in reversed(records):
                if ticketid and str(rec.get('TicketID','')).strip() != ticketid:
                    continue
                if row_filter and str(rec.get('Row','')).strip() != row_filter:
                    continue
                out.append(rec)
                if len(out) >= limit:
                    break
            return jsonify({'ok': True, 'rows': out, 'total': len(out), 'updated_at': now()})
        except Exception as e:
            log.exception('sd audit failed')
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/api/smart-defense/lock-status')
    def sd_lock_status():
        user, err = auth()
        if err: return err
        try:
            _, _, _, data = get_rows(get_client)
            row_num = int(request.args.get('row') or 0)
            cur = next((x for x in data if x['row'] == row_num), None)
            if not cur:
                return jsonify({'ok': False, 'error': 'row_not_found'}), 404
            if not can_view(user, cur.get('region'), cur.get('province'), cur.get('owner_group')):
                return jsonify({'ok': False, 'error': 'permission_denied'}), 403
            return jsonify({'ok': True, 'lock': {
                'row': cur['row'], 'ticketid': cur['ticketid'], 'step': cur['sd_step'], 'status': cur['sd_status'],
                'lock_step1': cur['lock_step1'], 'lock_step2': cur['lock_step2'], 'final_lock': cur['final_lock'],
                'defend_count': cur['defend_count'], 'permissions': {
                    'step1_save': can_action(user,'step1_save',cur), 'step1_confirm': can_action(user,'step1_confirm',cur),
                    'step2_confirm': can_action(user,'step2_confirm',cur), 'defend': can_action(user,'defend',cur),
                    'accept': can_action(user,'accept',cur), 'manager_approve': can_action(user,'manager_approve',cur)
                }
            }})
        except Exception as e:
            log.exception('sd lock status failed')
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/api/smart-defense/cache-status')
    def sd_cache_status():
        user, err = auth()
        if err: return err
        return jsonify({'ok': True, 'cache': {
            'users_cached': _SD_CACHE['users'].get('data') is not None,
            'tickets_cached': _SD_CACHE['tickets'].get('data') is not None,
            'audit_ready_cached': _SD_CACHE['audit_ready'].get('data') is not None,
            'users_age_sec': round(time.time() - _SD_CACHE['users'].get('ts', 0), 1) if _SD_CACHE['users'].get('data') is not None else None,
            'tickets_age_sec': round(time.time() - _SD_CACHE['tickets'].get('ts', 0), 1) if _SD_CACHE['tickets'].get('data') is not None else None,
            'audit_ready_age_sec': round(time.time() - _SD_CACHE['audit_ready'].get('ts', 0), 1) if _SD_CACHE['audit_ready'].get('data') is not None else None,
            'ttl_users_sec': CACHE_TTL_USERS,
            'ttl_tickets_sec': CACHE_TTL_TICKETS,
            'ttl_audit_sec': CACHE_TTL_AUDIT
        }, 'updated_at': now()})

    @app.route('/api/smart-defense/clear-cache', methods=['POST'])
    def sd_clear_cache():
        user, err = auth()
        if err: return err
        r = role(user)
        if not ('REGIONAL' in r or 'MANAGER' in r or user.get('user','').lower() == 'admin'):
            return jsonify({'ok': False, 'error': 'permission_denied'}), 403
        _cache_clear()
        return jsonify({'ok': True, 'message': 'smart defense cache cleared', 'updated_at': now()})

    @app.route('/api/smart-defense/setup-users', methods=['POST'])
    def sd_setup_users():
        expected = os.environ.get('SMART_DEFENSE_SETUP_KEY')
        key = request.args.get('key') or (request.get_json(silent=True) or {}).get('key')
        if expected and key != expected:
            return jsonify({'ok': False, 'error': 'setup_key_required'}), 403
        try:
            ws = open_ws(get_client, USER_SHEET, True)
            ws.clear()
            rows = [
                ['User','Pass','Name','Group','Role','Region','Province','Systems','Active','Remark'],
                ['bbtec_regional','ChangeMe123','BBTEC Regional','BBTEC','BBTEC_REGIONAL','ALL','ALL','SMART_DEFENSE,PDT','TRUE','ดูภาพรวมทั้งหมด'],
                ['bbtec_mgr_nor1','ChangeMe123','BBTEC Manager NOR1','BBTEC','BBTEC_MANAGER','NOR1','ALL','SMART_DEFENSE','TRUE','อนุมัติ NOR1'],
                ['bbtec_mgr_nor2','ChangeMe123','BBTEC Manager NOR2','BBTEC','BBTEC_MANAGER','NOR2','ALL','SMART_DEFENSE','TRUE','อนุมัติ NOR2'],
                ['engineer_cmi','ChangeMe123','Engineer CMI','ENGINEER','ENGINEER_ZONE','NOR1','TRUE-TH-BBT-NOR1-CMI1-NOP,TRUE-TH-BBT-NOR1-CMI2-NOP','SMART_DEFENSE','TRUE','แก้ไข Step1'],
                ['sitesup_cmi','ChangeMe123','Site Sup CMI','SITE','SITE_SUP','NOR1','TRUE-TH-BBT-NOR1-CMI1-NOP','SMART_DEFENSE','TRUE','แก้ไข Step1'],
                ['fso_regional','ChangeMe123','FSO Regional','FSO','FSO_REGIONAL','ALL','ALL','SMART_DEFENSE','TRUE','ดูภาพรวม FSO'],
                ['fso_nor1','ChangeMe123','FSO NOR1','FSO','FSO_ZONE','NOR1','ALL','SMART_DEFENSE','TRUE','แก้ไข Step2']
            ]
            ws.update('A1:J' + str(len(rows)), rows)
            ensure_audit_sheet(get_client)
            _cache_clear('users')
            _cache_clear('audit_ready')
            return jsonify({'ok': True, 'sheet': USER_SHEET, 'audit_sheet': AUDIT_SHEET, 'message': 'USER_ACCOUNT template and audit sheet created'})
        except Exception as e:
            log.exception('sd setup users failed')
            return jsonify({'ok': False, 'error': str(e)}), 500
