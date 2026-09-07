# version: v202609063
"""
handlers_nan_province.py — NAN Province War Room
OF: 10 ทีม | NO: 3 ทีม
4 กลุ่ม: กำลังทำงาน / รอออก / ไม่มี action / ลา
"""
import os, asyncio, json, gspread, re
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from telegram.ext import Application, CommandHandler
from telegram.constants import ParseMode
import sheets

NAN_CHAT_ID  = int(os.environ.get("NAN_CHAT_ID", "-5447107805"))
OWS_SHEET_ID = os.environ.get("OWS_SHEET_ID", "1PsJnXf8X7rBbA6G96L0ojE_ioby4mKqYcJQLp7fFiOw")
OWS_TAB      = "Daily"
SCOPES       = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
INACTIVE_THR = 2 * 3600

_on_leave:  set  = set()
_completed: dict = {}

DEPART_STATUS  = {"07) Depart","08) Arrive","09) Update","15) Resume"}
PENDING_STATUS = {"04) Dispatch","05) Reassign to","06) Accept","16) Reject"}
DONE_STATUS    = {"10) Complete","11) Leave","12) Close"}
ACTIVE_STATUS  = DEPART_STATUS | PENDING_STATUS

STATUS_EMOJI = {
    "07) Depart":"🚗","08) Arrive":"📍","09) Update":"📝",
    "10) Complete":"✅","11) Leave":"🔒","12) Close":"🔒",
    "14) Suspend":"⏸","15) Resume":"▶️",
    "04) Dispatch":"❌","05) Reassign to":"❌",
    "06) Accept":"❌","16) Reject":"❌",
}

def _bkknow():
    return datetime.utcnow() + timedelta(hours=7)

def _parse(s):
    for f in ["%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M",
              "%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M",
              "%m/%d/%Y %H:%M:%S","%m/%d/%Y %H:%M"]:
        try: return datetime.strptime(str(s).strip(), f)
        except: pass
    return None

def _elapsed_str(dt):
    if not dt: return "-"
    secs = max(int((_bkknow()-dt).total_seconds()), 0)
    h, m = divmod(secs//60, 60)
    return f"{h}h {m}m" if h else f"{m}m"

def _short_team(team):
    t = str(team).strip()
    if not t or t.lower() == "none": return "-"
    for key in ["_OF_","_NO_"]:
        idx = t.upper().find(key)
        if idx != -1:
            rest = t[idx+1:]
            # ตัดแค่ _BBT หรือ Tel. ไม่ตัด _0 เพราะทำให้ NF_001 → NF
            for stop in ["_BBT","_bbt"," Tel"," tel",".Tel"]:
                si = rest.find(stop)
                if si != -1: rest = rest[:si]; break
            # ตัด _N05, _N01 ท้าย
            import re
            rest = re.sub(r"_N\d{2,}$", "", rest)
            return rest
    return t.split("(")[0].strip()[:20]

def _ows_short_team(team_full):
    """
    NF-NR-EXT-OFC-004 ( Sophon_OF_NAN_NF_004 Tel... ) → OF_NAN_NF_004
    NF-NR-NAN-OFC-001 ( somphong_OF_NAN_NF_001_BBT Tel... ) → OF_NAN_NF_001
    ดึงจากวงเล็บก่อน ถ้าไม่มีค่อยใช้ชื่อหลัก
    """
    if "(" in team_full and ")" in team_full:
        inner = team_full.split("(")[1].split(")")[0].strip()
        # ตัด Tel. ออก
        inner = inner.split("Tel.")[0].strip()
        return _short_team(inner)
    return _short_team(team_full)

def _is_nan_team(team_full):
    """เช็คว่าเป็นทีม NAN จริงๆ โดยดูจาก team code ไม่ใช่ชื่อคน
    - NF-NR-NAN-OFC-001 → True
    - NF-NR-EXT-OFC-004 ( Sophon_OF_NAN_NF_004 ) → True
    - NF-NR-CRI-OFC-009 → False
    """
    prefix = team_full.split("(")[0].upper()
    if re.search(r'[-_]NAN[-_]', prefix):
        return True
    if "(" in team_full and ")" in team_full:
        inner = team_full.split("(")[1].split(")")[0].upper()
        if re.search(r'_NAN_', inner):
            return True
    return False

def _team_type(team_short):
    t = team_short.upper()
    if t.startswith("OF_"): return "OF"
    if t.startswith("NO_"): return "NO"
    return "OTHER"

def _is_nan_gss(r):
    return "NAN" in str(r.get("TRUEOWNERGROUP","")).upper()

def _had_depart_today(tickets):
    """True ถ้า Depart วันนี้เป็นต้นไป"""
    today = _bkknow().date()
    for tid, ci, st, ts, created, target in tickets:
        if ts and ts.date() == today and st in DEPART_STATUS:
            return True
    return False

def _has_pending(tickets):
    """True ถ้ามี ticket ที่ Dispatch/Accept แต่ยังไม่ Depart"""
    return any(t[2] in PENDING_STATUS for t in tickets if t[2] not in DONE_STATUS)

# ─── OWS Reader ───
def _get_ows_client():
    creds_env = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    info = json.loads(creds_env) if creds_env else json.load(open("credentials.json"))
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=SCOPES))

def _load_ows_nan_teams() -> dict:
    """คืน {team_short: {"depart_today": bool, "last_depart": datetime|None}}"""
    try:
        ws   = _get_ows_client().open_by_key(OWS_SHEET_ID).worksheet(OWS_TAB)
        vals = ws.get_all_values()
        if not vals: return {}
        raw = vals[0]; seen: dict = {}; headers = []
        for h in raw:
            h = h.strip()
            if h in seen: seen[h] += 1; headers.append(f"{h}_{seen[h]}")
            else: seen[h] = 0; headers.append(h)
        rows = [dict(zip(headers, row)) for row in vals[1:]]
    except Exception as e:
        print(f"[nan_ows] error: {e}"); return {}

    today = _bkknow().date()
    result: dict = {}
    for r in rows:
        team_full = str(r.get("Team","")).strip()
        if not _is_nan_team(team_full): continue
        team = _ows_short_team(team_full)
        if not team or team == "-": continue
        dep_dt = _parse(str(r.get("Departed","")).strip())
        if team not in result:
            result[team] = {"depart_today": False, "last_depart": None}
        if dep_dt:
            if dep_dt.date() == today:
                result[team]["depart_today"] = True
            if not result[team]["last_depart"] or dep_dt > result[team]["last_depart"]:
                result[team]["last_depart"] = dep_dt
    return result

# ─── GGS Team Map ───
def _load_team_map(nan_rows) -> dict:
    teams: dict = {}
    for r in nan_rows:
        team = _short_team(str(r.get("Tech_Team","")).strip())
        if not team or team == "-": continue
        tid     = str(r.get("TICKETID","")).strip()
        ci      = str(r.get("CINAME","")).strip()
        st      = str(r.get("Tech_Status","")).strip()
        ts      = _parse(str(r.get("Tech_timestamp","")))
        created = _parse(str(r.get("CREATIONDATE","")))
        target  = _parse(str(r.get("TARGETFINISH","")))
        if team not in teams:
            teams[team] = {"tickets":[], "last_done_ts":None, "last_done_tid":""}
        teams[team]["tickets"].append((tid, ci, st, ts, created, target))
        if st in DONE_STATUS and ts:
            if not teams[team]["last_done_ts"] or ts > teams[team]["last_done_ts"]:
                teams[team]["last_done_ts"] = ts
                teams[team]["last_done_tid"] = tid
    return teams

def _snapshot_completed(team_map):
    today = _bkknow().strftime("%Y-%m-%d")
    if today not in _completed: _completed[today] = []
    existing = {x[0] for x in _completed[today]}
    for team, data in team_map.items():
        for tid, ci, st, ts, created, target in data["tickets"]:
            if st in DONE_STATUS and ts and tid not in existing:
                dur = ""
                if created and ts:
                    h, m = divmod(max(int((ts-created).total_seconds()),0)//60, 60)
                    dur = f"{h}h {m}m" if h else f"{m}m"
                _completed[today].append((tid, ci, team, ts, dur))
                existing.add(tid)

# ─── War Room Builder ───
def _build_war_room(team_map, ins, ows_teams=None) -> list:
    now   = _bkknow()
    today = now.strftime("%Y-%m-%d")
    done_today = len(_completed.get(today, []))

    on_leave_list = []
    # 3 กลุ่มหลัก แบ่ง OF/NO
    working = {"OF":[],"NO":[],"OTHER":[]}   # Depart แล้ว
    pending = {"OF":[],"NO":[],"OTHER":[]}   # รับงานแล้ว รอออก
    idle    = {"OF":[],"NO":[],"OTHER":[]}   # ไม่มี action วันนี้

    gss_teams: set = set()

    for team, data in sorted(team_map.items()):
        gss_teams.add(team)
        if team in _on_leave:
            on_leave_list.append(team); continue
        all_t  = data["tickets"]
        active = [t for t in all_t if t[2] not in DONE_STATUS]
        ttype  = _team_type(team)

        if _had_depart_today(all_t) and active:
            working[ttype].append((team, data))
        elif _has_pending(active):
            pending[ttype].append((team, data))
        else:
            idle[ttype].append((team, data))

    # เพิ่มทีมจาก OWS ที่ไม่มีใน GGS
    if ows_teams:
        for team, od in sorted(ows_teams.items()):
            if team in gss_teams or team in _on_leave: continue
            ttype = _team_type(team)
            fake  = {"tickets":[], "last_done_ts": od.get("last_depart"), "last_done_tid":""}
            if od.get("depart_today"):
                working[ttype].append((team, fake))
            else:
                idle[ttype].append((team, fake))

    # นับ
    n_of_working = len(working["OF"])
    n_no_working = len(working["NO"])
    n_of_total   = n_of_working + len(pending["OF"]) + len(idle["OF"])
    n_no_total   = n_no_working + len(pending["NO"]) + len(idle["NO"])

    blocks = []

    # ─── Header ───
    blocks.append(("code",[
        f"📍 NAN Province | {now.strftime('%d/%m %H:%M')}",
        f"Pending: {ins}",
        f"วันนี้ปิด {done_today} TT",
        "─"*30,
        f"📊 OF: {n_of_working}/{n_of_total} ทีม | NO: {n_no_working}/{n_no_total} ทีม",
        "   (กำลังทำงาน / ทั้งหมด)",
    ]))

    # ─── กำลังทำงาน (Depart แล้ว) ───
    if any(working[k] for k in working):
        blocks.append(("html","🔄 <b>กำลังทำงาน</b>"))
        lines = []
        for ttype, label in [("OF","[ OF ]"),("NO","[ NO ]"),("OTHER","[ อื่นๆ ]")]:
            if not working[ttype]: continue
            lines.append(label)
            for team, data in working[ttype]:
                active = [t for t in data["tickets"] if t[2] not in DONE_STATUS]
                if not active:
                    lines.append(f"  ✅ {team} — ว่าง {_elapsed_str(data['last_done_ts'])}")
                    lines.append(""); continue
                latest = max(active, key=lambda t: t[3] or datetime.min)
                tid, ci, st, ts, created, target = latest
                em     = STATUS_EMOJI.get(st,"❓")
                ts_str = ts.strftime("%H:%M") if ts else "-"
                no_upd = int((now-ts).total_seconds()) if ts else 0
                warn   = f" ⚠️ ไม่ update {_elapsed_str(ts)}" if no_upd >= INACTIVE_THR else ""
                lines.append(f"  {em} {team} | {ci}")
                lines.append(f"     {ts_str} | ผ่านมา {_elapsed_str(ts)}{warn}")
                if len(active) > 1:
                    lines.append(f"     ({len(active)} ticket ในมือ)")
                lines.append("")
        blocks.append(("code", lines))

    # ─── รับงานแล้ว รอออก ───
    if any(pending[k] for k in pending):
        blocks.append(("html","⏳ <b>รับงานแล้ว — รอออกเดินทาง</b>"))
        lines = []
        for ttype, label in [("OF","[ OF ]"),("NO","[ NO ]"),("OTHER","[ อื่นๆ ]")]:
            if not pending[ttype]: continue
            lines.append(label)
            for team, data in pending[ttype]:
                active = [t for t in data["tickets"] if t[2] in PENDING_STATUS]
                latest = max(active, key=lambda t: t[3] or datetime.min) if active else None
                if latest:
                    tid, ci, st, ts, created, target = latest
                    em     = STATUS_EMOJI.get(st,"❌")
                    ts_str = ts.strftime("%H:%M") if ts else "-"
                    lines.append(f"  {em} {team} | {ci}")
                    lines.append(f"     {st} ตั้งแต่ {ts_str} | ผ่านมา {_elapsed_str(ts)}")
                    if len(active) > 1:
                        lines.append(f"     ({len(active)} ticket รอ)")
                lines.append("")
        blocks.append(("code", lines))

    # ─── ไม่มี action วันนี้ ───
    if any(idle[k] for k in idle):
        blocks.append(("html","😴 <b>ไม่มี action วันนี้</b>"))
        lines = []
        for ttype, label in [("OF","[ OF ]"),("NO","[ NO ]"),("OTHER","[ อื่นๆ ]")]:
            if not idle[ttype]: continue
            lines.append(label)
            for team, data in idle[ttype]:
                if data["last_done_ts"]:
                    lines.append(f"  ➖ {team} — ว่าง {_elapsed_str(data['last_done_ts'])}")
                else:
                    lines.append(f"  ➖ {team} — ไม่มีข้อมูล")
            lines.append("")
        blocks.append(("code", lines))

    # ─── ลา ───
    if on_leave_list:
        blocks.append(("code",["🔕 ลา/หยุด"] + [f"   {t}" for t in on_leave_list]))

    # ─── เสร็จวันนี้ ───
    today_done = _completed.get(today,[])
    if today_done:
        blocks.append(("html","✅ <b>เสร็จวันนี้</b>"))
        lines = []
        for tid, ci, team, ts, dur in sorted(today_done, key=lambda x: x[3], reverse=True)[:10]:
            ts_str = ts.strftime("%H:%M") if ts else "-"
            lines.append(f"  {ci} | {ts_str} | {team}")
            if dur: lines.append(f"  ใช้เวลา {dur}")
            lines.append("")
        blocks.append(("code", lines))

    blocks.append(("code",["─"*30,
        "/nan | /leave ชื่อทีม | /return ชื่อทีม | /leavelist"]))
    return blocks

# ─── Send ───
async def _safe_send(bot, chat_id, text, mode):
    for _ in range(3):
        try:
            await bot.send_message(chat_id, text, parse_mode=mode)
            await asyncio.sleep(0.8); return
        except Exception as e:
            if "Flood control" in str(e) or "retry" in str(e).lower():
                import re
                m = re.search(r"Retry in (\d+)", str(e))
                await asyncio.sleep(int(m.group(1))+2 if m else 35)
            else: raise

async def _send_blocks(bot, chat_id, blocks):
    MAX = 3800
    def chunks(ls):
        txt = "```\n"+"\n".join(ls)+"\n```"
        if len(txt) <= MAX: return [ls]
        mid = len(ls)//2
        return chunks(ls[:mid]) + chunks(ls[mid:])
    for block in blocks:
        btype, content = block if isinstance(block, tuple) else ("code", block)
        if btype in ("md","html"):
            await _safe_send(bot, chat_id, content, "HTML")
        else:
            for chunk in chunks(content):
                await _safe_send(bot, chat_id,
                    "```\n"+"\n".join(chunk)+"\n```", ParseMode.MARKDOWN)

# ─── Auto ───
async def run_nan_auto(app: Application):
    print("[nan_province] sending NAN war room")
    try:
        rows      = await asyncio.to_thread(sheets.get_all_records)
        ins       = sheets.get_latest_insert_time()
        nan_rows  = [r for r in rows if _is_nan_gss(r)]
        team_map  = _load_team_map(nan_rows)
        ows_teams = await asyncio.to_thread(_load_ows_nan_teams)
        _snapshot_completed(team_map)
        blocks    = _build_war_room(team_map, ins, ows_teams)
        await _send_blocks(app.bot, NAN_CHAT_ID, blocks)
    except Exception as e:
        print(f"[nan_province] error: {e}")
        try: await app.bot.send_message(NAN_CHAT_ID, f"❌ NAN error: {e}")
        except: pass

# ─── Commands ───
def get_handlers():
    async def cmd_nan(update, ctx):
        import asyncio as _a
        await update.message.reply_text("⏳ กำลังโหลด NAN Province...")
        rows      = await _a.to_thread(sheets.get_all_records)
        ins       = sheets.get_latest_insert_time()
        nan_rows  = [r for r in rows if _is_nan_gss(r)]
        team_map  = _load_team_map(nan_rows)
        ows_teams = await _a.to_thread(_load_ows_nan_teams)
        _snapshot_completed(team_map)
        blocks    = _build_war_room(team_map, ins, ows_teams)
        await _send_blocks(ctx.bot, update.effective_chat.id, blocks)

    async def cmd_leave(update, ctx):
        args = " ".join(ctx.args).strip().upper() if ctx.args else ""
        if not args:
            await update.message.reply_text("รูปแบบ: /leave OF_NAN_KO_001"); return
        _on_leave.add(args)
        await update.message.reply_text(f"✅ Mark {args} ลา/หยุด\n/return {args} เพื่อกลับมา")

    async def cmd_return(update, ctx):
        args = " ".join(ctx.args).strip().upper() if ctx.args else ""
        if not args:
            await update.message.reply_text("รูปแบบ: /return OF_NAN_KO_001"); return
        _on_leave.discard(args)
        await update.message.reply_text(f"✅ {args} กลับมาทำงานแล้ว")

    async def cmd_leavelist(update, ctx):
        if not _on_leave:
            await update.message.reply_text("ไม่มีทีมที่ mark ลา"); return
        await update.message.reply_text(
            "🔕 ทีมที่ลา:\n" + "\n".join(f"  • {t}" for t in sorted(_on_leave)))

    return [
        CommandHandler("nan",       cmd_nan),
        CommandHandler("leave",     cmd_leave),
        CommandHandler("return",    cmd_return),
        CommandHandler("leavelist", cmd_leavelist),
    ]
