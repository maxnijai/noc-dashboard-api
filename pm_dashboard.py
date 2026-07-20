"""
pm_dashboard.py — PM Dashboard API routes
เพิ่มเข้า app.py ของ noc-dashboard-api
"""
import os, json, time, gspread
from datetime import datetime
from flask import jsonify, request
from google.oauth2.service_account import Credentials

PM_SHEET_ID = "12wGTkeheYEtH2p0iTCmVlN605JUl3qORBsOlEEy9_Cc"
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Column indices (1-based → 0-based for list access)
DN_COLS = {
    "site_id":   1,   # B
    "node_type": 2,   # C
    "con_no":    3,   # D
    "building":  4,   # E
    "pm_date":   8,   # I
    "pm_round":  10,  # K
    "team":      13,  # N
    "pm_name":   14,  # O
    "vfy_st":    993, # ALF
    "vfy_dt":    994, # ALG
    "vfy_name":  996, # ALI
    "bat1_bt":   443, # QB
    "bat2_bt":   733, # ABF
    "has_trans": 63,  # BL
    "has_gen":   80,  # CC
}

NODEB_COLS = {
    "site_id":  1,   # A
    "cab_no":   5,   # E
    "bat_bt":   197, # GO
    "vfy_st":   270, # JJ
    "vfy_name": 271, # JK
}

_pm_cache = {}
CACHE_TTL = 1800  # 30 นาที

def _get_pm_client():
    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=SCOPES))

def _load_tab(tab: str) -> list[list]:
    now = time.time()
    if tab in _pm_cache and (now - _pm_cache[tab]["ts"]) < CACHE_TTL:
        return _pm_cache[tab]["data"]
    ws = _get_pm_client().open_by_key(PM_SHEET_ID).worksheet(tab)
    data = ws.get_all_values()
    _pm_cache[tab] = {"data": data, "ts": now}
    return data

def g(row, idx):
    i = idx - 1  # convert 1-based to 0-based
    return row[i].strip() if len(row) > i else ""

def _parse_dn_rows(vals):
    rows = []
    for raw in vals[1:]:
        site_id = g(raw, DN_COLS["site_id"])
        if not site_id: continue
        rows.append({
            "site_id":   site_id,
            "node_type": g(raw, DN_COLS["node_type"]),
            "con_no":    g(raw, DN_COLS["con_no"]),
            "building":  g(raw, DN_COLS["building"]),
            "pm_date":   g(raw, DN_COLS["pm_date"]),
            "pm_round":  g(raw, DN_COLS["pm_round"]),
            "team":      g(raw, DN_COLS["team"]),
            "pm_name":   g(raw, DN_COLS["pm_name"]),
            "vfy_st":    g(raw, DN_COLS["vfy_st"]),
            "vfy_dt":    g(raw, DN_COLS["vfy_dt"]),
            "vfy_name":  g(raw, DN_COLS["vfy_name"]),
            "bat1_bt":   g(raw, DN_COLS["bat1_bt"]),
            "bat2_bt":   g(raw, DN_COLS["bat2_bt"]),
            "has_trans": g(raw, DN_COLS["has_trans"]).upper(),
            "has_gen":   g(raw, DN_COLS["has_gen"]).upper(),
        })
    return rows

def _parse_nodeb_rows(vals):
    rows = []
    for raw in vals[1:]:
        site_id = g(raw, NODEB_COLS["site_id"])
        if not site_id: continue
        rows.append({
            "site_id":  site_id,
            "cab_no":   g(raw, NODEB_COLS["cab_no"]),
            "bat_bt":   g(raw, NODEB_COLS["bat_bt"]),
            "vfy_st":   g(raw, NODEB_COLS["vfy_st"]),
            "vfy_name": g(raw, NODEB_COLS["vfy_name"]),
        })
    return rows

def build_pm_summary():
    """สร้าง summary สำหรับ Dashboard"""
    result = {}
    for tab, parser in [("DN", _parse_dn_rows), ("NodeB", _parse_nodeb_rows), ("Broadband", _parse_dn_rows)]:
        try:
            vals = _load_tab(tab)
            rows = parser(vals)
            sites = {}
            for r in rows:
                sid = r["site_id"]
                if sid not in sites:
                    sites[sid] = r
            total    = len(sites)
            pm_done  = sum(1 for r in sites.values() if r.get("pm_date") or r.get("bat_bt"))
            vfy_done = sum(1 for r in sites.values() if (r.get("vfy_st","").upper() in ("PASS","VERIFIED","✓","ผ่าน")))
            result[tab] = {
                "total":    total,
                "pm_done":  pm_done,
                "pm_pct":   round(pm_done/total*100) if total else 0,
                "vfy_done": vfy_done,
                "vfy_pct":  round(vfy_done/total*100) if total else 0,
                "pending":  total - pm_done,
                "sites":    sorted(sites.keys()),
                "site_rows": [dict(
                    site_id  = r["site_id"],
                    pm_date  = r.get("pm_date",""),
                    vfy_st   = r.get("vfy_st",""),
                    team     = r.get("team","") or r.get("cab_no",""),
                    bat_bt   = r.get("bat1_bt","") or r.get("bat_bt",""),
                ) for r in sorted(sites.values(), key=lambda x: x.get("pm_date",""), reverse=True)],
            }
        except Exception as e:
            result[tab] = {"error": str(e)}
    result["cached_at"] = datetime.now().isoformat()
    return result

def register_pm_routes(app):
    @app.route("/api/pm/summary")
    def pm_summary():
        try:
            return jsonify(build_pm_summary())
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/pm/site/<tab>/<site_id>")
    def pm_site(tab, site_id):
        try:
            vals = _load_tab(tab)
            parser = _parse_nodeb_rows if tab == "NodeB" else _parse_dn_rows
            rows = [r for r in parser(vals) if r["site_id"].upper() == site_id.upper()]
            return jsonify({"site_id": site_id, "tab": tab, "rows": rows})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/pm")
    def pm_page():
        from flask import render_template
        return render_template("pm.html")
