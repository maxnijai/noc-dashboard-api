"""Operation Temp Point Improvement - a NEW tab.

Data source: Google Sheet "Map" tab (see TEMP_POINT_SHEET_ID/GID below).
Confirmed by direct inspection before writing any of this:

- Region/Province: the sheet's OWN "Region"/"Province" columns use an
  inconsistent format ("NOR_PCB") that doesn't match the rest of this
  app's "NOR1"/"NOR2" convention - so they are NOT used. Region/Province
  are instead derived from "Activity Owner Group" (e.g.
  "TRUE-TH-BBT-NOR2-PCB-NOP") using the exact same regex sla_improvement.py
  already uses for the same purpose, reused here as the literal source of
  truth ("Logic ของ Data เดิม" per the request).
- "Complete Lat Lon" is a single cell formatted "lat, lon" (comma
  separated), not two separate columns.
- The sheet has EMBEDDED REPEATED HEADER ROWS scattered through the data
  (confirmed: rows where the header row's OWN text reappears mid-data,
  apparently from each province's chunk having been pasted in with its
  header row still attached). These must be detected and skipped or they
  corrupt the parse as garbage data rows - never silently included.

Cluster logic (the whole point of this tab): SEQUENTIAL chain distance,
never a radius from a centroid and never distance-from-first-point. Points
are ordered via a per-province greedy nearest-neighbor walk (the sheet has
no explicit route-sequence column), then walked in that order: each
consecutive gap <= 500m keeps the chain going; a gap > 500m starts a new
chain. A chain of length 1 is "Individual", not a cluster.
"""

import logging
import math
import re
import threading
from datetime import datetime

log = logging.getLogger(__name__)

TEMP_POINT_SHEET_ID = "15n-UyUIiR0rYgyMSanZcM4u6vkmYsTjdTFG24Rer0UQ"
TEMP_POINT_WORKSHEET_GID = 711428479
TEMP_POINT_WORKSHEET_NAME = "Map"

CHAIN_DISTANCE_THRESHOLD_M = 500

REQUIRED_COLUMNS = [
    "Source TT", "INC", "Site/Cable", "CI Name", "Severity", "FME", "Subject",
    "Activity Owner Group", "Sub Root Cause", "Type work", "Complete Lat Lon", "Month",
]

# Same pattern sla_improvement.py already uses for the same purpose - the
# literal "Logic ของ Data เดิม" this request asks to reuse.
_TOG_PATTERN = re.compile(r"TRUE-TH-BBT-(NOR[12])-([A-Z0-9]+)-NOP")
_PROVINCE_CODE_MERGE = {"CMI1": "CMI", "CMI2": "CMI"}


def _extract_region_province(activity_owner_group):
    m = _TOG_PATTERN.match(str(activity_owner_group or "").strip())
    if not m:
        return None, None
    region, code = m.group(1), m.group(2)
    return region, _PROVINCE_CODE_MERGE.get(code, code)


def _haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance in meters - standard haversine formula."""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(min(1, math.sqrt(a)))


def _parse_complete_lat_lon(raw):
    """"lat, lon" comma-separated string -> (lat, lon) floats, or None if
    missing/malformed/out of Thailand's bounding box - never guessed."""
    if not raw or not str(raw).strip():
        return None
    parts = str(raw).split(",")
    if len(parts) != 2:
        return None
    try:
        lat, lon = float(parts[0].strip()), float(parts[1].strip())
    except ValueError:
        return None
    if lat == 0 or lon == 0:
        return None
    if not (5 <= lat <= 21 and 97 <= lon <= 106):  # rough Thailand bounding box
        return None
    return lat, lon


class ImportValidationError(Exception):
    pass


def fetch_temp_point_rows(gs_client):
    """Reads the "Map" worksheet raw (get_all_values, not get_all_records,
    since embedded repeated header rows would otherwise get parsed as
    garbage data). Returns (rows, warnings) where each row is a dict of
    the REQUIRED_COLUMNS plus derived region/province/lat/lon - rows with
    an unparseable Complete Lat Lon are still included (data is never
    dropped, per explicit request) but flagged invalid_coord=True."""
    sh = gs_client.open_by_key(TEMP_POINT_SHEET_ID)
    ws = None
    for candidate in sh.worksheets():
        if candidate.id == TEMP_POINT_WORKSHEET_GID:
            ws = candidate
            break
    if ws is None:
        ws = sh.worksheet(TEMP_POINT_WORKSHEET_NAME)

    raw_values = ws.get_all_values()
    if not raw_values:
        raise ImportValidationError("ไม่พบข้อมูลในชีท Map")

    header = [str(h).strip() for h in raw_values[0]]
    header_idx = {h: i for i, h in enumerate(header)}
    missing = [c for c in REQUIRED_COLUMNS if c not in header_idx]
    if missing:
        raise ImportValidationError(f"ชีท Map ขาด Column ที่จำเป็น: {', '.join(missing)}")

    get = lambda raw, col: raw[header_idx[col]] if col in header_idx and header_idx[col] < len(raw) else ""

    rows = []
    warnings = []
    skipped_embedded_headers = 0
    invalid_coord_count = 0
    unmapped_region_count = 0
    seen_point_keys = set()
    duplicate_count = 0

    for raw in raw_values[1:]:
        if not any(str(c).strip() for c in raw):
            continue  # fully blank row (e.g. a spacer between pasted blocks)
        # Embedded repeated header rows: the row's own "Source TT" cell
        # literally contains the text "Source TT" (its own header label) -
        # confirmed by direct inspection this is how the sheet's pasted-in
        # per-province blocks re-attach their header row mid-data.
        source_tt_val = get(raw, "Source TT")
        if source_tt_val.strip() == "Source TT":
            skipped_embedded_headers += 1
            continue

        activity_owner_group = get(raw, "Activity Owner Group")
        region, province = _extract_region_province(activity_owner_group)
        if region is None:
            unmapped_region_count += 1

        latlon = _parse_complete_lat_lon(get(raw, "Complete Lat Lon"))
        if latlon is None:
            invalid_coord_count += 1

        point_key = (get(raw, "Source TT").strip(), get(raw, "INC").strip())
        is_duplicate = point_key in seen_point_keys and point_key != ("", "")
        if is_duplicate:
            duplicate_count += 1
        seen_point_keys.add(point_key)

        rows.append({
            "source_tt": get(raw, "Source TT"), "inc": get(raw, "INC"),
            "site_cable": get(raw, "Site/Cable"), "ci_name": get(raw, "CI Name"),
            "severity": get(raw, "Severity"), "fme": get(raw, "FME"),
            "subject": get(raw, "Subject"), "activity_owner_group": activity_owner_group,
            "sub_root_cause": get(raw, "Sub Root Cause"), "type_work": get(raw, "Type work"),
            "complete_lat_lon_raw": get(raw, "Complete Lat Lon"), "month": get(raw, "Month"),
            "region": region, "province": province,
            "latitude": latlon[0] if latlon else None, "longitude": latlon[1] if latlon else None,
            "invalid_coord": latlon is None, "is_duplicate": is_duplicate,
        })

    if skipped_embedded_headers:
        warnings.append(f"ข้าม {skipped_embedded_headers} แถวที่เป็น Header ซ้ำฝังอยู่กลางข้อมูล")
    if invalid_coord_count:
        warnings.append(f"{invalid_coord_count} แถวมีพิกัดไม่ถูกต้อง/ไม่มีพิกัด (Invalid/Unmapped)")
    if unmapped_region_count:
        warnings.append(f"{unmapped_region_count} แถวระบุ Region/Province ไม่ได้จาก Activity Owner Group")
    if duplicate_count:
        warnings.append(f"พบ {duplicate_count} แถวซ้ำ (Source TT + INC ซ้ำกัน)")

    return rows, warnings


# ── Ordering + Sequential Chain Clustering ──────────────────────────────

def _nearest_neighbor_order(points):
    """Greedy nearest-neighbor walk - the sheet has no explicit
    route-sequence column, so this is the practical stand-in for "order
    points along the road alignment" per the request. Starts from the
    westmost-then-southmost point (an arbitrary but deterministic anchor)
    and repeatedly jumps to the nearest unvisited point."""
    if not points:
        return []
    remaining = list(points)
    remaining.sort(key=lambda p: (p["longitude"], p["latitude"]))
    ordered = [remaining.pop(0)]
    while remaining:
        last = ordered[-1]
        best_i, best_d = None, None
        for i, p in enumerate(remaining):
            d = _haversine_m(last["latitude"], last["longitude"], p["latitude"], p["longitude"])
            if best_d is None or d < best_d:
                best_d, best_i = d, i
        ordered.append(remaining.pop(best_i))
    return ordered


def build_clusters(rows):
    """Points with valid coordinates are grouped by province, ordered via
    nearest-neighbor walk within each province, then walked in that order
    applying the sequential <=500m chain rule. Adds distance_to_previous_m,
    cluster_id, cluster_distance_m, is_cluster to every valid point.
    Points with invalid/missing coordinates are returned separately
    (never silently dropped) with cluster_id="Invalid / Unmapped"."""
    valid = [r for r in rows if not r["invalid_coord"]]
    invalid = [r for r in rows if r["invalid_coord"]]
    invalid_out = [{**r, "cluster_id": "Invalid / Unmapped", "distance_to_previous_m": None, "cluster_distance_m": None, "is_cluster": False} for r in invalid]

    by_province = {}
    for r in valid:
        by_province.setdefault(r["province"] or "(ไม่ระบุ)", []).append(r)

    cluster_counter = 0
    out = []
    for province in sorted(by_province.keys()):
        ordered = _nearest_neighbor_order(by_province[province])
        # Walk the ordered chain, cutting a new cluster whenever the gap
        # to the immediately preceding point exceeds the threshold.
        current_chain = []
        chains = []
        prev = None
        for i, p in enumerate(ordered):
            if prev is None:
                dist = 0.0
            else:
                dist = _haversine_m(prev["latitude"], prev["longitude"], p["latitude"], p["longitude"])
            p_out = dict(p)
            p_out["distance_to_previous_m"] = round(dist, 1)
            if prev is not None and dist > CHAIN_DISTANCE_THRESHOLD_M:
                chains.append(current_chain)
                current_chain = []
            current_chain.append(p_out)
            prev = p
        if current_chain:
            chains.append(current_chain)

        for chain in chains:
            is_cluster = len(chain) > 1
            cumulative = 0.0
            for i, p in enumerate(chain):
                cumulative += p["distance_to_previous_m"]
                p["cluster_distance_m"] = round(cumulative, 1)
            if is_cluster:
                cluster_counter += 1
                cluster_id = f"C{cluster_counter:03d}"
            else:
                cluster_id = "Individual"
            for p in chain:
                p["cluster_id"] = cluster_id
                p["is_cluster"] = is_cluster
                out.append(p)

    return out, invalid_out


# ── Summary / Province table / Improvement priority ────────────────────

def build_summary(clustered, invalid):
    total_valid = len(clustered)
    cluster_points = [p for p in clustered if p["is_cluster"]]
    individual_points = [p for p in clustered if not p["is_cluster"]]
    cluster_ids = {p["cluster_id"] for p in cluster_points}
    total_cluster_distance = sum(
        max((p["cluster_distance_m"] for p in clustered if p["cluster_id"] == cid), default=0)
        for cid in cluster_ids
    )
    return {
        "total_temp_point": total_valid + len(invalid),
        "total_valid_point": total_valid,
        "cluster_point": len(cluster_points),
        "individual_point": len(individual_points),
        "invalid_point": len(invalid),
        "total_cluster": len(cluster_ids),
        "total_cluster_distance_m": round(total_cluster_distance, 1),
        "avg_cluster_distance_m": round(total_cluster_distance / len(cluster_ids), 1) if cluster_ids else 0,
        "pct_cluster_point": round(len(cluster_points) / total_valid * 100, 2) if total_valid else 0,
        "pct_individual_point": round(len(individual_points) / total_valid * 100, 2) if total_valid else 0,
    }


def build_province_summary(clustered):
    """Total Distance per province = sum of each COMPLETE cluster's own
    cluster_distance_m (the cluster's final/max cumulative value) - never
    summed from individual point-to-point gaps directly, and never
    including the gap BETWEEN two different clusters, per explicit
    "ต้องไม่เอา Distance ระหว่าง Cluster มารวม" instruction."""
    by_province = {}
    for p in clustered:
        by_province.setdefault(p["province"] or "(ไม่ระบุ)", []).append(p)

    rows = []
    for province, pts in by_province.items():
        cluster_pts = [p for p in pts if p["is_cluster"]]
        individual_pts = [p for p in pts if not p["is_cluster"]]
        cluster_ids = {p["cluster_id"] for p in cluster_pts}
        total_distance = sum(
            max((p["cluster_distance_m"] for p in pts if p["cluster_id"] == cid), default=0)
            for cid in cluster_ids
        )
        region = pts[0]["region"] if pts else None
        rows.append({
            "province": province, "region": region,
            "total_temp_point": len(pts), "cluster_point": len(cluster_pts),
            "cluster_count": len(cluster_ids), "individual_point": len(individual_pts),
            "total_distance_m": round(total_distance, 1),
            "avg_distance_per_cluster_m": round(total_distance / len(cluster_ids), 1) if cluster_ids else 0,
        })
    rows.sort(key=lambda r: -r["cluster_count"])
    return rows


def build_improvement_priority(province_summary):
    """🔴 High / 🟠 Medium / 🔵 Monitor - data-driven (median cluster count
    and median total distance among provinces that have at least one
    cluster), never hardcoded thresholds, per explicit request."""
    cluster_counts = sorted(r["cluster_count"] for r in province_summary if r["cluster_count"] > 0)
    distances = sorted(r["total_distance_m"] for r in province_summary if r["total_distance_m"] > 0)
    med_clusters = cluster_counts[len(cluster_counts) // 2] if cluster_counts else 0
    med_distance = distances[len(distances) // 2] if distances else 0

    out = []
    for r in province_summary:
        high_clusters = r["cluster_count"] >= med_clusters and r["cluster_count"] > 0
        high_distance = r["total_distance_m"] >= med_distance and r["total_distance_m"] > 0
        if high_clusters and high_distance:
            priority = "high"
        elif high_clusters or high_distance:
            priority = "medium"
        else:
            priority = "monitor"
        out.append({**r, "priority": priority})
    return out


DETAIL_TABLE_COLUMNS = [
    "Cluster ID", "Source TT", "INC", "Site/Cable", "CI Name", "Severity", "FME", "Subject",
    "Activity Owner Group", "Sub Root Cause", "Type work", "Complete Lat Lon",
    "Distance to Previous Point (m)", "Cluster Distance (m)", "Month",
]


def _point_to_detail_row(p):
    return {
        "Cluster ID": p["cluster_id"], "Source TT": p["source_tt"], "INC": p["inc"],
        "Site/Cable": p["site_cable"], "CI Name": p["ci_name"], "Severity": p["severity"],
        "FME": p["fme"], "Subject": p["subject"], "Activity Owner Group": p["activity_owner_group"],
        "Sub Root Cause": p["sub_root_cause"], "Type work": p["type_work"],
        "Complete Lat Lon": p["complete_lat_lon_raw"],
        "Distance to Previous Point (m)": p["distance_to_previous_m"],
        "Cluster Distance (m)": p["cluster_distance_m"], "Month": p["month"],
    }


def build_temp_point_response(gs_client=None):
    if gs_client is None:
        from pending_trend import get_drive_and_sheets_clients
        _, gs_client = get_drive_and_sheets_clients()

    rows, warnings = fetch_temp_point_rows(gs_client)
    clustered, invalid = build_clusters(rows)
    summary = build_summary(clustered, invalid)
    province_summary = build_province_summary(clustered)
    province_priority = build_improvement_priority(province_summary)

    points_out = [{
        "cluster_id": p["cluster_id"], "is_cluster": p["is_cluster"],
        "source_tt": p["source_tt"], "inc": p["inc"], "site_cable": p["site_cable"],
        "ci_name": p["ci_name"], "severity": p["severity"], "fme": p["fme"], "subject": p["subject"],
        "activity_owner_group": p["activity_owner_group"], "sub_root_cause": p["sub_root_cause"],
        "type_work": p["type_work"], "month": p["month"],
        "region": p["region"], "province": p["province"],
        "latitude": p["latitude"], "longitude": p["longitude"],
        "distance_to_previous_m": p["distance_to_previous_m"], "cluster_distance_m": p["cluster_distance_m"],
    } for p in clustered]
    invalid_out = [{
        "source_tt": p["source_tt"], "inc": p["inc"], "site_cable": p["site_cable"],
        "ci_name": p["ci_name"], "severity": p["severity"], "province": p["province"],
        "complete_lat_lon_raw": p["complete_lat_lon_raw"],
    } for p in invalid]

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "warnings": warnings,
        "summary": summary,
        "province_summary": province_priority,
        "points": points_out,
        "invalid_points": invalid_out,
        "detail_table_columns": DETAIL_TABLE_COLUMNS,
    }


# ── Export ───────────────────────────────────────────────────────────

def build_export_rows(clustered):
    return [_point_to_detail_row(p) for p in clustered]

