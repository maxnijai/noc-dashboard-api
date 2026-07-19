"""
ticket_views.py
----------------
Shared "which 4 groups do we split tickets into" definition, used by both
realtime_monitor.py (live sheet) and pending_trend.py's repeat-ticket
aggregation (historical Drive snapshots). Kept in its own tiny module so
neither of those two files has to import the other.
"""

BOOKMARK_VIEWS = {
    "FBB":      {"label": "4.FBB with SA1-4",           "bookmark": "4.FBB with SA1-4", "severity": None},
    "NW_NSA12": {"label": "3. All NW Incident NSA1-2",   "bookmark": "3. All NW Incident NSA1-2", "severity": None},
    "MB":       {"label": "7.MB with SA1-4",             "bookmark": "7.MB with SA1-4", "severity": None},
    "NSA34":    {"label": "NSA3 / NSA4",                 "bookmark": None, "severity": {"NSA3", "NSA4"}},
}


def row_matches_view(row, view_key):
    view = BOOKMARK_VIEWS[view_key]
    if view["bookmark"] is not None:
        return str(row.get("Bookmark", "")).strip() == view["bookmark"]
    if view["severity"] is not None:
        return str(row.get("SEVERITY", "")).strip() in view["severity"]
    return False
