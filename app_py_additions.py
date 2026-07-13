"""
Additions for app.py — Pending Ticket Trend (Tap #1)
=====================================================
Copy the pieces below into app.py. They're kept separate here so they're easy
to diff/review before merging into the 1,960-line file.

1) requirements.txt — add these two lines:
   openpyxl==3.1.5
   google-api-python-client==2.149.0

2) Near the top of app.py, alongside the other imports:
"""

from pending_trend import (
    run_nightly_job,
    build_api_response,
    get_drive_and_sheets_clients,
    PENDING_TREND_SHEET,
)

"""
3) Extend the existing get_client() scope (it currently only requests
   spreadsheets.readonly, which won't allow Drive file listing/download).
   Simplest fix: leave get_client() as-is for the rest of the app, and let
   pending_trend.py use its own get_drive_and_sheets_clients() with the wider
   scope — same GOOGLE_CREDENTIALS_JSON env var, no extra secret needed.
   Just make sure the service account has been given at least Viewer access
   to the "Sheet_Backups" Drive folder (share it with the service account's
   client_email, the same way SHEET_ID / FIREBURN_SHEET_ID are shared today).

4) Register the nightly job on the existing APScheduler instance
   (app.py already creates one for the 6-hour cache rebuild):
"""

# scheduler.add_job(
#     lambda: run_nightly_job(SHEET_ID),
#     'cron', hour=1, minute=35, id='pending_trend_nightly',
#     misfire_grace_time=3600,   # tolerate the box waking up late
# )

"""
5) New route — add near the other @app.route('/api/...') definitions:
"""

# @app.route('/api/pending-trend')
# def api_pending_trend():
#     period = request.args.get('period', '14d')          # '7d' | '14d' | '21d' | '1m'
#     trueowner = request.args.get('trueowner') or None    # e.g. 'TRUE-TH-BBT-NOR1-CMI1-NOP'
#     try:
#         _, gs_client = get_drive_and_sheets_clients()
#         data = build_api_response(gs_client, SHEET_ID, period=period, trueowner_filter=trueowner)
#         return jsonify(data)
#     except Exception as e:
#         log.exception("pending-trend API failed")
#         return jsonify({"error": str(e)}), 500

"""
Note: `request` needs to be imported from flask alongside the existing
Flask, jsonify, render_template, send_from_directory import at the top of app.py:

    from flask import Flask, jsonify, render_template, send_from_directory, request

6) One-off backfill (run manually once after deploying, to seed history for
   however many days of hourly backups still exist in the Drive folder):

    from datetime import date, timedelta
    from pending_trend import run_nightly_job
    d = date.today()
    for i in range(30):                      # walks backward up to 30 days
        ok = run_nightly_job(SHEET_ID, for_date=d - timedelta(days=i))
        if not ok:
            break   # stop once backups no longer exist that far back
"""
