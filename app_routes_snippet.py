# ── Paste this into app.py, right after the existing
# @app.route('/api/temp-point-improvement') view function (around the
# line that starts @app.route('/api/temp-point-improvement/export')) ──

@app.route('/api/temp-point-improvement/group', methods=['POST'])
def api_temp_point_improvement_group():
    try:
        payload = request.get_json(force=True) or {}
        source_tts = [str(tt).strip() for tt in (payload.get('source_tts') or []) if str(tt).strip()]
        if len(source_tts) < 2:
            return jsonify({'error': 'ต้องเลือกอย่างน้อย 2 จุดเพื่อจัดเป็น Cluster'}), 400
        _, gs_client = get_drive_and_sheets_clients()
        updated_by = session.get('user_email')
        manual_group_id = temp_point_improvement.save_manual_action(gs_client, source_tts, 'group', updated_by=updated_by)
        return jsonify({'ok': True, 'manual_group_id': manual_group_id})
    except Exception as e:
        log.exception("temp-point-improvement manual group failed")
        return jsonify({'error': str(e)}), 500


@app.route('/api/temp-point-improvement/individual', methods=['POST'])
def api_temp_point_improvement_individual():
    try:
        payload = request.get_json(force=True) or {}
        source_tts = [str(tt).strip() for tt in (payload.get('source_tts') or []) if str(tt).strip()]
        if not source_tts:
            return jsonify({'error': 'ไม่มีจุดที่เลือก'}), 400
        _, gs_client = get_drive_and_sheets_clients()
        updated_by = session.get('user_email')
        temp_point_improvement.save_manual_action(gs_client, source_tts, 'individual', updated_by=updated_by)
        return jsonify({'ok': True})
    except Exception as e:
        log.exception("temp-point-improvement manual individual failed")
        return jsonify({'error': str(e)}), 500


# Optional (not wired to any UI button yet, but available if you want an
# "undo / revert to auto" affordance later): removes the override rows for
# the given source_tt list, so they fall back to the automatic
# road-alignment clustering again on the next load.
@app.route('/api/temp-point-improvement/clear-override', methods=['POST'])
def api_temp_point_improvement_clear_override():
    try:
        payload = request.get_json(force=True) or {}
        source_tts = [str(tt).strip() for tt in (payload.get('source_tts') or []) if str(tt).strip()]
        if not source_tts:
            return jsonify({'error': 'ไม่มีจุดที่เลือก'}), 400
        _, gs_client = get_drive_and_sheets_clients()
        temp_point_improvement.clear_manual_overrides(gs_client, source_tts)
        return jsonify({'ok': True})
    except Exception as e:
        log.exception("temp-point-improvement clear override failed")
        return jsonify({'error': str(e)}), 500
