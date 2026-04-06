import os
from flask import Flask, jsonify, request
from settings import APP_NAME, SECRET_KEY, ADMIN_PASSWORD
from db import fetch_all, fetch_one, execute
from services.prediction_service import build_simple_prediction
from services.coupon_service import get_today_coupon_package, generate_daily_coupon_package

app = Flask(__name__)
app.secret_key = SECRET_KEY


def safe_json_load(value, default):
    import json
    try:
        if value is None:
            return default
        if isinstance(value, (dict, list)):
            return value
        return json.loads(value)
    except Exception:
        return default


# ================= DEBUG =================
@app.get("/api/debug-routes")
def debug_routes():
    return {
        "routes": sorted([str(rule) for rule in app.url_map.iter_rules()])
    }


@app.get("/api/debug-version")
def debug_version():
    return {
        "version": "ORANATLAS DEBUG BUILD V1",
        "message": "Bu doğru app.py çalışıyor"
    }


# ================= CORE =================
@app.get("/api/health")
def api_health():
    return jsonify({"success": True, "message": f"{APP_NAME} çalışıyor."})


@app.get("/api/matches")
def api_matches():
    rows = fetch_all(
        '''
        SELECT id,
               home_team AS home,
               away_team AS away,
               country_name AS country,
               league_name AS league,
               TO_CHAR(starting_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS date,
               status
        FROM fixtures
        WHERE DATE(starting_at_utc AT TIME ZONE 'UTC') IN (CURRENT_DATE, CURRENT_DATE + 1)
        ORDER BY starting_at_utc ASC
        '''
    )
    return jsonify({"success": True, "matches": rows})


@app.post("/api/analyze")
def api_analyze():
    payload = request.get_json(silent=True) or {}
    fixture_id = payload.get("match_id")

    if not fixture_id:
        return jsonify({"success": False, "message": "match_id zorunludur."}), 400

    match_row = fetch_one(
        '''
        SELECT id,
               home_team AS home,
               away_team AS away
        FROM fixtures
        WHERE id = %s
        ''',
        (fixture_id,),
    )

    if not match_row:
        return jsonify({"success": False, "message": "Maç bulunamadı."}), 404

    prediction = build_simple_prediction(int(fixture_id))

    if not prediction:
        return jsonify({"success": False, "message": "Odds yok"}), 404

    return jsonify({
        "success": True,
        "match": match_row,
        "prediction": prediction
    })


# ================= SYSTEM =================
@app.get("/api/init-db")
def init_db_route():
    import subprocess

    if request.args.get("key") != "123456":
        return jsonify({"status": "unauthorized"}), 401

    subprocess.run(["python", "init_db.py"])
    return jsonify({"status": "ok"})


@app.get("/api/run-update")
def run_update():
    import subprocess

    if request.args.get("key") != "123456":
        return jsonify({"status": "unauthorized"}), 401

    subprocess.Popen(
        ["python", "run_data_update.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )

    return jsonify({"status": "ok", "message": "background started"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
