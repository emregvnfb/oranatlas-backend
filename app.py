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
        "version": "ORANATLAS DEBUG BUILD V2",
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


@app.get("/api/analyzable-matches")
def analyzable_matches():
    rows = fetch_all(
        '''
        SELECT DISTINCT
               f.id,
               f.home_team AS home,
               f.away_team AS away,
               f.league_name AS league,
               f.country_name AS country,
               TO_CHAR(f.starting_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS date,
               f.status
        FROM fixtures f
        JOIN odds_latest ol ON ol.fixture_id = f.id
        WHERE DATE(f.starting_at_utc AT TIME ZONE 'UTC') IN (CURRENT_DATE, CURRENT_DATE + 1)
        ORDER BY f.starting_at_utc ASC
        LIMIT 50
        '''
    )
    return jsonify({
        "success": True,
        "count": len(rows),
        "matches": rows
    })


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
               away_team AS away,
               country_name AS country,
               league_name AS league,
               TO_CHAR(starting_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS date
        FROM fixtures
        WHERE id = %s
        ''',
        (fixture_id,),
    )

    if not match_row:
        return jsonify({"success": False, "message": "Maç bulunamadı."}), 404

    prediction = build_simple_prediction(int(fixture_id))

    if not prediction:
        return jsonify({"success": False, "message": "Odds yok", "match": match_row}), 404

    return jsonify({
        "success": True,
        "match": match_row,
        "prediction": prediction
    })


# ================= COUPONS =================
@app.get("/api/coupons/today")
def api_coupons_today():
    package = get_today_coupon_package()
    if not package:
        today = fetch_one("SELECT TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS coupon_date")
        return jsonify({
            "success": False,
            "message": "Bugün için kupon paketi henüz üretilmedi.",
            "coupon_date": today["coupon_date"] if today else None,
            "pool": [],
            "high_odd_coupons": [],
            "coupons_3": [],
            "coupons_4": [],
            "coupons_5": [],
            "coupons_6": []
        })
    return jsonify({"success": True, "data": safe_json_load(package, {})})


@app.get("/api/coupon-results/today")
def api_coupon_results_today():
    row = fetch_one(
        '''
        SELECT result_json
        FROM coupon_result_cache
        WHERE coupon_date = CURRENT_DATE
        '''
    )
    if not row:
        return jsonify({
            "success": False,
            "message": "Bugün için sonuç cache kaydı yok.",
            "data": None
        })
    return jsonify({"success": True, "data": safe_json_load(row["result_json"], {})})


@app.get("/api/editor-coupon")
def api_editor_coupon_get():
    row = fetch_one("SELECT coupon_text FROM editor_coupon ORDER BY updated_at DESC LIMIT 1")
    return jsonify({"success": True, "text": row["coupon_text"] if row else ""})


@app.post("/api/editor-coupon")
def api_editor_coupon_save():
    payload = request.get_json(silent=True) or {}
    password = str(payload.get("password") or "")
    if password != ADMIN_PASSWORD:
        return jsonify({"success": False, "message": "Şifre yanlış."}), 403

    text = str(payload.get("text") or "").strip()
    execute("DELETE FROM editor_coupon")
    execute(
        "INSERT INTO editor_coupon (coupon_text, updated_at) VALUES (%s, NOW())",
        (text,)
    )
    return jsonify({"success": True, "message": "Editör kuponu kaydedildi."})


@app.delete("/api/editor-coupon")
def api_editor_coupon_delete():
    payload = request.get_json(silent=True) or {}
    password = str(payload.get("password") or "")
    if password != ADMIN_PASSWORD:
        return jsonify({"success": False, "message": "Şifre yanlış."}), 403

    execute("DELETE FROM editor_coupon")
    return jsonify({"success": True, "message": "Editör kuponu silindi."})


@app.post("/api/admin/action")
def api_admin_action():
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "")
    password = str(payload.get("password") or "")
    if password and password != ADMIN_PASSWORD:
        return jsonify({"success": False, "message": "Şifre yanlış."}), 403

    if action == "delete_today_coupon":
        execute("DELETE FROM coupon_packages WHERE coupon_date = CURRENT_DATE AND coupon_type = 'daily'")
        return jsonify({"success": True, "message": "Bugünün kuponu silindi."})

    if action == "generate_today_coupon":
        from datetime import datetime
        package = generate_daily_coupon_package(datetime.utcnow().strftime("%Y-%m-%d"))
        return jsonify({"success": True, "message": "Bugünün kuponu üretildi.", "data": package})

    return jsonify({"success": False, "message": "Geçersiz işlem."}), 400


# ================= SYSTEM =================
@app.get("/api/init-db")
def init_db_route():
    import subprocess

    if request.args.get("key") != "123456":
        return jsonify({"status": "unauthorized"}), 401

    try:
        subprocess.run(["python", "init_db.py"], check=True)
        return jsonify({"status": "ok", "message": "db initialized"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.get("/api/run-update")
def run_update():
    import subprocess

    if request.args.get("key") != "123456":
        return jsonify({"status": "unauthorized"}), 401

    try:
        subprocess.Popen(
            ["python", "run_data_update.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True
        )
        return jsonify({"status": "ok", "message": "update started in background"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
