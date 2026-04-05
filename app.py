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

@app.get("/api/health")
def api_health():
    return jsonify({"success": True, "message": f"{APP_NAME} çalışıyor."})

@app.get("/api/matches")
def api_matches():
    rows = fetch_all(
        '''
        SELECT id, home_team AS home, away_team AS away, country_name AS country,
               league_name AS league, TO_CHAR(starting_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS date,
               status
        FROM fixtures
        WHERE DATE(starting_at_utc AT TIME ZONE 'UTC') IN (CURRENT_DATE, CURRENT_DATE + 1)
        ORDER BY starting_at_utc ASC
        '''
    )
    return jsonify({"success": True, "matches": rows})

@app.get("/api/coupons/today")
def api_coupons_today():
    package = get_today_coupon_package()
    if not package:
        today = fetch_one("SELECT TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS coupon_date")
        return jsonify({
            "success": False,
            "message": "Bugün için kupon paketi henüz üretilmedi.",
            "coupon_date": today["coupon_date"],
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

@app.post("/api/analyze")
def api_analyze():
    payload = request.get_json(silent=True) or {}
    fixture_id = payload.get("match_id")
    if not fixture_id:
        return jsonify({"success": False, "message": "match_id zorunludur."}), 400

    match_row = fetch_one(
        '''
        SELECT id, home_team AS home, away_team AS away, country_name AS country, league_name AS league,
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
        return jsonify({"success": False, "message": "Bu maç için yeterli odds verisi yok."}), 404

    home_pct = draw_pct = away_pct = 0
    if prediction["selection_key"] == "MS1":
        home_pct = prediction["confidence_score"]
    elif prediction["selection_key"] == "MSX":
        draw_pct = prediction["confidence_score"]
    elif prediction["selection_key"] == "MS2":
        away_pct = prediction["confidence_score"]

    return jsonify({
        "success": True,
        "match": match_row,
        "prediction": {
            "home": round(home_pct, 1),
            "draw": round(draw_pct, 1),
            "away": round(away_pct, 1),
            "over25": 0,
            "btts_yes": 0
        },
        "summary": {
            "count": 1,
            "ev": round(home_pct, 1),
            "ber": round(draw_pct, 1),
            "dep": round(away_pct, 1)
        },
        "prediction_texts": [
            f"Basit model en güçlü seçimi {prediction['selection_key']} olarak görüyor."
        ],
        "recommendations": [
            f"Tavsiye: {prediction['selection_key']} | Güven %{prediction['confidence_score']}"
        ]
    })

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


@app.route("/api/run-update")
def run_update():
    from flask import request
    import subprocess

    if request.args.get("key") != "123456":
        return {"status": "unauthorized"}, 401

    try:
        subprocess.run(["python", "run_data_update.py"], check=True)
        return {"status": "ok", "message": "update started"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
