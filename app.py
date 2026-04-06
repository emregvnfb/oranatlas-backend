import os
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from settings import APP_NAME, SECRET_KEY, ADMIN_PASSWORD
from db import fetch_all, fetch_one, execute
from services.prediction_service import build_simple_prediction
from services.coupon_service import get_today_coupon_package, generate_daily_coupon_package

try:
    from services.prediction_service_v2 import PredictionServiceV2
except Exception:
    PredictionServiceV2 = None

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


prediction_service_v2 = PredictionServiceV2(db_path="matches.db") if PredictionServiceV2 else None


def get_match_row_for_analysis(fixture_id):
    return fetch_one(
        '''
        SELECT f.id,
               f.home_team AS home,
               f.away_team AS away,
               f.country_name AS country,
               f.league_name AS league,
               TO_CHAR(f.starting_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS date,
               f.status
        FROM fixtures f
        WHERE f.id = %s
        ''',
        (fixture_id,),
    )


def get_normalized_odds_for_fixture(fixture_id):
    rows = fetch_all(
        '''
        SELECT lower(coalesce(m.market_key, '')) AS market_key,
               lower(coalesce(ms.selection_key, '')) AS selection_key,
               MAX(ol.latest_odd_decimal) AS best_odd
        FROM odds_latest ol
        JOIN markets m ON ol.market_id = m.id
        JOIN market_selections ms ON ol.selection_id = ms.id
        WHERE ol.fixture_id = %s
        GROUP BY lower(coalesce(m.market_key, '')),
                 lower(coalesce(ms.selection_key, ''))
        ''',
        (fixture_id,),
    ) or []

    odds_data = {}

    def set_if_empty(key, value):
        try:
            value = float(value)
        except Exception:
            return
        if value <= 1.01:
            return
        if key not in odds_data:
            odds_data[key] = value

    for row in rows:
        market_key = str(row.get("market_key") or "")
        selection_key = str(row.get("selection_key") or "")
        odd = row.get("best_odd")

        # Match Winner / 1X2
        if (
            market_key == "match winner"
            or ("match" in market_key and "winner" in market_key)
            or market_key in {"1x2", "fulltime result", "full time result"}
        ):
            if selection_key in {"home", "1"}:
                set_if_empty("home_win", odd)
            elif selection_key in {"draw", "x"}:
                set_if_empty("draw", odd)
            elif selection_key in {"away", "2"}:
                set_if_empty("away_win", odd)

        # Double chance
        if (
            "double chance" in market_key
            or market_key in {"double_chance", "dc"}
        ):
            if selection_key in {"1x", "home/draw", "1-x"}:
                set_if_empty("double_chance_1x", odd)
            elif selection_key in {"x2", "draw/away", "x-2"}:
                set_if_empty("double_chance_x2", odd)
            elif selection_key in {"12", "home/away", "1-2"}:
                set_if_empty("double_chance_12", odd)

        # Both teams to score
        if (
            "both teams score" in market_key
            or "both teams to score" in market_key
            or "btts" in market_key
        ):
            if selection_key in {"yes", "gg", "btts yes"}:
                set_if_empty("btts_yes", odd)
            elif selection_key in {"no", "ng", "btts no"}:
                set_if_empty("btts_no", odd)

        # Over / Under
        if "over" in selection_key and "1.5" in selection_key:
            set_if_empty("over_1_5", odd)
        if "under" in selection_key and "3.5" in selection_key:
            set_if_empty("under_3_5", odd)

    return odds_data


def build_feature_row_from_fixture(match_row, fixture_id):
    feature_row = fetch_one(
        '''
        SELECT *
        FROM fixture_features
        WHERE fixture_id = %s
        LIMIT 1
        ''',
        (fixture_id,),
    ) or {}

    if not isinstance(feature_row, dict):
        feature_row = dict(feature_row)

    return {
        "match_id": fixture_id,
        "league_name": match_row.get("league") or "",
        "home_team": match_row.get("home") or "",
        "away_team": match_row.get("away") or "",
        "match_date": match_row.get("date") or "",
        "home_form": float(feature_row.get("home_form") or feature_row.get("home_form_score") or 0.50),
        "away_form": float(feature_row.get("away_form") or feature_row.get("away_form_score") or 0.50),
        "home_attack": float(feature_row.get("home_attack") or feature_row.get("home_attack_score") or 1.20),
        "away_attack": float(feature_row.get("away_attack") or feature_row.get("away_attack_score") or 1.10),
        "home_defense": float(feature_row.get("home_defense") or feature_row.get("home_defense_score") or 1.00),
        "away_defense": float(feature_row.get("away_defense") or feature_row.get("away_defense_score") or 1.00),
        "home_points_per_match": float(feature_row.get("home_points_per_match") or feature_row.get("home_ppm") or 1.40),
        "away_points_per_match": float(feature_row.get("away_points_per_match") or feature_row.get("away_ppm") or 1.20),
        "home_goals_for": float(feature_row.get("home_goals_for") or feature_row.get("home_avg_goals_for") or 1.40),
        "away_goals_for": float(feature_row.get("away_goals_for") or feature_row.get("away_avg_goals_for") or 1.20),
        "home_goals_against": float(feature_row.get("home_goals_against") or feature_row.get("home_avg_goals_against") or 1.10),
        "away_goals_against": float(feature_row.get("away_goals_against") or feature_row.get("away_avg_goals_against") or 1.10),
        "home_win_rate": float(feature_row.get("home_win_rate") or 0.45),
        "away_win_rate": float(feature_row.get("away_win_rate") or 0.30),
        "draw_rate": float(feature_row.get("draw_rate") or 0.26),
    }


def build_v2_prediction_response(fixture_id, match_row):
    if not prediction_service_v2:
        return None

    odds_data = get_normalized_odds_for_fixture(fixture_id)
    if not odds_data:
        return None

    feature_row = build_feature_row_from_fixture(match_row, fixture_id)
    result = prediction_service_v2.analyze_match(feature_row, odds_data)

    if not result.best_market:
        return {
            "engine": "v2",
            "available": True,
            "message": "Uygun market bulunamadı.",
            "all_predictions": [],
            "summary": result.summary,
        }

    best = result.best_market
    return {
        "engine": "v2",
        "available": True,
        "best_market": {
            "market_key": best.market_key,
            "market_name": best.market_name,
            "selection": best.selection,
            "probability": best.probability,
            "implied_probability": best.implied_probability,
            "odds": best.odds,
            "ev": best.ev,
            "confidence": best.confidence,
            "league_weight": best.league_weight,
            "score": best.score,
            "reason": best.reason,
        },
        "all_predictions": [
            {
                "market_key": item.market_key,
                "market_name": item.market_name,
                "selection": item.selection,
                "probability": item.probability,
                "implied_probability": item.implied_probability,
                "odds": item.odds,
                "ev": item.ev,
                "confidence": item.confidence,
                "league_weight": item.league_weight,
                "score": item.score,
                "reason": item.reason,
            }
            for item in result.all_predictions
        ],
        "summary": result.summary,
    }


@app.get("/api/debug-routes")
def debug_routes():
    return {"routes": sorted([str(rule) for rule in app.url_map.iter_rules()])}


@app.get("/api/debug-version")
def debug_version():
    return {
        "version": "ORANATLAS DEBUG BUILD V6",
        "message": "Bu doğru app.py çalışıyor",
        "v2_prediction_enabled": prediction_service_v2 is not None
    }


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
        SELECT f.id,
               f.home_team AS home,
               f.away_team AS away,
               f.league_name AS league,
               f.country_name AS country,
               TO_CHAR(f.starting_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS date,
               f.status,
               f.starting_at_utc
        FROM fixtures f
        WHERE DATE(f.starting_at_utc AT TIME ZONE 'UTC') IN (CURRENT_DATE, CURRENT_DATE + 1)
          AND EXISTS (
              SELECT 1
              FROM odds_latest ol
              WHERE ol.fixture_id = f.id
          )
        ORDER BY f.starting_at_utc ASC
        LIMIT 50
        '''
    )

    result = []
    for row in rows:
        row = dict(row)
        row.pop("starting_at_utc", None)
        result.append(row)

    return jsonify({"success": True, "count": len(result), "matches": result})


@app.post("/api/analyze")
def api_analyze():
    payload = request.get_json(silent=True) or {}
    fixture_id = payload.get("match_id")
    use_v2 = bool(payload.get("use_v2", True))

    if not fixture_id:
        return jsonify({"success": False, "message": "match_id zorunludur."}), 400

    fixture_id = int(fixture_id)
    match_row = get_match_row_for_analysis(fixture_id)
    if not match_row:
        return jsonify({"success": False, "message": "Maç bulunamadı."}), 404

    if use_v2 and prediction_service_v2:
        v2_prediction = build_v2_prediction_response(fixture_id, match_row)
        if v2_prediction:
            return jsonify({"success": True, "match": match_row, "prediction": v2_prediction})

    prediction = build_simple_prediction(fixture_id)
    if not prediction:
        return jsonify({"success": False, "message": "Odds yok", "match": match_row}), 404

    return jsonify({
        "success": True,
        "match": match_row,
        "prediction": {
            "engine": "legacy",
            "available": True,
            "best_market": prediction
        }
    })


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
        return jsonify({"success": False, "message": "Bugün için sonuç cache kaydı yok.", "data": None})
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
        package = generate_daily_coupon_package(datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        return jsonify({"success": True, "message": "Bugünün kuponu üretildi.", "data": package})

    return jsonify({"success": False, "message": "Geçersiz işlem."}), 400


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
