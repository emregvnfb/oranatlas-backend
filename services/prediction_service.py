from db import fetch_one, fetch_all


BET_LABEL_TO_SELECTION = {
    "MS1": ["Home", "1"],
    "MSX": ["Draw", "X"],
    "MS2": ["Away", "2"],
}


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _fetch_fixture_context(fixture_id: int):
    return fetch_one(
        '''
        SELECT f.id,
               f.home_team,
               f.away_team,
               f.league_name,
               f.country_name
        FROM fixtures f
        WHERE f.id = %s
        ''',
        (fixture_id,),
    )


def _fetch_feature_row(fixture_id: int):
    try:
        return fetch_one(
            '''
            SELECT *
            FROM fixture_features
            WHERE fixture_id = %s
            LIMIT 1
            ''',
            (fixture_id,),
        )
    except Exception:
        return None


def _fetch_match_winner_odds(fixture_id: int):
    rows = fetch_all(
        '''
        SELECT ms.selection_key, MAX(ol.latest_odd_decimal) AS best_odd
        FROM odds_latest ol
        JOIN markets m ON ol.market_id = m.id
        JOIN market_selections ms ON ol.selection_id = ms.id
        WHERE ol.fixture_id = %s
          AND (
                lower(m.market_key) = 'match winner'
                OR (lower(m.market_key) LIKE '%%match%%' AND lower(m.market_key) LIKE '%%winner%%')
              )
        GROUP BY ms.selection_key
        ''',
        (fixture_id,),
    ) or []

    result = {}
    for row in rows:
        key = str(row.get("selection_key") or "").strip().lower()
        odd = _safe_float(row.get("best_odd"), 0)
        if key:
            result[key] = odd
    return result


def _normalize_three_way_odds(odds_map: dict):
    home = None
    draw = None
    away = None

    for key, value in odds_map.items():
        if key in ("home", "1"):
            home = _safe_float(value, 0)
        elif key in ("draw", "x"):
            draw = _safe_float(value, 0)
        elif key in ("away", "2"):
            away = _safe_float(value, 0)

    return home, draw, away


def _confidence_from_features(feature_row):
    if not feature_row:
        return 50.0

    home_form = _safe_float(feature_row.get("home_form"), 0.50)
    away_form = _safe_float(feature_row.get("away_form"), 0.50)
    home_ppm = _safe_float(feature_row.get("home_points_per_match"), 1.30)
    away_ppm = _safe_float(feature_row.get("away_points_per_match"), 1.20)
    home_attack = _safe_float(feature_row.get("home_attack"), 1.20)
    away_attack = _safe_float(feature_row.get("away_attack"), 1.10)
    home_def = _safe_float(feature_row.get("home_defense"), 1.00)
    away_def = _safe_float(feature_row.get("away_defense"), 1.00)

    edge = 0.0
    edge += (home_form - away_form) * 22
    edge += (home_ppm - away_ppm) * 10
    edge += (home_attack - away_def) * 9
    edge -= (away_attack - home_def) * 7

    return max(35.0, min(78.0, 50.0 + edge))


def build_simple_prediction(fixture_id: int):
    fixture = _fetch_fixture_context(int(fixture_id))
    if not fixture:
        return None

    feature_row = _fetch_feature_row(int(fixture_id))
    odds_map = _fetch_match_winner_odds(int(fixture_id))
    home_odd, draw_odd, away_odd = _normalize_three_way_odds(odds_map)

    if not any([home_odd, draw_odd, away_odd]):
        return None

    confidence_home = _confidence_from_features(feature_row)
    confidence_draw = max(30.0, min(55.0, 52.0 - abs(confidence_home - 50.0) * 0.6))
    confidence_away = max(30.0, min(75.0, 100.0 - confidence_home + 5.0))

    options = []

    if home_odd and home_odd >= 1.20:
        options.append({
            "selection_key": "MS1",
            "selection_label": "MS 1",
            "market_name": "Maç Sonucu",
            "odd": round(home_odd, 2),
            "confidence_score": round(confidence_home, 2),
        })

    if draw_odd and draw_odd >= 1.20:
        options.append({
            "selection_key": "MSX",
            "selection_label": "MS X",
            "market_name": "Maç Sonucu",
            "odd": round(draw_odd, 2),
            "confidence_score": round(confidence_draw, 2),
        })

    if away_odd and away_odd >= 1.20:
        options.append({
            "selection_key": "MS2",
            "selection_label": "MS 2",
            "market_name": "Maç Sonucu",
            "odd": round(away_odd, 2),
            "confidence_score": round(confidence_away, 2),
        })

    if not options:
        return None

    options.sort(key=lambda x: (x["confidence_score"], -abs(x["odd"] - 1.45)), reverse=True)
    best = options[0]

    return {
        "fixture_id": int(fixture_id),
        "home_team": fixture.get("home_team"),
        "away_team": fixture.get("away_team"),
        "league_name": fixture.get("league_name"),
        "country_name": fixture.get("country_name"),
        "market_key": "match_winner",
        "market_name": best["market_name"],
        "selection_key": best["selection_key"],
        "selection_label": best["selection_label"],
        "bet_label": best["selection_label"],
        "bet_odd": best["odd"],
        "odd": best["odd"],
        "confidence_score": best["confidence_score"],
        "reason": "legacy fallback prediction",
    }