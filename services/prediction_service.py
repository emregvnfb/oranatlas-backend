from db import fetch_one, fetch_all, execute
from datetime import datetime, timezone


def build_simple_fixture_feature(fixture_id: int):
    rows = fetch_all(
        '''
        SELECT 
            m.market_key,
            ms.selection_key,
            ol.latest_odd_decimal
        FROM odds_latest ol
        JOIN markets m ON ol.market_id = m.id
        JOIN market_selections ms ON ol.selection_id = ms.id
        WHERE ol.fixture_id = %s
        ''',
        (fixture_id,),
    )

    home_odd = draw_odd = away_odd = None

    for row in rows:
        market = str(row["market_key"] or "").strip().lower()
        selection = str(row["selection_key"] or "").strip().lower()

        if "match" in market and "winner" in market:
            if selection in ["home", "1"]:
                home_odd = float(row["latest_odd_decimal"])
            elif selection in ["draw", "x"]:
                draw_odd = float(row["latest_odd_decimal"])
            elif selection in ["away", "2"]:
                away_odd = float(row["latest_odd_decimal"])

    odds = []
    labels = []

    if home_odd:
        odds.append(home_odd)
        labels.append("home")
    if draw_odd:
        odds.append(draw_odd)
        labels.append("draw")
    if away_odd:
        odds.append(away_odd)
        labels.append("away")

    if len(odds) < 2:
        return None

    inv_sum = sum(1 / odd for odd in odds)
    probs = [(1 / odd) / inv_sum for odd in odds]

    consensus_home = 0
    consensus_draw = 0
    consensus_away = 0

    for label, prob in zip(labels, probs):
        if label == "home":
            consensus_home = round(prob, 6)
        elif label == "draw":
            consensus_draw = round(prob, 6)
        elif label == "away":
            consensus_away = round(prob, 6)

    execute(
        '''
        INSERT INTO fixture_features (
            fixture_id, consensus_home_prob, consensus_draw_prob, consensus_away_prob, created_at
        )
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id)
        DO UPDATE SET
            consensus_home_prob = EXCLUDED.consensus_home_prob,
            consensus_draw_prob = EXCLUDED.consensus_draw_prob,
            consensus_away_prob = EXCLUDED.consensus_away_prob,
            created_at = EXCLUDED.created_at
        ''',
        (
            fixture_id,
            consensus_home,
            consensus_draw,
            consensus_away,
            datetime.now(timezone.utc),
        ),
    )

    return {
        "consensus_home_prob": consensus_home,
        "consensus_draw_prob": consensus_draw,
        "consensus_away_prob": consensus_away,
    }


def build_simple_prediction(fixture_id: int, model_version: str = "baseline_v1"):
    feature = fetch_one(
        '''
        SELECT consensus_home_prob, consensus_draw_prob, consensus_away_prob
        FROM fixture_features
        WHERE fixture_id = %s
        ''',
        (fixture_id,),
    )

    if not feature:
        feature = build_simple_fixture_feature(fixture_id)
        if not feature:
            return None

    scores = {
        "MS1": float(feature["consensus_home_prob"]),
        "MSX": float(feature["consensus_draw_prob"]),
        "MS2": float(feature["consensus_away_prob"]),
    }

    scores = {key: value for key, value in scores.items() if value > 0}

    if not scores:
        return None

    best_key = max(scores, key=scores.get)
    best_prob = scores[best_key]
    confidence = round(best_prob * 100, 2)

    execute(
        '''
        INSERT INTO model_predictions (
            fixture_id, model_version, market_key, selection_key,
            predicted_probability, confidence_score, created_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,NOW())
        ''',
        (
            fixture_id,
            model_version,
            "match_winner",
            best_key,
            best_prob,
            confidence,
        ),
    )

    return {
        "selection_key": best_key,
        "predicted_probability": best_prob,
        "confidence_score": confidence,
    }
