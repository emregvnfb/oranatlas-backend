
import json
from datetime import datetime, timezone
from db import fetch_all, execute, fetch_one
from services.prediction_service_v2 import PredictionServiceV2

BIG_LEAGUE_KEYWORDS = [
    "Premier League", "La Liga", "Serie A", "Bundesliga", "Ligue 1",
    "Süper Lig", "Super Lig", "Champions League", "Europa League",
    "Conference League", "Liga Profesional Argentina", "Liga MX", "MLS",
]

prediction_service_v2 = PredictionServiceV2(db_path="matches.db")


def is_big_league(league_name: str) -> bool:
    league_name = str(league_name or "")
    return any(keyword.lower() in league_name.lower() for keyword in BIG_LEAGUE_KEYWORDS)


def get_normalized_odds_for_fixture(fixture_id: int):
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

        if market_key == "match winner" or ("match" in market_key and "winner" in market_key) or market_key in {"1x2", "fulltime result", "full time result"}:
            if selection_key in {"home", "1"}:
                set_if_empty("home_win", odd)
            elif selection_key in {"draw", "x"}:
                set_if_empty("draw", odd)
            elif selection_key in {"away", "2"}:
                set_if_empty("away_win", odd)

        if "double chance" in market_key or market_key in {"double_chance", "dc"}:
            if selection_key in {"1x", "home/draw", "1-x"}:
                set_if_empty("double_chance_1x", odd)
            elif selection_key in {"x2", "draw/away", "x-2"}:
                set_if_empty("double_chance_x2", odd)
            elif selection_key in {"12", "home/away", "1-2"}:
                set_if_empty("double_chance_12", odd)

        if "both teams score" in market_key or "both teams to score" in market_key or "btts" in market_key:
            if selection_key in {"yes", "gg", "btts yes"}:
                set_if_empty("btts_yes", odd)
            elif selection_key in {"no", "ng", "btts no"}:
                set_if_empty("btts_no", odd)

        if "over" in selection_key and "1.5" in selection_key:
            set_if_empty("over_1_5", odd)
        if "under" in selection_key and "3.5" in selection_key:
            set_if_empty("under_3_5", odd)

    return odds_data


def score_pick(item: dict) -> float:
    confidence = float(item.get("confidence") or 0)
    odd = float(item.get("bet_odd") or 0)
    ev = float(item.get("ev") or 0)
    market_key = str(item.get("market_key") or "")
    score = float(item.get("score") or 0)

    if 1.18 <= odd <= 1.65:
        score += 14
    elif 1.65 < odd <= 2.10:
        score += 8
    elif 2.10 < odd <= 3.00:
        score += 3
    else:
        score -= 8

    if confidence >= 44:
        score += 6
    elif confidence >= 35:
        score += 3
    else:
        score -= 8

    if ev >= 0.18:
        score += 10
    elif ev >= 0.12:
        score += 7
    elif ev >= 0.08:
        score += 4
    elif ev >= 0.06:
        score += 2
    else:
        score -= 8

    if market_key in {"OVER_1_5", "DOUBLE_CHANCE_1X", "DOUBLE_CHANCE_X2", "UNDER_3_5"}:
        score += 8
    elif market_key == "BTTS_YES":
        score += 1
    else:
        score -= 3

    if is_big_league(item.get("league")):
        score += 4

    return round(score, 2)


def _fetch_pool_source_rows():
    return fetch_all(
        '''
        SELECT DISTINCT
            f.id, f.league_name, f.country_name, f.home_team, f.away_team,
            f.starting_at_utc, f.status, ff.*
        FROM fixtures f
        JOIN fixture_features ff ON ff.fixture_id = f.id
        WHERE f.status NOT IN ('FT', 'AET', 'PEN')
          AND f.starting_at_utc >= (NOW() AT TIME ZONE 'UTC')
          AND EXISTS (SELECT 1 FROM odds_latest ol WHERE ol.fixture_id = f.id)
        ORDER BY f.starting_at_utc ASC
        LIMIT 700
        '''
    ) or []


def _build_feature_row(fixture_row):
    row = dict(fixture_row)
    return {
        "match_id": row.get("id"),
        "league_name": row.get("league_name") or "",
        "home_team": row.get("home_team") or "",
        "away_team": row.get("away_team") or "",
        "match_date": row.get("starting_at_utc").isoformat() if row.get("starting_at_utc") else "",
        "home_form": float(row.get("home_form") or row.get("home_form_score") or 0.50),
        "away_form": float(row.get("away_form") or row.get("away_form_score") or 0.50),
        "home_attack": float(row.get("home_attack") or row.get("home_attack_score") or 1.20),
        "away_attack": float(row.get("away_attack") or row.get("away_attack_score") or 1.10),
        "home_defense": float(row.get("home_defense") or row.get("home_defense_score") or 1.00),
        "away_defense": float(row.get("away_defense") or row.get("away_defense_score") or 1.00),
        "home_points_per_match": float(row.get("home_points_per_match") or row.get("home_ppm") or 1.40),
        "away_points_per_match": float(row.get("away_points_per_match") or row.get("away_ppm") or 1.20),
        "home_goals_for": float(row.get("home_goals_for") or row.get("home_avg_goals_for") or 1.40),
        "away_goals_for": float(row.get("away_goals_for") or row.get("away_avg_goals_for") or 1.20),
        "home_goals_against": float(row.get("home_goals_against") or row.get("home_avg_goals_against") or 1.10),
        "away_goals_against": float(row.get("away_goals_against") or row.get("away_avg_goals_against") or 1.10),
        "home_win_rate": float(row.get("home_win_rate") or 0.45),
        "away_win_rate": float(row.get("away_win_rate") or 0.30),
        "draw_rate": float(row.get("draw_rate") or 0.26),
    }


def _risk_bucket(item: dict) -> str:
    odd = float(item.get("bet_odd") or 0)
    ev = float(item.get("ev") or 0)
    confidence = float(item.get("confidence") or 0)
    market_key = str(item.get("market_key") or "")

    if odd <= 1.75 and ev >= 0.08 and confidence >= 38 and market_key in {"OVER_1_5", "DOUBLE_CHANCE_1X", "DOUBLE_CHANCE_X2", "UNDER_3_5"}:
        return "safe"
    if odd <= 2.20 and ev >= 0.08 and confidence >= 35 and market_key in {"OVER_1_5", "DOUBLE_CHANCE_1X", "DOUBLE_CHANCE_X2", "UNDER_3_5", "BTTS_YES"}:
        return "balanced"
    if odd <= 3.00 and ev >= 0.10 and confidence >= 35:
        return "aggressive"
    return "wild"


def _is_pool_eligible(item: dict) -> bool:
    if item.get("engine") != "v2":
        return False

    market_key = str(item.get("market_key") or "")
    odd = float(item.get("bet_odd") or 0)
    ev = float(item.get("ev") or 0)
    confidence = float(item.get("confidence") or 0)

    if ev < 0.06:
        return False
    if confidence < 35:
        return False
    if odd > 3.00:
        return False
    if market_key == "DRAW":
        return False
    if market_key == "BTTS_YES" and ev < 0.12:
        return False
    return True


def build_pool():
    rows = _fetch_pool_source_rows()
    pool = []
    used_fixture_ids = set()

    for row in rows:
        fixture_id = int(row["id"])
        if fixture_id in used_fixture_ids:
            continue

        odds_data = get_normalized_odds_for_fixture(fixture_id)
        feature_row = _build_feature_row(row)

        if not odds_data:
            continue

        v2_result = prediction_service_v2.analyze_match(feature_row, odds_data)
        best = v2_result.best_market
        if not best:
            continue

        item = {
            "fixture_id": fixture_id,
            "home": row["home_team"],
            "away": row["away_team"],
            "league": row["league_name"],
            "country": row["country_name"],
            "date": row["starting_at_utc"].isoformat() if row["starting_at_utc"] else "",
            "market_key": best.market_key,
            "market_name": best.market_name,
            "bet_label": best.selection,
            "selection_key": best.market_key,
            "confidence": round(float(best.confidence) * 100.0, 2),
            "bet_odd": round(float(best.odds), 2),
            "probability": round(float(best.probability), 4),
            "implied_probability": round(float(best.implied_probability), 4),
            "ev": round(float(best.ev), 4),
            "reason": best.reason,
            "score": round(float(best.score), 2),
            "is_big_league": is_big_league(row["league_name"]),
            "engine": "v2",
        }
        item["score"] = score_pick(item)
        item["risk_bucket"] = _risk_bucket(item)

        if not _is_pool_eligible(item):
            continue

        pool.append(item)
        used_fixture_ids.add(fixture_id)

    pool.sort(key=lambda x: (
        1 if x.get("is_big_league") else 0,
        float(x.get("score") or 0),
        float(x.get("ev") or 0),
        float(x.get("confidence") or 0),
    ), reverse=True)
    return pool


def split_pool(pool):
    safe_candidates = [x for x in pool if x.get("risk_bucket") == "safe"]
    balanced_candidates = [x for x in pool if x.get("risk_bucket") in {"safe", "balanced"}]
    aggressive_candidates = [x for x in pool if x.get("risk_bucket") in {"balanced", "aggressive"}]
    return safe_candidates, balanced_candidates, aggressive_candidates


def unique_extend(target, source, limit):
    used = {x["fixture_id"] for x in target}
    for item in source:
        if item["fixture_id"] in used:
            continue
        target.append(item)
        used.add(item["fixture_id"])
        if len(target) >= limit:
            break
    return target


def fill_to_target(base_items, pool, target_size, max_aggressive=1):
    selected = []
    used = set()
    aggressive_count = 0

    for item in base_items:
        if len(selected) >= target_size:
            break
        fixture_id = item["fixture_id"]
        if fixture_id in used:
            continue
        if item.get("risk_bucket") == "aggressive":
            if aggressive_count >= max_aggressive:
                continue
            aggressive_count += 1
        selected.append(item)
        used.add(fixture_id)

    for item in pool:
        if len(selected) >= target_size:
            break
        fixture_id = item["fixture_id"]
        if fixture_id in used:
            continue
        if item.get("risk_bucket") == "aggressive":
            if aggressive_count >= max_aggressive:
                continue
            aggressive_count += 1
        selected.append(item)
        used.add(fixture_id)

    return selected


def build_coupon(selected_items, risk_label):
    if not selected_items:
        return None

    total_odd = 1.0
    for item in selected_items:
        total_odd *= float(item["bet_odd"])

    return {
        "coupon_size": len(selected_items),
        "total_odd": round(total_odd, 2),
        "avg_confidence": round(sum(float(x["confidence"]) for x in selected_items) / len(selected_items), 2),
        "avg_ev": round(sum(float(x.get("ev") or 0) for x in selected_items) / len(selected_items), 4),
        "items": selected_items,
        "risk": risk_label,
        "is_high_odd": False,
    }


def take_top_unique(pool, n):
    result = []
    used = set()
    for item in pool:
        if item["fixture_id"] in used:
            continue
        result.append(item)
        used.add(item["fixture_id"])
        if len(result) >= n:
            break
    return result


def generate_daily_coupon_package(coupon_date: str):
    pool = build_pool()
    safe_pool, balanced_pool, aggressive_pool = split_pool(pool)

    pool_public = []
    for item in pool:
        item_copy = dict(item)
        item_copy.pop("score", None)
        pool_public.append(item_copy)

    coupons_3 = []
    coupons_4 = []
    coupons_5 = []
    coupons_6 = []
    high_odd_coupons = []

    # 3'lü güvenli kupon
    c3_seed = take_top_unique(safe_pool, 3)
    c3 = fill_to_target(c3_seed, balanced_pool + pool, 3, max_aggressive=0)
    coupon3 = build_coupon(c3, "Güvenli")
    if coupon3:
        coupons_3.append(coupon3)

    # 4'lü dengeli kupon
    c4_seed = []
    c4_seed = unique_extend(c4_seed, safe_pool, 2)
    c4_seed = unique_extend(c4_seed, balanced_pool, 4)
    c4 = fill_to_target(c4_seed, balanced_pool + aggressive_pool + pool, 4, max_aggressive=1)
    coupon4 = build_coupon(c4, "Dengeli")
    if coupon4:
        coupons_4.append(coupon4)

    # Kullanıcının isteği: 5'li ve 6'lı kupon üretme
    package = {
        "pool": pool_public,
        "high_odd_coupons": high_odd_coupons,
        "coupons_3": coupons_3,
        "coupons_4": coupons_4,
        "coupons_5": coupons_5,
        "coupons_6": coupons_6,
    }

    execute(
        '''
        INSERT INTO coupon_packages (coupon_date, coupon_type, package_json, created_at)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (coupon_date, coupon_type)
        DO UPDATE SET package_json = EXCLUDED.package_json, created_at = EXCLUDED.created_at
        ''',
        (coupon_date, "daily", json.dumps(package, ensure_ascii=False), datetime.now(timezone.utc)),
    )
    return package


def get_today_coupon_package():
    row = fetch_one(
        '''
        SELECT package_json
        FROM coupon_packages
        WHERE coupon_date = CURRENT_DATE
          AND coupon_type = 'daily'
        '''
    )
    return row["package_json"] if row else None
