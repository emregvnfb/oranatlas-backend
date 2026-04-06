import json
from datetime import datetime, timezone
from db import fetch_all, execute, fetch_one
from services.prediction_service import build_simple_prediction
from services.prediction_service_v2 import PredictionServiceV2

BIG_LEAGUE_KEYWORDS = [
    "Premier League",
    "La Liga",
    "Serie A",
    "Bundesliga",
    "Ligue 1",
    "Süper Lig",
    "Super Lig",
    "Champions League",
    "Europa League",
    "Conference League",
    "Liga Profesional Argentina",
    "Liga MX",
    "MLS",
]

SAFE_MIN_ODD = 1.20
SAFE_MAX_ODD = 1.65

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

        if "double chance" in market_key or market_key in {"double_chance", "dc"}:
            if selection_key in {"1x", "home/draw", "1-x"}:
                set_if_empty("double_chance_1x", odd)
            elif selection_key in {"x2", "draw/away", "x-2"}:
                set_if_empty("double_chance_x2", odd)
            elif selection_key in {"12", "home/away", "1-2"}:
                set_if_empty("double_chance_12", odd)

        if (
            "both teams score" in market_key
            or "both teams to score" in market_key
            or "btts" in market_key
        ):
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
    league_name = str(item.get("league") or "")
    ev = float(item.get("ev") or 0)
    base_score = float(item.get("score") or 0)

    score = base_score if base_score > 0 else confidence

    if is_big_league(league_name):
        score += 8

    if SAFE_MIN_ODD <= odd <= SAFE_MAX_ODD:
        score += 12
    elif 1.65 < odd <= 2.35:
        score += 5
    elif odd > 4.20:
        score -= 8
    elif odd > 3.20:
        score -= 4

    if confidence < 38:
        score -= 8
    elif confidence >= 55:
        score += 6

    if ev >= 0.08:
        score += 8
    elif ev >= 0.00:
        score += 4
    elif ev >= -0.03:
        score += 1

    return round(score, 2)


def _fetch_pool_source_rows():
    return fetch_all(
        '''
        SELECT DISTINCT
            f.id,
            f.league_name,
            f.country_name,
            f.home_team,
            f.away_team,
            f.starting_at_utc,
            f.status,
            ff.*
        FROM fixtures f
        JOIN fixture_features ff ON ff.fixture_id = f.id
        WHERE f.status NOT IN ('FT', 'AET', 'PEN')
          AND f.starting_at_utc >= (NOW() AT TIME ZONE 'UTC')
          AND EXISTS (
              SELECT 1
              FROM odds_latest ol
              WHERE ol.fixture_id = f.id
          )
        ORDER BY f.starting_at_utc ASC
        LIMIT 500
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

        if odds_data:
            v2_result = prediction_service_v2.analyze_match(feature_row, odds_data)
            best = v2_result.best_market
            if best:
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
                pool.append(item)
                used_fixture_ids.add(fixture_id)
                continue

        prediction = build_simple_prediction(fixture_id)
        if not prediction:
            continue

        item = {
            "fixture_id": fixture_id,
            "home": row["home_team"],
            "away": row["away_team"],
            "league": row["league_name"],
            "country": row["country_name"],
            "date": row["starting_at_utc"].isoformat() if row["starting_at_utc"] else "",
            "market_key": prediction.get("market_key", "match_winner"),
            "market_name": prediction.get("market_name", "Maç Sonucu"),
            "bet_label": prediction.get("selection_label", prediction.get("bet_label", "")),
            "selection_key": prediction.get("selection_key", ""),
            "confidence": round(float(prediction.get("confidence_score") or 0), 2),
            "bet_odd": round(float(prediction.get("bet_odd") or prediction.get("odd") or 0), 2),
            "probability": None,
            "implied_probability": None,
            "ev": 0.0,
            "reason": prediction.get("reason", "legacy fallback"),
            "score": 0.0,
            "is_big_league": is_big_league(row["league_name"]),
            "engine": "legacy",
        }
        item["score"] = score_pick(item)
        pool.append(item)
        used_fixture_ids.add(fixture_id)

    pool.sort(key=lambda x: (x["score"], x["confidence"]), reverse=True)
    return pool


def split_pool(pool):
    safe_candidates = [
        x for x in pool
        if SAFE_MIN_ODD <= float(x.get("bet_odd") or 0) <= SAFE_MAX_ODD and float(x.get("confidence") or 0) >= 38
    ]
    medium_candidates = [
        x for x in pool
        if 1.65 < float(x.get("bet_odd") or 0) <= 2.35 and float(x.get("confidence") or 0) >= 38
    ]
    high_candidates = [
        x for x in pool
        if 2.35 < float(x.get("bet_odd") or 0) <= 4.20 and float(x.get("confidence") or 0) >= 36
    ]
    return safe_candidates, medium_candidates, high_candidates


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


def build_coupon(items, risk_label, require_safe=False, min_total_odd=None):
    if not items:
        return None

    selected = []
    if require_safe:
        safe_items = [x for x in items if SAFE_MIN_ODD <= float(x.get("bet_odd") or 0) <= SAFE_MAX_ODD]
        if safe_items:
            selected.append(safe_items[0])

    selected = unique_extend(selected, items, len(items))

    total_odd = 1.0
    usable_items = []
    used_fixture_ids = set()

    for item in selected:
        if item["fixture_id"] in used_fixture_ids:
            continue
        used_fixture_ids.add(item["fixture_id"])
        usable_items.append(item)
        total_odd *= float(item["bet_odd"])

    if not usable_items:
        return None

    total_odd = round(total_odd, 2)

    if min_total_odd is not None and total_odd < min_total_odd:
        return None

    return {
        "coupon_size": len(usable_items),
        "total_odd": total_odd,
        "avg_confidence": round(sum(float(x["confidence"]) for x in usable_items) / len(usable_items), 2),
        "items": usable_items,
        "risk": risk_label,
        "is_high_odd": risk_label.lower().startswith("yüksek"),
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
    safe_pool, medium_pool, high_pool = split_pool(pool)

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

    base_3 = []
    base_3 = unique_extend(base_3, safe_pool, 1)
    base_3 = unique_extend(base_3, medium_pool, 3)
    base_3 = unique_extend(base_3, pool, 3)
    base_3 = take_top_unique(base_3, 3)
    coupon3 = build_coupon(base_3, "Düşük-Orta", require_safe=True, min_total_odd=2.2)
    if coupon3:
        coupons_3.append(coupon3)

    base_4 = []
    base_4 = unique_extend(base_4, safe_pool, 1)
    base_4 = unique_extend(base_4, medium_pool, 4)
    base_4 = unique_extend(base_4, pool, 4)
    base_4 = take_top_unique(base_4, 4)
    coupon4 = build_coupon(base_4, "Orta", require_safe=True, min_total_odd=3.0)
    if coupon4:
        coupons_4.append(coupon4)

    base_5 = []
    base_5 = unique_extend(base_5, safe_pool, 1)
    base_5 = unique_extend(base_5, medium_pool, 3)
    base_5 = unique_extend(base_5, high_pool, 5)
    base_5 = unique_extend(base_5, pool, 5)
    base_5 = take_top_unique(base_5, 5)
    coupon5 = build_coupon(base_5, "Orta-Yüksek", require_safe=True, min_total_odd=4.5)
    if coupon5:
        coupons_5.append(coupon5)

    base_6 = []
    base_6 = unique_extend(base_6, safe_pool, 1)
    base_6 = unique_extend(base_6, medium_pool, 3)
    base_6 = unique_extend(base_6, high_pool, 6)
    base_6 = unique_extend(base_6, pool, 6)
    base_6 = take_top_unique(base_6, 6)
    coupon6 = build_coupon(base_6, "Yüksek", require_safe=True, min_total_odd=6.5)
    if coupon6:
        coupons_6.append(coupon6)

    high_special = []
    high_special = unique_extend(high_special, medium_pool, 2)
    high_special = unique_extend(high_special, high_pool, 4)
    high_special = unique_extend(high_special, pool, 4)
    high_special = take_top_unique(high_special, 4)
    high_coupon = build_coupon(high_special, "Yüksek Oran", require_safe=False, min_total_odd=7.0)
    if high_coupon:
        high_odd_coupons.append(high_coupon)

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
