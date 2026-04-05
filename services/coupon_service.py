import json
from datetime import datetime, timezone
from db import fetch_all, execute, fetch_one
from services.prediction_service import build_simple_prediction

BET_LABEL_TO_SELECTION = {
    "MS1": ["Home", "1"],
    "MSX": ["Draw", "X"],
    "MS2": ["Away", "2"],
}

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
SAFE_MAX_ODD = 1.50

def is_big_league(league_name: str) -> bool:
    league_name = str(league_name or "")
    return any(keyword.lower() in league_name.lower() for keyword in BIG_LEAGUE_KEYWORDS)

def get_best_odd_for_prediction(fixture_id: int, bet_label: str):
    selection_keys = BET_LABEL_TO_SELECTION.get(bet_label)
    if not selection_keys:
        return None

    row = fetch_one(
        '''
        SELECT MAX(ol.latest_odd_decimal) AS best_odd
        FROM odds_latest ol
        JOIN markets m ON ol.market_id = m.id
        JOIN market_selections ms ON ol.selection_id = ms.id
        WHERE ol.fixture_id = %s
          AND (
                lower(m.market_key) = 'match winner'
                OR (lower(m.market_key) LIKE '%match%' AND lower(m.market_key) LIKE '%winner%')
              )
          AND lower(ms.selection_key) = ANY(%s)
        ''',
        (fixture_id, [x.lower() for x in selection_keys]),
    )
    if not row or row["best_odd"] is None:
        return None
    return float(row["best_odd"])

def score_pick(item: dict) -> float:
    confidence = float(item.get("confidence") or 0)
    odd = float(item.get("bet_odd") or 0)
    league_name = str(item.get("league") or "")

    score = confidence

    if is_big_league(league_name):
        score += 8

    if SAFE_MIN_ODD <= odd <= SAFE_MAX_ODD:
        score += 12
    elif 1.50 < odd <= 1.90:
        score += 5
    elif odd > 2.50:
        score -= 6

    if confidence < 38:
        score -= 8
    elif confidence >= 55:
        score += 6

    return round(score, 2)

def build_pool():
    fixtures = fetch_all(
        '''
        SELECT DISTINCT
            f.id,
            f.league_name,
            f.country_name,
            f.home_team,
            f.away_team,
            f.starting_at_utc,
            f.status
        FROM fixtures f
        JOIN fixture_features ff ON ff.fixture_id = f.id
        JOIN odds_latest ol ON ol.fixture_id = f.id
        WHERE f.status NOT IN ('FT', 'AET', 'PEN')
          AND f.starting_at_utc >= (NOW() AT TIME ZONE 'UTC')
        ORDER BY f.starting_at_utc ASC
        LIMIT 500
        '''
    )

    pool = []
    used_fixture_ids = set()

    for fixture in fixtures:
        if fixture["id"] in used_fixture_ids:
            continue

        prediction = build_simple_prediction(fixture["id"])
        if not prediction:
            continue

        best_odd = get_best_odd_for_prediction(fixture["id"], prediction["selection_key"])
        if best_odd is None:
            continue

        item = {
            "fixture_id": fixture["id"],
            "home": fixture["home_team"],
            "away": fixture["away_team"],
            "league": fixture["league_name"],
            "country": fixture["country_name"],
            "date": fixture["starting_at_utc"].isoformat() if fixture["starting_at_utc"] else "",
            "bet_label": prediction["selection_key"],
            "confidence": round(float(prediction["confidence_score"]), 2),
            "bet_odd": round(float(best_odd), 2),
            "is_big_league": is_big_league(fixture["league_name"]),
        }
        item["score"] = score_pick(item)

        pool.append(item)
        used_fixture_ids.add(fixture["id"])

    pool.sort(key=lambda x: (x["score"], x["confidence"]), reverse=True)
    return pool

def split_pool(pool):
    safe_candidates = [
        x for x in pool
        if SAFE_MIN_ODD <= float(x.get("bet_odd") or 0) <= SAFE_MAX_ODD and float(x.get("confidence") or 0) >= 40
    ]
    medium_candidates = [
        x for x in pool
        if 1.50 < float(x.get("bet_odd") or 0) <= 2.20 and float(x.get("confidence") or 0) >= 40
    ]
    high_candidates = [
        x for x in pool
        if float(x.get("bet_odd") or 0) > 2.20 and float(x.get("confidence") or 0) >= 36
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
        if not safe_items:
            return None
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
    coupon3 = build_coupon(base_3, "Düşük-Orta", require_safe=True, min_total_odd=2.5)
    if coupon3:
        coupons_3.append(coupon3)

    base_4 = []
    base_4 = unique_extend(base_4, safe_pool, 1)
    base_4 = unique_extend(base_4, medium_pool, 4)
    base_4 = unique_extend(base_4, pool, 4)
    base_4 = take_top_unique(base_4, 4)
    coupon4 = build_coupon(base_4, "Orta", require_safe=True, min_total_odd=3.5)
    if coupon4:
        coupons_4.append(coupon4)

    base_5 = []
    base_5 = unique_extend(base_5, safe_pool, 1)
    base_5 = unique_extend(base_5, medium_pool, 3)
    base_5 = unique_extend(base_5, high_pool, 5)
    base_5 = unique_extend(base_5, pool, 5)
    base_5 = take_top_unique(base_5, 5)
    coupon5 = build_coupon(base_5, "Orta-Yüksek", require_safe=True, min_total_odd=5.0)
    if coupon5:
        coupons_5.append(coupon5)

    base_6 = []
    base_6 = unique_extend(base_6, safe_pool, 1)
    base_6 = unique_extend(base_6, medium_pool, 3)
    base_6 = unique_extend(base_6, high_pool, 6)
    base_6 = unique_extend(base_6, pool, 6)
    base_6 = take_top_unique(base_6, 6)
    coupon6 = build_coupon(base_6, "Yüksek", require_safe=True, min_total_odd=8.0)
    if coupon6:
        coupons_6.append(coupon6)

    high_special = []
    high_special = unique_extend(high_special, safe_pool, 1)
    high_special = unique_extend(high_special, high_pool, 4)
    high_special = unique_extend(high_special, pool, 4)
    high_special = take_top_unique(high_special, 4)
    high_coupon = build_coupon(high_special, "Yüksek Oran", require_safe=True, min_total_odd=8.0)
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
