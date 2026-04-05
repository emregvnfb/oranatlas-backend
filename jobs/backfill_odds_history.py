from datetime import datetime, timedelta, timezone
import json

from db import fetch_one, execute
from services.the_odds_history_service import TheOddsHistoryService
from services.odds_service import implied_probability

service = TheOddsHistoryService()

SPORT_KEY_MAP = {
    "Premier League": "soccer_epl",
    "La Liga": "soccer_spain_la_liga",
    "Serie A": "soccer_italy_serie_a",
    "Bundesliga": "soccer_germany_bundesliga",
    "Ligue 1": "soccer_france_ligue_one",
    "Süper Lig": "soccer_turkey_super_league",
    "Super Lig": "soccer_turkey_super_league",
}

def ensure_bookmaker(bookmaker_name: str):
    row = fetch_one(
        '''
        SELECT id
        FROM bookmakers
        WHERE provider_name = %s AND normalized_bookmaker_key = %s
        ''',
        ("the_odds_api", bookmaker_name.lower().strip()),
    )
    if row:
        return row["id"]

    execute(
        '''
        INSERT INTO bookmakers (provider_name, bookmaker_name, normalized_bookmaker_key)
        VALUES (%s,%s,%s)
        ''',
        ("the_odds_api", bookmaker_name, bookmaker_name.lower().strip()),
    )
    row = fetch_one(
        '''
        SELECT id
        FROM bookmakers
        WHERE provider_name = %s AND normalized_bookmaker_key = %s
        ''',
        ("the_odds_api", bookmaker_name.lower().strip()),
    )
    return row["id"]

def ensure_market_and_selection(market_key: str, selection_key: str):
    market = fetch_one("SELECT id FROM markets WHERE market_key = %s", (market_key,))
    if not market:
        execute(
            '''
            INSERT INTO markets (market_key, market_name, market_group)
            VALUES (%s,%s,%s)
            ''',
            (market_key, market_key, "historical"),
        )
        market = fetch_one("SELECT id FROM markets WHERE market_key = %s", (market_key,))

    selection = fetch_one(
        "SELECT id FROM market_selections WHERE market_id = %s AND selection_key = %s",
        (market["id"], selection_key),
    )
    if not selection:
        execute(
            '''
            INSERT INTO market_selections (market_id, selection_key, selection_name)
            VALUES (%s,%s,%s)
            ''',
            (market["id"], selection_key, selection_key),
        )
        selection = fetch_one(
            "SELECT id FROM market_selections WHERE market_id = %s AND selection_key = %s",
            (market["id"], selection_key),
        )
    return market["id"], selection["id"]

def normalize_outcome_name(sport_key: str, market_key: str, outcome_name: str):
    text = (outcome_name or "").strip()
    if market_key == "h2h":
        return text
    if market_key == "totals":
        return text
    return text

def find_fixture_id_by_event(event_row):
    # safest version: try exact primary provider mapping if stored, else fuzzy match by teams+date
    provider_event_id = str(event_row.get("id") or "")
    mapped = fetch_one(
        '''
        SELECT fixture_id
        FROM fixture_provider_map
        WHERE provider_name = %s AND provider_fixture_id = %s
        ''',
        ("the_odds_api", provider_event_id),
    )
    if mapped:
        return mapped["fixture_id"]

    home = ((event_row.get("home_team") or "") if isinstance(event_row, dict) else "").strip()
    away = ((event_row.get("away_team") or "") if isinstance(event_row, dict) else "").strip()
    commence_time = event_row.get("commence_time")

    if home and away and commence_time:
        fuzzy = fetch_one(
            '''
            SELECT id
            FROM fixtures
            WHERE lower(home_team) = lower(%s)
              AND lower(away_team) = lower(%s)
              AND ABS(EXTRACT(EPOCH FROM (starting_at_utc - %s::timestamptz))) <= 21600
            ORDER BY ABS(EXTRACT(EPOCH FROM (starting_at_utc - %s::timestamptz))) ASC
            LIMIT 1
            ''',
            (home, away, commence_time, commence_time),
        )
        if fuzzy:
            execute(
                '''
                INSERT INTO fixture_provider_map (fixture_id, provider_name, provider_fixture_id)
                VALUES (%s,%s,%s)
                ON CONFLICT DO NOTHING
                ''',
                (fuzzy["id"], "the_odds_api", provider_event_id),
            )
            return fuzzy["id"]

    return None

def insert_snapshot(fixture_id, bookmaker_name, market_key, selection_key, odd_decimal, captured_at_utc, raw_payload):
    bookmaker_id = ensure_bookmaker(bookmaker_name)
    market_id, selection_id = ensure_market_and_selection(market_key, selection_key)

    execute(
        '''
        INSERT INTO odds_snapshots (
            fixture_id, provider_name, bookmaker_id, market_id, selection_id,
            odd_decimal, implied_probability, is_opening_snapshot, is_closing_snapshot,
            is_inplay, captured_at_utc, raw_payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        (
            fixture_id, "the_odds_api", bookmaker_id, market_id, selection_id,
            odd_decimal, implied_probability(odd_decimal), False, False,
            False, captured_at_utc, json.dumps(raw_payload, ensure_ascii=False)
        ),
    )

    execute(
        '''
        INSERT INTO odds_latest (
            fixture_id, bookmaker_id, market_id, selection_id, latest_odd_decimal, latest_captured_at_utc
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id, bookmaker_id, market_id, selection_id)
        DO UPDATE SET
            latest_odd_decimal = EXCLUDED.latest_odd_decimal,
            latest_captured_at_utc = EXCLUDED.latest_captured_at_utc
        ''',
        (fixture_id, bookmaker_id, market_id, selection_id, odd_decimal, captured_at_utc),
    )

def process_snapshot_payload(snapshot_payload):
    data = snapshot_payload.get("data") or snapshot_payload
    timestamp = data.get("timestamp") or datetime.now(timezone.utc).isoformat()

    # The Odds API payload structures can vary by endpoint/plan; this handler is intentionally defensive
    games = data.get("data") if isinstance(data.get("data"), list) else data.get("events") or data.get("games") or []
    if isinstance(games, dict):
        games = [games]

    inserted = 0
    for event in games:
        fixture_id = find_fixture_id_by_event(event)
        if not fixture_id:
            continue

        bookmakers = event.get("bookmakers") or []
        for bookmaker in bookmakers:
            bookmaker_name = bookmaker.get("title") or bookmaker.get("key") or "unknown"
            markets = bookmaker.get("markets") or []
            for market in markets:
                market_key = market.get("key") or "unknown_market"
                outcomes = market.get("outcomes") or []
                for outcome in outcomes:
                    outcome_name = normalize_outcome_name("", market_key, outcome.get("name") or "")
                    odd_decimal = outcome.get("price")
                    if odd_decimal is None:
                        continue
                    try:
                        odd_decimal = float(odd_decimal)
                    except Exception:
                        continue

                    insert_snapshot(
                        fixture_id=fixture_id,
                        bookmaker_name=bookmaker_name,
                        market_key=market_key,
                        selection_key=outcome_name,
                        odd_decimal=odd_decimal,
                        captured_at_utc=timestamp,
                        raw_payload={
                            "event_id": event.get("id"),
                            "bookmaker_key": bookmaker.get("key"),
                            "market_key": market_key,
                            "outcome_name": outcome_name,
                        },
                    )
                    inserted += 1
    return inserted

def backfill_league(league_name: str, start_iso: str, end_iso: str, step_minutes: int = 1440):
    sport_key = SPORT_KEY_MAP.get(league_name)
    if not sport_key:
        raise RuntimeError(f"Sport key eşleşmesi bulunamadı: {league_name}")

    current = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    total_inserted = 0

    while current <= end_dt:
        payload = service.get_historical_odds(
            sport_key=sport_key,
            date_iso=current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            regions="eu",
            markets="h2h,totals",
        )
        total_inserted += process_snapshot_payload(payload)
        current += timedelta(minutes=step_minutes)

    return total_inserted

def main():
    # Basit örnek kullanım:
    # 1 günlük adımla belirli tarih aralığı
    inserted = backfill_league(
        league_name="Premier League",
        start_iso="2025-08-01T00:00:00Z",
        end_iso="2025-08-10T00:00:00Z",
        step_minutes=1440,
    )
    print(f"backfill_odds_history tamamlandı. eklenen snapshot sayısı: {inserted}")

if __name__ == "__main__":
    main()
