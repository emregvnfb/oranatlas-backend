import json
import time
from datetime import datetime, timedelta, timezone

from db import fetch_all, fetch_one, execute
from services.api_football_service import APIFootballService
from services.odds_service import normalize_api_football_odds

service = APIFootballService()

# API bookmaker ve bet alanlarında tek integer bekliyor.
# Bu yüzden çoklu filtre yerine tek bookmaker + tek bet ile hızlı ve stabil çalışıyoruz.
TARGET_BOOKMAKER = "Bet365"
TARGET_PREMATCH_BETS = [
    "Match Winner",
    "Both Teams Score",
    "Over Under",
]

LOOKAHEAD_DAYS = 2
REQUEST_SLEEP_SECONDS = 0.20
MAX_PAGES_PER_REQUEST = 20

PRIORITY_LEAGUE_KEYWORDS = [
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
    "Eredivisie",
    "Primeira Liga",
    "Championship",
    "Scottish Premiership",
    "Jupiler Pro League",
    "Liga Profesional Argentina",
    "Liga MX",
    "MLS",
    "Brasileirão",
    "Brasileirao",
]

EXCLUDED_KEYWORDS = [
    "u17", "u18", "u19", "u20", "u21", "u23",
    "youth", "juvenil", "reserve", "reserves", "women",
    "feminino", "femenino", "friendly", "friendlies",
]


def normalize_text(value):
    return str(value or "").strip().lower()


def contains_any(text, keywords):
    text = normalize_text(text)
    return any(k.lower() in text for k in keywords)


def is_excluded_name(text):
    return contains_any(text, EXCLUDED_KEYWORDS)


def is_priority_league(league_name):
    return contains_any(league_name, PRIORITY_LEAGUE_KEYWORDS)


def ensure_bookmaker(bookmaker_name: str):
    bookmaker_key = str(bookmaker_name or "unknown").lower().strip()

    row = fetch_one(
        """
        SELECT id
        FROM bookmakers
        WHERE provider_name = %s AND normalized_bookmaker_key = %s
        """,
        ("api_football", bookmaker_key),
    )
    if row:
        return row["id"]

    execute(
        """
        INSERT INTO bookmakers (provider_name, bookmaker_name, normalized_bookmaker_key)
        VALUES (%s,%s,%s)
        """,
        ("api_football", str(bookmaker_name or "unknown"), bookmaker_key),
    )
    row = fetch_one(
        """
        SELECT id
        FROM bookmakers
        WHERE provider_name = %s AND normalized_bookmaker_key = %s
        """,
        ("api_football", bookmaker_key),
    )
    return row["id"]


def ensure_market_and_selection(market_key: str, selection_key):
    market_key = str(market_key or "unknown_market").strip()
    selection_key = str(selection_key or "unknown_selection").strip()

    market = fetch_one("SELECT id FROM markets WHERE market_key = %s", (market_key,))
    if not market:
        execute(
            """
            INSERT INTO markets (market_key, market_name, market_group)
            VALUES (%s,%s,%s)
            """,
            (market_key, market_key, "general"),
        )
        market = fetch_one("SELECT id FROM markets WHERE market_key = %s", (market_key,))

    selection = fetch_one(
        "SELECT id FROM market_selections WHERE market_id = %s AND selection_key = %s",
        (market["id"], selection_key),
    )
    if not selection:
        execute(
            """
            INSERT INTO market_selections (market_id, selection_key, selection_name)
            VALUES (%s,%s,%s)
            """,
            (market["id"], selection_key, selection_key),
        )
        selection = fetch_one(
            "SELECT id FROM market_selections WHERE market_id = %s AND selection_key = %s",
            (market["id"], selection_key),
        )
    return market["id"], selection["id"]


def snapshot_exists(fixture_id, bookmaker_id, market_id, selection_id, odd_decimal, captured_at_utc):
    row = fetch_one(
        """
        SELECT id
        FROM odds_snapshots
        WHERE fixture_id = %s
          AND bookmaker_id = %s
          AND market_id = %s
          AND selection_id = %s
          AND odd_decimal = %s
          AND captured_at_utc = %s
        LIMIT 1
        """,
        (fixture_id, bookmaker_id, market_id, selection_id, odd_decimal, captured_at_utc),
    )
    return bool(row)


def insert_snapshot(fixture_id, row):
    bookmaker_id = ensure_bookmaker(row["bookmaker_name"])
    market_id, selection_id = ensure_market_and_selection(row["market_key"], row["selection_key"])

    if snapshot_exists(
        fixture_id,
        bookmaker_id,
        market_id,
        selection_id,
        row["odd_decimal"],
        row["captured_at_utc"],
    ):
        return 0

    execute(
        """
        INSERT INTO odds_snapshots (
            fixture_id, provider_name, bookmaker_id, market_id, selection_id,
            odd_decimal, implied_probability, is_opening_snapshot, is_closing_snapshot,
            is_inplay, captured_at_utc, raw_payload
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            fixture_id,
            row["provider_name"],
            bookmaker_id,
            market_id,
            selection_id,
            row["odd_decimal"],
            row["implied_probability"],
            False,
            False,
            row["is_inplay"],
            row["captured_at_utc"],
            json.dumps(row["raw_payload"], ensure_ascii=False),
        ),
    )

    execute(
        """
        INSERT INTO odds_latest (
            fixture_id, bookmaker_id, market_id, selection_id, latest_odd_decimal, latest_captured_at_utc
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fixture_id, bookmaker_id, market_id, selection_id)
        DO UPDATE SET
            latest_odd_decimal = EXCLUDED.latest_odd_decimal,
            latest_captured_at_utc = EXCLUDED.latest_captured_at_utc
        """,
        (
            fixture_id,
            bookmaker_id,
            market_id,
            selection_id,
            row["odd_decimal"],
            row["captured_at_utc"],
        ),
    )
    return 1


def get_fixture_lookup():
    rows = fetch_all(
        """
        SELECT id, provider_fixture_id_primary, league_name, country_name, home_team, away_team, starting_at_utc
        FROM fixtures
        WHERE status NOT IN ('FT', 'AET', 'PEN')
          AND starting_at_utc >= (NOW() AT TIME ZONE 'UTC')
          AND starting_at_utc <= ((NOW() AT TIME ZONE 'UTC') + (%s || ' days')::interval)
        ORDER BY starting_at_utc ASC
        """,
        (LOOKAHEAD_DAYS,),
    )
    lookup = {}
    for row in rows:
        provider_id = str(row["provider_fixture_id_primary"] or "").strip()
        if provider_id:
            lookup[provider_id] = row
    return lookup


def resolve_reference_ids():
    bookmaker_id = None
    try:
        row = service.search_bookmaker(TARGET_BOOKMAKER)
        if row and row.get("id") is not None:
            bookmaker_id = str(row["id"])
    except Exception as e:
        print(f"⚠️ Bookmaker aranamadı: {TARGET_BOOKMAKER} | {e}")

    bet_map = {}
    for name in TARGET_PREMATCH_BETS:
        try:
            row = service.search_prematch_bet(name)
            if row and row.get("id") is not None:
                bet_map[name] = str(row["id"])
        except Exception as e:
            print(f"⚠️ Bet aranamadı: {name} | {e}")

    return bookmaker_id, bet_map


def priority_score(local_fixture):
    league = str(local_fixture.get("league_name") or "")
    country = str(local_fixture.get("country_name") or "")
    score = 0

    if is_priority_league(league):
        score += 100

    if country in {
        "England", "Spain", "Italy", "Germany", "France", "Turkey",
        "Netherlands", "Portugal", "Belgium", "Scotland",
        "Argentina", "Brazil", "Mexico", "USA"
    }:
        score += 25

    dt = local_fixture.get("starting_at_utc")
    if dt:
        now = datetime.now(timezone.utc)
        hours = max((dt - now).total_seconds() / 3600, 0)
        score += max(24 - hours, 0)

    return score


def fetch_bulk_odds_for_date_and_bet(date_str, fixture_lookup, bookmaker_id, bet_name, bet_id):
    page = 1
    inserted = 0
    matched_fixtures = set()
    no_match_rows = 0

    while page <= MAX_PAGES_PER_REQUEST:
        response_items = service.get_odds_by_date(
            date_str=date_str,
            bookmaker_id=bookmaker_id,
            bet_id=bet_id,
            page=page,
        )

        if not response_items:
            break

        page_inserted = 0
        page_matched = 0

        prioritized_items = []
        for item in response_items:
            fixture_info = item.get("fixture") or {}
            fixture_id = str(fixture_info.get("id") or "").strip()
            local_fixture = fixture_lookup.get(fixture_id)
            if not local_fixture:
                no_match_rows += 1
                continue

            league_text = str((item.get("league") or {}).get("name") or local_fixture.get("league_name") or "")
            combined = f"{league_text} {local_fixture.get('home_team', '')} {local_fixture.get('away_team', '')}"
            if is_excluded_name(combined):
                continue

            prioritized_items.append((priority_score(local_fixture), local_fixture, item))

        prioritized_items.sort(key=lambda x: x[0], reverse=True)

        for _, local_fixture, item in prioritized_items:
            normalized = normalize_api_football_odds([item])
            if not normalized:
                continue

            inserted_for_fixture = 0
            for row in normalized:
                inserted_for_fixture += insert_snapshot(local_fixture["id"], row)

            if inserted_for_fixture > 0:
                matched_fixtures.add(local_fixture["id"])
                page_matched += 1
                page_inserted += inserted_for_fixture

        inserted += page_inserted

        print(
            f"  ↳ {date_str} | {bet_name} | page={page} | cevap={len(response_items)} | "
            f"eşleşen maç={page_matched} | eklenen snapshot={page_inserted}"
        )

        if len(response_items) < 10:
            break

        page += 1
        time.sleep(REQUEST_SLEEP_SECONDS)

    return inserted, matched_fixtures, no_match_rows


def main():
    fixture_lookup = get_fixture_lookup()
    if not fixture_lookup:
        print("collect_odds: uygun aktif fixture bulunamadı.")
        return

    bookmaker_id, bet_map = resolve_reference_ids()

    print("🚀 Bulk odds toplama başlıyor")
    print(f"Uygun fixture sayısı: {len(fixture_lookup)}")
    print(f"Bookmaker filtresi: {bookmaker_id if bookmaker_id else 'yok'}")
    print(f"Bet filtreleri: {bet_map if bet_map else 'yok'}")

    if not bookmaker_id:
        print("❌ Geçerli bookmaker bulunamadı.")
        return

    if not bet_map:
        print("❌ Geçerli bet filtresi bulunamadı.")
        return

    total_inserted = 0
    all_matched = set()
    total_no_match_rows = 0

    now_local = datetime.now()
    dates = [(now_local + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(LOOKAHEAD_DAYS)]

    for date_str in dates:
        print(f"\n📅 Tarih: {date_str}")
        for bet_name, bet_id in bet_map.items():
            inserted, matched_fixtures, no_match_rows = fetch_bulk_odds_for_date_and_bet(
                date_str=date_str,
                fixture_lookup=fixture_lookup,
                bookmaker_id=bookmaker_id,
                bet_name=bet_name,
                bet_id=bet_id,
            )
            total_inserted += inserted
            all_matched.update(matched_fixtures)
            total_no_match_rows += no_match_rows

    print("\n✅ collect_odds tamamlandı")
    print(f"Toplam yeni snapshot: {total_inserted}")
    print(f"Odds bulunan fixture sayısı: {len(all_matched)}")
    print(f"Yerel fixture ile eşleşmeyen satır sayısı: {total_no_match_rows}")


if __name__ == "__main__":
    main()
