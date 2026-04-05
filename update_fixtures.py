from services.api_football_service import APIFootballService
from db import get_connection

service = APIFootballService()

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def prepare_fixture_row(item):
    fixture = item.get("fixture", {})
    league = item.get("league", {})
    teams = item.get("teams", {})
    goals = item.get("goals", {})

    provider_fixture_id = str(fixture.get("id"))
    home_name = teams.get("home", {}).get("name") or "-"
    away_name = teams.get("away", {}).get("name") or "-"
    league_name = league.get("name") or "-"
    country_name = league.get("country") or "-"
    date_text = fixture.get("date")
    status = fixture.get("status", {}).get("short") or "NS"
    home_score = goals.get("home")
    away_score = goals.get("away")

    result_1x2 = result_over25 = result_btts = None
    if home_score is not None and away_score is not None and status in ("FT", "AET", "PEN"):
        if home_score > away_score:
            result_1x2 = "Home"
        elif away_score > home_score:
            result_1x2 = "Away"
        else:
            result_1x2 = "Draw"
        result_over25 = "Yes" if (home_score + away_score) >= 3 else "No"
        result_btts = "Yes" if home_score > 0 and away_score > 0 else "No"

    return (
        "api_football",
        provider_fixture_id,
        league_name,
        country_name,
        home_name,
        away_name,
        date_text,
        status,
        home_score,
        away_score,
        result_1x2,
        result_over25,
        result_btts,
    )

UPSERT_SQL = """
INSERT INTO fixtures (
    primary_provider, provider_fixture_id_primary, league_name, country_name,
    home_team, away_team, starting_at_utc, status, home_score, away_score,
    result_1x2, result_over25, result_btts, updated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
ON CONFLICT (primary_provider, provider_fixture_id_primary)
DO UPDATE SET
    league_name = EXCLUDED.league_name,
    country_name = EXCLUDED.country_name,
    home_team = EXCLUDED.home_team,
    away_team = EXCLUDED.away_team,
    starting_at_utc = EXCLUDED.starting_at_utc,
    status = EXCLUDED.status,
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    result_1x2 = EXCLUDED.result_1x2,
    result_over25 = EXCLUDED.result_over25,
    result_btts = EXCLUDED.result_btts,
    updated_at = NOW()
"""

def main():
    print("API-FOOTBALL verileri çekiliyor...")
    items = service.get_today_and_tomorrow_fixtures()

    print(f"Toplam işlenecek kayıt: {len(items)}")

    rows = [prepare_fixture_row(item) for item in items]

    conn = get_connection()
    cur = conn.cursor()
    try:
        processed = 0
        for batch in chunked(rows, 50):
            cur.executemany(UPSERT_SQL, batch)
            conn.commit()
            processed += len(batch)
            print(f"İşlenen kayıt: {processed}/{len(rows)}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    print(f"update_fixtures tamamlandı. kayıt sayısı: {len(rows)}")

if __name__ == "__main__":
    main()