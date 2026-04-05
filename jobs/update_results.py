from db import fetch_all, execute

def main():
    rows = fetch_all(
        '''
        SELECT id, home_score, away_score, status
        FROM fixtures
        WHERE status IN ('FT', 'AET', 'PEN')
          AND (result_1x2 IS NULL OR result_over25 IS NULL OR result_btts IS NULL)
        '''
    )
    updated = 0
    for row in rows:
        home = row["home_score"]
        away = row["away_score"]
        if home is None or away is None:
            continue
        if home > away:
            result_1x2 = "Home"
        elif away > home:
            result_1x2 = "Away"
        else:
            result_1x2 = "Draw"

        result_over25 = "Yes" if (home + away) >= 3 else "No"
        result_btts = "Yes" if home > 0 and away > 0 else "No"

        execute(
            '''
            UPDATE fixtures
            SET result_1x2 = %s,
                result_over25 = %s,
                result_btts = %s,
                updated_at = NOW()
            WHERE id = %s
            ''',
            (result_1x2, result_over25, result_btts, row["id"]),
        )
        updated += 1
    print(f"update_results tamamlandı. güncellenen maç: {updated}")

if __name__ == "__main__":
    main()
