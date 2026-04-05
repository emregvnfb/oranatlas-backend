from db import fetch_all
from services.prediction_service import build_simple_fixture_feature, build_simple_prediction

def main():
    fixtures = fetch_all(
        '''
        SELECT f.id, f.starting_at_utc
        FROM fixtures f
        JOIN odds_latest ol ON ol.fixture_id = f.id
        WHERE f.status NOT IN ('FT', 'AET', 'PEN')
          AND f.starting_at_utc >= (NOW() AT TIME ZONE 'UTC')
        GROUP BY f.id, f.starting_at_utc
        ORDER BY f.starting_at_utc ASC
        LIMIT 500
        '''
    )

    count = 0
    total = len(fixtures)

    for idx, row in enumerate(fixtures, start=1):
        try:
            feature = build_simple_fixture_feature(row["id"])
            if feature is None:
                continue

            prediction = build_simple_prediction(row["id"])
            if prediction is None:
                continue

            count += 1

            if idx % 25 == 0 or idx == total:
                print(f"İşlenen fixture: {idx}/{total} | başarılı feature: {count}")

        except Exception as e:
            print(f"Hata (fixture_id={row['id']}) atlandı: {e}")
            continue

    print(f"build_features tamamlandı. feature üretilen maç: {count}")

if __name__ == "__main__":
    main()
