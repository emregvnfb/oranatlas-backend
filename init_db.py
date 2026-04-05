from db import execute

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS fixtures (
        id SERIAL PRIMARY KEY,
        primary_provider TEXT NOT NULL DEFAULT 'api_football',
        provider_fixture_id_primary TEXT NOT NULL,
        league_name TEXT,
        country_name TEXT,
        home_team TEXT,
        away_team TEXT,
        starting_at_utc TIMESTAMPTZ,
        status TEXT,
        home_score INTEGER,
        away_score INTEGER,
        result_1x2 TEXT,
        result_over25 TEXT,
        result_btts TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (primary_provider, provider_fixture_id_primary)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fixture_provider_map (
        id SERIAL PRIMARY KEY,
        fixture_id INTEGER NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
        provider_name TEXT NOT NULL,
        provider_fixture_id TEXT NOT NULL,
        UNIQUE (fixture_id, provider_name, provider_fixture_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bookmakers (
        id SERIAL PRIMARY KEY,
        provider_name TEXT NOT NULL,
        bookmaker_name TEXT NOT NULL,
        normalized_bookmaker_key TEXT NOT NULL,
        UNIQUE (provider_name, normalized_bookmaker_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS markets (
        id SERIAL PRIMARY KEY,
        market_key TEXT NOT NULL UNIQUE,
        market_name TEXT,
        market_group TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_selections (
        id SERIAL PRIMARY KEY,
        market_id INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
        selection_key TEXT NOT NULL,
        selection_name TEXT,
        UNIQUE (market_id, selection_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS odds_snapshots (
        id SERIAL PRIMARY KEY,
        fixture_id INTEGER NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
        provider_name TEXT NOT NULL,
        bookmaker_id INTEGER NOT NULL REFERENCES bookmakers(id) ON DELETE CASCADE,
        market_id INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
        selection_id INTEGER NOT NULL REFERENCES market_selections(id) ON DELETE CASCADE,
        odd_decimal DOUBLE PRECISION NOT NULL,
        implied_probability DOUBLE PRECISION,
        is_opening_snapshot BOOLEAN DEFAULT FALSE,
        is_closing_snapshot BOOLEAN DEFAULT FALSE,
        is_inplay BOOLEAN DEFAULT FALSE,
        captured_at_utc TIMESTAMPTZ NOT NULL,
        raw_payload JSONB
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_odds_snapshots_fixture_id ON odds_snapshots(fixture_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS odds_latest (
        fixture_id INTEGER NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
        bookmaker_id INTEGER NOT NULL REFERENCES bookmakers(id) ON DELETE CASCADE,
        market_id INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
        selection_id INTEGER NOT NULL REFERENCES market_selections(id) ON DELETE CASCADE,
        latest_odd_decimal DOUBLE PRECISION NOT NULL,
        latest_captured_at_utc TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (fixture_id, bookmaker_id, market_id, selection_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fixture_features (
        fixture_id INTEGER PRIMARY KEY REFERENCES fixtures(id) ON DELETE CASCADE,
        consensus_home_prob DOUBLE PRECISION,
        consensus_draw_prob DOUBLE PRECISION,
        consensus_away_prob DOUBLE PRECISION,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS model_predictions (
        id SERIAL PRIMARY KEY,
        fixture_id INTEGER NOT NULL REFERENCES fixtures(id) ON DELETE CASCADE,
        model_version TEXT NOT NULL,
        market_key TEXT NOT NULL,
        selection_key TEXT NOT NULL,
        predicted_probability DOUBLE PRECISION,
        confidence_score DOUBLE PRECISION,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_predictions_fixture_id ON model_predictions(fixture_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS coupon_packages (
        coupon_date DATE NOT NULL,
        coupon_type TEXT NOT NULL,
        package_json JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (coupon_date, coupon_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS coupon_result_cache (
        coupon_date DATE PRIMARY KEY,
        result_json JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS editor_coupon (
        id SERIAL PRIMARY KEY,
        coupon_text TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """
]

def main():
    print("🚀 Veritabanı şeması oluşturuluyor...")
    for i, stmt in enumerate(SCHEMA_STATEMENTS, start=1):
        execute(stmt)
        print(f"✅ Adım {i}/{len(SCHEMA_STATEMENTS)} tamamlandı")
    print("🎯 Veritabanı hazır")

if __name__ == "__main__":
    main()
