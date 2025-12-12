"""
SQL схема для кэша Yandex Direct.

Определяет структуру таблицы и индексы.
"""


CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS direct_forecasts (
        phrase TEXT PRIMARY KEY,
        geo_id INTEGER,
        shows INTEGER,
        clicks INTEGER,
        ctr REAL,
        premium_ctr REAL,
        min_cpc REAL,
        avg_cpc REAL,
        max_cpc REAL,
        recommended_cpc REAL,
        competition_level TEXT,
        first_place_bid REAL,
        first_place_price REAL,
        created_at TEXT,
        updated_at TEXT
    )
"""

CREATE_PHRASE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_phrase 
    ON direct_forecasts(phrase)
"""

CREATE_UPDATED_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_updated_at 
    ON direct_forecasts(updated_at)
"""

SELECT_FORECAST_SQL = """
    SELECT * FROM direct_forecasts 
    WHERE phrase = ? AND geo_id = ?
"""

INSERT_FORECAST_SQL = """
    INSERT OR REPLACE INTO direct_forecasts (
        phrase, geo_id, shows, clicks, ctr, premium_ctr,
        min_cpc, avg_cpc, max_cpc, recommended_cpc,
        competition_level, first_place_bid, first_place_price,
        created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
              COALESCE((SELECT created_at FROM direct_forecasts WHERE phrase = ?), ?), 
              ?)
"""

DELETE_OLD_SQL = """
    DELETE FROM direct_forecasts 
    WHERE updated_at < ?
"""





