"""
Master Query Database Schema
Единая таблица со ВСЕМИ данными по запросу для быстрой переклассификации
"""

MASTER_QUERY_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS master_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    
    -- ========================================
    -- БАЗОВЫЕ ДАННЫЕ
    -- ========================================
    keyword TEXT NOT NULL,
    frequency_world INTEGER DEFAULT 0,
    frequency_exact INTEGER DEFAULT 0,
    
    -- ========================================
    -- ПРЕДОБРАБОТКА (быстрый кэш)
    -- ========================================
    normalized TEXT,
    lemmatized TEXT,
    words_count INTEGER DEFAULT 0,
    main_words TEXT,
    key_phrase TEXT,
    
    -- ========================================
    -- NER & GEO
    -- ========================================
    ner_entities TEXT,  -- JSON массив
    ner_locations TEXT,  -- JSON массив
    has_geo BOOLEAN DEFAULT 0,
    geo_type TEXT,  -- country/city/region
    geo_country TEXT,
    geo_city TEXT,
    
    -- ========================================
    -- INTENT CLASSIFICATION (кэш)
    -- ========================================
    main_intent TEXT,  -- commercial/informational/commercial_geo/navigational
    commercial_score REAL DEFAULT 0.0,
    informational_score REAL DEFAULT 0.0,
    navigational_score REAL DEFAULT 0.0,
    
    -- Флаги из keyword_group
    is_commercial BOOLEAN DEFAULT 0,
    is_wholesale BOOLEAN DEFAULT 0,
    is_urgent BOOLEAN DEFAULT 0,
    is_diy BOOLEAN DEFAULT 0,
    is_review BOOLEAN DEFAULT 0,
    is_brand_query BOOLEAN DEFAULT 0,
    
    -- ========================================
    -- SERP ДАННЫЕ (из xmlstock) + СТАТУС ЗАГРУЗКИ
    -- ========================================
    serp_query_hash TEXT,  -- MD5 хэш для уникальности
    serp_req_id TEXT,  -- ID запроса в xmlstock (для отслеживания)
    serp_status TEXT DEFAULT 'pending',  -- pending/processing/completed/error
    serp_error_message TEXT,  -- Текст ошибки если есть
    serp_found_docs INTEGER,
    serp_main_pages_count INTEGER,
    serp_titles_with_keyword INTEGER,
    serp_commercial_domains INTEGER,
    serp_info_domains INTEGER,
    serp_created_at TIMESTAMP,
    serp_updated_at TIMESTAMP,  -- Последнее обновление статуса
    
    -- ========================================
    -- SERP OFFER INFO (коммерческие данные)
    -- ========================================
    serp_intent TEXT,  -- commercial/informational (по offer_info)
    serp_confidence REAL DEFAULT 0.0,
    serp_docs_with_offers INTEGER DEFAULT 0,
    serp_total_docs INTEGER DEFAULT 0,
    serp_offer_ratio REAL DEFAULT 0.0,
    
    -- Цены
    serp_avg_price REAL,
    serp_min_price REAL,
    serp_max_price REAL,
    serp_median_price REAL,
    serp_currency TEXT DEFAULT 'RUR',
    serp_offers_count INTEGER DEFAULT 0,
    serp_offers_with_discount INTEGER DEFAULT 0,
    serp_avg_discount_percent REAL,
    
    -- ========================================
    -- SERP TOP URLs (для кластеризации)
    -- ========================================
    -- Храним TOP-20 URL как JSON массив для быстрой кластеризации
    serp_top_urls TEXT,  -- JSON: [{url, domain, position, title, is_commercial}, ...]
    
    -- ========================================
    -- LSI PHRASES
    -- ========================================
    serp_lsi_phrases TEXT,  -- JSON массив с частотами
    
    -- ========================================
    -- YANDEX DIRECT DATA
    -- ========================================
    direct_shows INTEGER,
    direct_clicks INTEGER,
    direct_ctr REAL,
    direct_min_cpc REAL,
    direct_avg_cpc REAL,
    direct_max_cpc REAL,
    direct_recommended_cpc REAL,
    direct_competition_level TEXT,  -- LOW/MEDIUM/HIGH
    direct_first_place_bid REAL,
    direct_first_place_price REAL,
    
    -- ========================================
    -- SEO МЕТРИКИ (рассчитываются)
    -- ========================================
    kei REAL DEFAULT 0.0,
    difficulty REAL DEFAULT 0.0,
    competition_score REAL DEFAULT 0.0,
    potential_traffic REAL DEFAULT 0.0,
    expected_ctr REAL DEFAULT 0.0,
    
    -- ========================================
    -- БРЕНДЫ
    -- ========================================
    detected_brand TEXT,
    brand_confidence REAL DEFAULT 0.0,
    
    -- ========================================
    -- ВОРОНКА
    -- ========================================
    funnel_stage TEXT,  -- awareness/consideration/decision
    funnel_priority INTEGER DEFAULT 5,
    
    -- ========================================
    -- КЛАСТЕРИЗАЦИЯ (пересчитывается на лету)
    -- ========================================
    -- Эти поля НЕ хранятся - рассчитываются при запуске
    -- semantic_cluster_id, cluster_name, topic_id и т.д.
    -- добавляются в DataFrame динамически
    
    -- ========================================
    -- МЕТАДАННЫЕ
    -- ========================================
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_version TEXT DEFAULT '2.0',
    
    -- Уникальность по группе + запросу
    UNIQUE(group_name, keyword),
    FOREIGN KEY (group_name) REFERENCES query_groups(group_name)
)
"""

# ========================================
# ИНДЕКСЫ для быстрого поиска (как в PostgreSQL)
# ========================================
MASTER_QUERY_INDEXES = [
    # Основной индекс - группа + запрос (UNIQUE поиск)
    """CREATE UNIQUE INDEX IF NOT EXISTS idx_master_group_keyword 
       ON master_queries(group_name, keyword)""",
    
    # Поиск по нормализованной форме (для дедупликации)
    """CREATE INDEX IF NOT EXISTS idx_master_normalized 
       ON master_queries(group_name, normalized)""",
    
    # Фильтр по интенту (для быстрого WHERE main_intent = 'commercial')
    """CREATE INDEX IF NOT EXISTS idx_master_intent 
       ON master_queries(main_intent)""",
    
    # Composite индекс для фильтра по интенту + частотности
    """CREATE INDEX IF NOT EXISTS idx_master_intent_freq 
       ON master_queries(group_name, main_intent, frequency_world DESC)""",
    
    # Связь с SERP данными
    """CREATE INDEX IF NOT EXISTS idx_master_serp_hash 
       ON master_queries(serp_query_hash)""",
    
    # Сортировка по частотности (для TOP-N запросов)
    """CREATE INDEX IF NOT EXISTS idx_master_frequency 
       ON master_queries(frequency_world DESC)""",
    
    # Сортировка по KEI (для приоритизации)
    """CREATE INDEX IF NOT EXISTS idx_master_kei 
       ON master_queries(kei DESC)""",
    
    # Composite для коммерческих запросов с SERP
    """CREATE INDEX IF NOT EXISTS idx_master_commercial 
       ON master_queries(is_commercial, serp_intent, serp_offer_ratio DESC)""",
    
    # ГЕО-запросы (для фильтра по городам)
    """CREATE INDEX IF NOT EXISTS idx_master_geo 
       ON master_queries(has_geo, geo_city, geo_country)""",
    
    # Брендовые запросы
    """CREATE INDEX IF NOT EXISTS idx_master_brand 
       ON master_queries(is_brand_query, detected_brand)""",
    
    # Воронка продаж
    """CREATE INDEX IF NOT EXISTS idx_master_funnel 
       ON master_queries(funnel_stage, funnel_priority DESC)""",
    
    # Composite для SEO-метрик (KEI + difficulty)
    """CREATE INDEX IF NOT EXISTS idx_master_seo_metrics 
       ON master_queries(group_name, kei DESC, difficulty ASC)""",
    
    # Yandex Direct данные (для фильтра по CPC)
    """CREATE INDEX IF NOT EXISTS idx_master_direct_cpc 
       ON master_queries(direct_avg_cpc, direct_competition_level)""",
    
    # SERP цены (для анализа ценовых диапазонов)
    """CREATE INDEX IF NOT EXISTS idx_master_prices 
       ON master_queries(serp_avg_price, serp_currency)""",
    
    # SERP статусы (для отслеживания загрузки и восстановления после падения)
    """CREATE INDEX IF NOT EXISTS idx_master_serp_status 
       ON master_queries(serp_status, serp_req_id)""",
    
    # Поиск незавершённых SERP запросов (для восстановления)
    """CREATE INDEX IF NOT EXISTS idx_master_serp_pending 
       ON master_queries(group_name, serp_status)""",
    
    # Covering index для экспорта (включает все часто используемые колонки)
    # Примечание: SQLite не поддерживает INCLUDE, поэтому используем обычный composite индекс
    """CREATE INDEX IF NOT EXISTS idx_master_export_covering 
       ON master_queries(group_name, frequency_world DESC, kei DESC, keyword, main_intent, serp_offer_ratio, direct_avg_cpc)""",
]

# View для быстрого экспорта в Excel/CSV
# Примечание: SQLite view не поддерживает параметры, поэтому используем без WHERE
MASTER_EXPORT_VIEW = """
CREATE VIEW IF NOT EXISTS export_ready_queries AS
SELECT 
    group_name,
    keyword,
    frequency_world,
    frequency_exact,
    normalized,
    lemmatized,
    words_count,
    
    -- Intent
    main_intent,
    commercial_score,
    informational_score,
    
    -- GEO
    has_geo,
    geo_city,
    geo_country,
    
    -- SERP
    serp_found_docs,
    serp_offer_ratio,
    serp_avg_price,
    serp_currency,
    
    -- Direct
    direct_shows,
    direct_ctr,
    direct_avg_cpc,
    direct_competition_level,
    
    -- Metrics
    kei,
    difficulty,
    potential_traffic,
    
    -- Бренды
    detected_brand,
    is_brand_query,
    
    -- Воронка
    funnel_stage,
    funnel_priority
    
FROM master_queries
ORDER BY kei DESC
"""

__all__ = [
    'MASTER_QUERY_TABLE_SCHEMA',
    'MASTER_QUERY_INDEXES',
    'MASTER_EXPORT_VIEW'
]

