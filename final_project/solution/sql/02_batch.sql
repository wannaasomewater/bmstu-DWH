-- Batch target tables
DROP TABLE IF EXISTS batch_views_per_minute;
DROP TABLE IF EXISTS batch_views_by_country_tier;
DROP TABLE IF EXISTS batch_top5_content_daily;
DROP TABLE IF EXISTS batch_search_word_freq;
DROP TABLE IF EXISTS scd2_profiles;
DROP TABLE IF EXISTS report_boundary;
DROP VIEW  IF EXISTS common_report;

CREATE TABLE batch_views_per_minute (
    minute              DateTime,
    content_id          UInt32,
    views_count         UInt64,
    total_seconds       UInt64,
    run_id              String,
    calc_interval_start DateTime,
    calc_interval_end   DateTime,
    processed_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (minute, content_id);

CREATE TABLE batch_views_by_country_tier (
    day                 Date,
    country             LowCardinality(String),
    subscription_tier   LowCardinality(String),
    views_count         UInt64,
    total_seconds       UInt64,
    run_id              String,
    calc_interval_start DateTime,
    calc_interval_end   DateTime,
    processed_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (day, country, subscription_tier);

CREATE TABLE batch_top5_content_daily (
    day                 Date,
    rank                UInt8,
    content_id          UInt32,
    views_count         UInt64,
    run_id              String,
    calc_interval_start DateTime,
    calc_interval_end   DateTime,
    processed_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (day, rank);

CREATE TABLE batch_search_word_freq (
    day                 Date,
    word                String,
    freq                UInt64,
    run_id              String,
    calc_interval_start DateTime,
    calc_interval_end   DateTime,
    processed_at        DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (day, word);

CREATE TABLE scd2_profiles (
    user_id           UInt32,
    country           LowCardinality(String),
    subscription_tier LowCardinality(String),
    valid_from        DateTime,
    valid_to          DateTime,
    change_id         String,
    run_id            String,
    processed_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (user_id, valid_from);

-- Граница T - batch покрывает minute < T, speed — minute >= T
CREATE TABLE report_boundary (
    id          UInt8 DEFAULT 1,
    boundary_t  DateTime,
    updated_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

INSERT INTO report_boundary (id, boundary_t) VALUES (1, toDateTime('1970-01-01 00:00:00'));

CREATE VIEW common_report AS
WITH (SELECT boundary_t FROM report_boundary FINAL WHERE id = 1) AS T
SELECT
    minute,
    content_id,
    sum(views_count)   AS views_count,
    sum(total_seconds) AS total_seconds,
    'batch'            AS source
FROM batch_views_per_minute FINAL
WHERE minute < T
GROUP BY minute, content_id
UNION ALL
SELECT
    minute,
    content_id,
    views_count,
    total_seconds,
    'speed'            AS source
FROM speed_views_per_minute FINAL
WHERE minute >= T;
