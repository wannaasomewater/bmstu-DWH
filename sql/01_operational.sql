-- Speed layer. Имена полей — строго по data/README (watch_time_seconds, query).
DROP VIEW IF EXISTS speed_views_per_minute_mv;
DROP TABLE IF EXISTS speed_views_per_minute;
DROP VIEW IF EXISTS mv_events;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS kafka_events;
DROP TABLE IF EXISTS mv_speed_views;
DROP TABLE IF EXISTS mv_events_old;
DROP TABLE IF EXISTS target_events;
DROP TABLE IF EXISTS mv_events_old;

-- Kafka Engine: читает топик cinema.events отдельной consumer group.
CREATE TABLE kafka_events (
    event_id           String,
    event_time         DateTime,
    user_id            UInt32,
    event_type         LowCardinality(String),
    content_id         Nullable(UInt32),
    watch_time_seconds Nullable(UInt32),
    query              Nullable(String),
    available_at       String
) ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list   = 'cinema.events',
         kafka_group_name   = 'ch_speed_events_v1',
         kafka_format       = 'JSONEachRow',
         kafka_num_consumers = 1,
         input_format_skip_unknown_fields = 1;

-- Каноническая таблица событий: дедуп по event_id через ReplacingMergeTree.
CREATE TABLE events (
    event_id           String,
    event_time         DateTime,
    user_id            UInt32,
    event_type         LowCardinality(String),
    content_id         Nullable(UInt32),
    watch_time_seconds Nullable(UInt32),
    query              Nullable(String),
    _loaded_at         DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY event_id;

CREATE MATERIALIZED VIEW mv_events TO events AS
SELECT event_id, event_time, user_id, event_type,
       content_id, watch_time_seconds, query, now() AS _loaded_at
FROM kafka_events;

-- Target speed-агрегат. Refreshable MV периодически пересчитывает
-- из events FINAL, поэтому повторы не завышают показатели.
CREATE TABLE speed_views_per_minute (
    minute        DateTime,
    content_id    UInt32,
    views_count   UInt64,
    total_seconds UInt64,
    refreshed_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(refreshed_at)
ORDER BY (minute, content_id);

CREATE MATERIALIZED VIEW speed_views_per_minute_mv
REFRESH EVERY 30 SECOND
TO speed_views_per_minute
AS
SELECT
    toStartOfMinute(event_time) AS minute,
    content_id,
    count()                     AS views_count,
    sum(watch_time_seconds)     AS total_seconds
FROM events FINAL
WHERE event_type = 'view' AND content_id IS NOT NULL
GROUP BY minute, content_id;

-- Сразу дёрнем первичный refresh, чтобы данные появились без ожидания.
SYSTEM REFRESH VIEW speed_views_per_minute_mv;
