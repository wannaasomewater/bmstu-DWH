# DWH онлайн-кинотеатра — итоговый проект

## Архитектура
```
Генератор ─┬─→ Kafka ─┬─→ ClickHouse KafkaEngine -> MV -> events (ReplacingMergeTree)
│ │ └→ speed_views_per_minute (Refreshable MV)
│ └─→ s3_archiver (CG=s3_archiver_group) -> S3 raw_events/raw_profile_changes
│
Каталог ───┴─→ S3 ref_catalogue/

Airflow DAG - prepare -> scd2 -> marts -> validate -> publish -> advance_boundary
(Spark, RDD↔DataFrame сверка, публикация в ClickHouse, T только после успеха)
```

## Проектные решения

### Speed-слой
- Сначала сделал speed_views_per_minute на SummingMergeTree. На повторах из demo-набора он начал складывать дельты дважды, и FINAL не помогал — суммирование это не дедупликация. Переделал на ReplacingMergeTree + Refreshable MV (REFRESH EVERY 30 SECOND): MV при каждом refresh полностью пересчитывает агрегат по events FINAL, где для каждого event_id уже лежит одна версия. Проверил на 6138 сообщении (6000 уникальных + 138 дублей) — count() и uniqExact(event_id) совпали.

### S3-архив 
- **Отдельная CG `s3_archiver_group`**, `auto_offset_reset=earliest`, `enable.auto.commit=True`.
- Партиционирование `raw_events/dt=YYYY-MM-DD` по `event_time[:10]` (business date), `raw_profile_changes/dt=YYYY-MM-DD` по `effective_at[:10]`.
- `ref_catalogue/` - каталог (200 объектов), `control_set/` - контрольный набор для RDD↔DF.

### SCD2 
 - Сначала row_number() по (user_id, effective_at, available_at ASC) — отбрасываю повторную доставку, оставляю самую раннюю. -
 - Потом lag(country/tier) - если state не изменился, версия не создаётся.
 - Потом lead(effective_at) даёт valid_to, интервалы полуоткрытые [valid_from, valid_to), событие на границе относится к новой версии. Открытая версия получает 2105-12-31 как sentinel.
 - Для проверки пересечений — отдельный запрос в validate через leadInFrame по valid_from; если valid_to > next_from — пайплайн падает. После первой реализации было 3 пересечения — забыл, что повторный state с новым change_id тоже нужно схлопывать. После добавления фильтра — 0.

### Batch + исторический JOIN 
- JOIN с SCD2 по `event_time ∈ [valid_from, valid_to)` - версия профиля **на момент события**.
- JOIN с каталогом по `content_id`. `unmatched=0` - каждая запись получила ровно одну версию профиля и один объект каталога.
- Дедупликация `dropDuplicates(["event_id"])` до JOIN - повторы не задваивают минуты.

### RDD↔DataFrame 
- `validate` читает `s3a://dwh-archive/control_set/*.jsonl` (fixture, 28 строк).
- Top-5: `count DESC, content_id ASC` (детерминированный tie-break).
- Word-freq: `lower(query)`, `split\s+`, `length > 4`.
- Оба правила дают идентичный результат в RDD и DataFrame.

### Публикация 
- `ALTER TABLE … DELETE WHERE run_id=current` + `INSERT` в ReplacingMergeTree(processed_at). При пересчёте старые версии по `(minute, content_id)` схлопываются.
- Метаполя `run_id`, `calc_interval_start`, `calc_interval_end`, `processed_at` - по результату определяется запуск.

### Граница T 
- `report_boundary(id=1, boundary_t)` - точка раздела.
- `common_report`: `batch WHERE minute < T UNION ALL speed WHERE minute >= T`.
- `advance_boundary` двигает `T <- interval_end` **только если** `current_t == interval_start`. Пересчёт исторического дня не откатывает T.

### Поздние события 
- `s3_archiver` пишет поздние события с их **business date**, а не с текущей датой.
- Пересчёт затронутого дня берёт все файлы по `dt=<business_date>`.
- Демонстрация: `stage1` (без late) `views=1421, secs=3581408` → `stage2` (после late) `views=1491, secs=3755275`. Δ `+70` просмотров за 1 января, дублей нет.

## Порядок запуска

### 0. Jar-файлы для Spark

Положить в `solution/scripts/jars/`:
- `hadoop-aws-3.4.1.jar` (Maven Central: `org.apache.hadoop:hadoop-aws:3.4.1`)
- `bundle-2.20.0.jar` — AWS SDK Bundle для S3A (`com.amazonaws:aws-java-sdk-bundle`, https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle)

Затем `docker exec spark-master ls /opt/spark/scripts/jars/`.

### 1. Инфраструктура
```bash
cd solution/docker
docker compose up -d
docker exec kafka kafka-topics.sh --bootstrap-server kafka:29092 \
  --create --topic cinema.events --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec kafka kafka-topics.sh --bootstrap-server kafka:29092 \
  --create --topic cinema.profile_changes --partitions 3 --replication-factor 1 2>/dev/null || true
```

### 2. DDL
```bash
docker exec -i clickhouse clickhouse-client --multiquery < sql/01_operational.sql
docker exec -i clickhouse clickhouse-client --multiquery < sql/02_batch.sql
```

### 3. Данные

```bash
python3 scripts/replay_kafka.py --dataset data/demo --rate 800
python3 scripts/s3_archiver.py --idle-timeout 25 --max-runtime 240
```

### 4. Через Airflow

```bash
docker exec airflow airflow dags unpause dwh_batch_daily
docker exec airflow airflow dags trigger dwh_batch_daily \
  --conf '{"calc_date": "2026-01-01"}'
```

Проверка:

```bash
docker exec airflow airflow dags list-runs -d dwh_batch_daily
docker exec airflow airflow tasks states-for-dag-run dwh_batch_daily <run_id>
```

### 5. Пересчёт после late-данных

```bash
python3 scripts/replay_kafka.py --dataset data/demo --from-arrival 2026-01-04T00:00:00Z --rate 800
python3 scripts/s3_archiver.py --idle-timeout 20 --max-runtime 180
docker exec airflow airflow dags trigger dwh_batch_daily --conf '{"calc_date": "2026-01-01"}'
```

### Примеры запросов к итоговым отчётам

```sql
-- 1. Общий отчёт (batch для минут < T, speed для минут >= T)
SELECT source, count() rows, min(minute), max(minute),
       sum(views_count) views, sum(total_seconds) secs
FROM common_report GROUP BY source ORDER BY 1;

-- 2. Детализация
SELECT * FROM common_report ORDER BY minute, content_id LIMIT 50;

-- 3. Топ-5 контента за день
SELECT day, rank, content_id, views_count
FROM batch_top5_content_daily FINAL
WHERE day = '2026-01-01' ORDER BY rank;

-- 4. Просмотры по стране и тарифу на дату просмотра
SELECT day, country, subscription_tier, views_count, total_seconds
FROM batch_views_by_country_tier FINAL
WHERE day BETWEEN '2026-01-01' AND '2026-01-03'
ORDER BY day, country, subscription_tier;

-- 5. Частоты слов в поиске
SELECT day, word, freq FROM batch_search_word_freq FINAL
WHERE day = '2026-01-01' ORDER BY freq DESC, word LIMIT 10;

-- 6. SCD2 история пользователя
SELECT user_id, country, subscription_tier,
       toString(valid_from) vf, toString(valid_to) vt
FROM scd2_profiles FINAL WHERE user_id = 1001 ORDER BY valid_from;

-- 7. Граница T
SELECT toString(boundary_t) FROM report_boundary FINAL WHERE id=1;

-- 8. Дубли по ключу витрины 
SELECT count() FROM (
  SELECT minute, content_id, count() c
  FROM batch_views_per_minute FINAL
  GROUP BY minute, content_id HAVING c > 1);

-- 9. Пересечения SCD2 
SELECT count() AS overlaps
FROM (SELECT * FROM scd2_profiles FINAL) AS a
INNER JOIN (SELECT * FROM scd2_profiles FINAL) AS b
  ON a.user_id = b.user_id
 AND a.valid_from < b.valid_from
 AND a.valid_to   > b.valid_from;

-- 10. Speed-слой
SELECT minute, content_id, views_count, total_seconds
FROM speed_views_per_minute FINAL ORDER BY minute DESC LIMIT 20;
```
### Компоненты


| Компонент | Образ / версия | Назначение |
|---|---|---|
| Kafka | `bitnamilegacy/kafka:4.0.0` | events + profile_changes |
| ClickHouse | `clickhouse-server:latest` (26.10) | Kafka Engine, витрины, общий отчёт |
| MinIO | `pgsty/silo` | S3-совместимое хранилище |
| Spark | `bitnamilegacy/spark:4.0.0` (master + worker) | batch, SCD2, RDD↔DF |
| Airflow | `apache/airflow:2.8.4-python3.11` | DAG |

## Демонстрация

**Baseline 1 января (`stage1`, `available_at < 2026-01-04`):**
`events_dedup=5690`, `scd2=179`, `views=1421`, `secs=3581408`.

**После late tail (`stage2`, `available_at ≥ 2026-01-04`):**
`events_dedup=6000`, `scd2=187`, `views=1491`, `secs=3755275`.

Δ **+70 просмотров**, **+173 867 секунд**. Дублей по ключу `(minute, content_id)` = **0**.

**Airflow (`manual__2026-11-24T13:43:27 success`):** 6 задач `success`, T `2026-01-01 → 2026-01-02`, batch = `1491 / 3755275` — идемпотентно с `stage2`.

### По пунктам из REQUIREMENTS

| № | Где реализовано |
|---|---|
| 1 | `sql/01_operational.sql`, `speed_views_per_minute_mv` |
| 2 | `scripts/s3_archiver.py`, `s3a://dwh-archive/raw_*` |
| 3 | `scripts/spark_batch.py:stage_scd2`, `scd2_profiles` |
| 4 | `scripts/spark_batch.py:stage_marts` |
| 5 | `scripts/spark_batch.py:stage_validate` |
| 6 | `solution/dags/dwh_batch_dag.py` |
| 7 | `common_report`, `report_boundary` |
| 8 | `scripts/spark_batch.py:stage_publish` |
| 9 | демонстрация `stage1 -> stage2` выше |
