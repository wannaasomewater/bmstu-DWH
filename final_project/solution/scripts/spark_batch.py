#!/usr/bin/env python3
"""Spark batch: prepare -> scd2 -> marts -> validate -> publish."""
import argparse, sys
from datetime import datetime
import clickhouse_connect
from pyspark.sql import SparkSession, functions as F, Window

BUCKET = "dwh-archive"

def make_spark():
    return (SparkSession.builder
        .appName("DWH_Batch")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())

def ch_client():
    return clickhouse_connect.get_client(host="clickhouse", port=8123,
                                          username="default", password="")

def work(run_id, tail):
    return f"s3a://{BUCKET}/work/{run_id}/{tail}"

def stage_prepare(spark, run_id, *_):
    print(f"[prepare] run_id={run_id}")
    events    = spark.read.json(f"s3a://{BUCKET}/raw_events/*/*.jsonl")
    profiles  = spark.read.json(f"s3a://{BUCKET}/raw_profile_changes/*/*.jsonl")
    catalogue = spark.read.json(f"s3a://{BUCKET}/ref_catalogue/*.jsonl")
    print(f"[prepare] events_raw={events.count()}  profiles_raw={profiles.count()}  catalogue={catalogue.count()}")

    w = Window.partitionBy("event_id").orderBy(F.desc("available_at"))
    ev = events.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    print(f"[prepare] events_dedup={ev.count()}")

    w2 = Window.partitionBy("change_id").orderBy(F.desc("available_at"))
    pr = profiles.withColumn("rn", F.row_number().over(w2)).filter(F.col("rn") == 1).drop("rn")
    print(f"[prepare] profiles_dedup={pr.count()}")

    ev.write.mode("overwrite").parquet(work(run_id, "events"))
    pr.write.mode("overwrite").parquet(work(run_id, "profiles"))
    catalogue.write.mode("overwrite").parquet(work(run_id, "catalogue"))

def stage_scd2(spark, run_id, *_):
    p = (spark.read.parquet(work(run_id, "profiles"))
         .withColumn("effective_at", F.to_timestamp("effective_at"))
         .withColumn("available_at", F.to_timestamp("available_at")))

    w1 = Window.partitionBy("user_id","effective_at").orderBy(F.asc("available_at"), F.asc("change_id"))
    first = p.withColumn("rn", F.row_number().over(w1)).filter(F.col("rn") == 1).drop("rn")

    w2 = Window.partitionBy("user_id").orderBy(F.asc("effective_at"))
    changed = (first
        .withColumn("pc", F.lag("country").over(w2))
        .withColumn("pt", F.lag("subscription_tier").over(w2))
        .filter(F.col("pc").isNull() | (F.col("pc") != F.col("country"))
                | (F.col("pt") != F.col("subscription_tier"))))

    w3 = Window.partitionBy("user_id").orderBy(F.asc("effective_at"))
    scd2 = (changed
        .withColumn("valid_from", F.col("effective_at"))
        .withColumn("valid_to", F.coalesce(F.lead("effective_at").over(w3),
                                           F.to_timestamp(F.lit("2105-12-31 00:00:00"))))
        .select("user_id","country","subscription_tier","valid_from","valid_to","change_id"))

    n = scd2.count(); print(f"[scd2] rows={n}")
    scd2.write.mode("overwrite").parquet(work(run_id, "scd2"))

    client = ch_client()
    client.command("TRUNCATE TABLE scd2_profiles")
    if n:
        client.insert("scd2_profiles",
            [(int(r.user_id), r.country, r.subscription_tier,
              r.valid_from, r.valid_to, r.change_id, run_id) for r in scd2.collect()],
            column_names=["user_id","country","subscription_tier",
                          "valid_from","valid_to","change_id","run_id"])
    print(f"[scd2] published={n}")

def stage_marts(spark, run_id, interval_start, interval_end):
    events    = spark.read.parquet(work(run_id, "events"))
    scd2      = spark.read.parquet(work(run_id, "scd2"))
    catalogue = spark.read.parquet(work(run_id, "catalogue"))

    events = (events.withColumn("event_time", F.to_timestamp("event_time"))
        .filter((F.col("event_time") >= F.lit(interval_start))
              & (F.col("event_time") <  F.lit(interval_end))))
    print(f"[marts] events_in_interval={events.count()}")

    views    = events.filter(F.col("event_type") == "view")
    searches = events.filter(F.col("event_type") == "search")

    joined = (views.alias("v").join(
        scd2.alias("p"),
        (F.col("v.user_id") == F.col("p.user_id"))
        & (F.col("v.event_time") >= F.col("p.valid_from"))
        & (F.col("v.event_time") <  F.col("p.valid_to")), "left")
        .join(catalogue.alias("c"), F.col("v.content_id") == F.col("c.content_id"), "left"))

    unmatched = joined.filter(F.col("p.user_id").isNull() | F.col("c.content_id").isNull()).count()
    print(f"[marts] unmatched={unmatched}")

    minute = (joined.filter(F.col("v.content_id").isNotNull())
        .groupBy(F.date_trunc("minute", F.col("v.event_time")).alias("minute"),
                 F.col("v.content_id").cast("int").alias("content_id"))
        .agg(F.count("*").alias("views_count"),
             F.coalesce(F.sum(F.col("v.watch_time_seconds")), F.lit(0)).cast("long").alias("total_seconds")))
    print(f"[marts] minute={minute.count()}")
    minute.write.mode("overwrite").parquet(work(run_id, "mart_minute"))

    daily = (joined.filter(F.col("p.country").isNotNull())
        .groupBy(F.to_date(F.col("v.event_time")).alias("day"),
                 F.col("p.country").alias("country"),
                 F.col("p.subscription_tier").alias("tier"))
        .agg(F.count("*").alias("views_count"),
             F.coalesce(F.sum(F.col("v.watch_time_seconds")), F.lit(0)).cast("long").alias("total_seconds")))
    print(f"[marts] daily={daily.count()}")
    daily.write.mode("overwrite").parquet(work(run_id, "mart_daily"))

    counts = views.groupBy(
        F.to_date(F.col("event_time")).alias("day"),
        F.col("content_id").cast("int").alias("content_id")
    ).agg(F.count("*").alias("views_count"))
    w = Window.partitionBy("day").orderBy(F.desc("views_count"), F.asc("content_id"))
    top5 = counts.withColumn("rank", F.row_number().over(w).cast("int")).filter(F.col("rank") <= 5)
    print(f"[marts] top5={top5.count()}")
    top5.write.mode("overwrite").parquet(work(run_id, "mart_top5"))

    words = (searches.filter(F.col("query").isNotNull())
        .select(F.to_date(F.col("event_time")).alias("day"),
                F.explode(F.split(F.lower(F.col("query")), r"\s+")).alias("word"))
        .filter(F.length(F.col("word")) > 4))
    wf = words.groupBy("day","word").agg(F.count("*").alias("freq"))
    print(f"[marts] words={wf.count()}")
    wf.write.mode("overwrite").parquet(work(run_id, "mart_words"))

def stage_validate(spark, run_id, *_):
    print("[validate] checks")
    errors = []
    minute = spark.read.parquet(work(run_id, "mart_minute"))
    daily  = spark.read.parquet(work(run_id, "mart_daily"))
    top5   = spark.read.parquet(work(run_id, "mart_top5"))
    words  = spark.read.parquet(work(run_id, "mart_words"))
    scd2   = spark.read.parquet(work(run_id, "scd2"))

    for name, df, keys in [
        ("minute", minute, ["minute","content_id"]),
        ("daily",  daily,  ["day","country","tier"]),
        ("top5",   top5,   ["day","rank"]),
        ("words",  words,  ["day","word"]),
    ]:
        if df.groupBy(*keys).count().filter(F.col("count") > 1).count() > 0:
            errors.append(f"{name}: duplicate keys")

    w = Window.partitionBy("user_id").orderBy(F.asc("valid_from"))
    ov = (scd2.withColumn("next_from", F.lead("valid_from").over(w))
              .filter(F.col("next_from").isNotNull())
              .filter(F.col("valid_to") > F.col("next_from"))).count()
    if ov: errors.append(f"scd2: {ov} overlapping intervals")

    try:
        ctl = spark.read.json(f"s3a://{BUCKET}/control_set/*.jsonl")
        n = ctl.count()
        if n > 0:
            print(f"[validate] control_set rows={n}")
            cv = ctl.filter(F.col("event_type") == "view")
            df_t5 = [int(r.content_id) for r in
                cv.groupBy("content_id").count()
                  .orderBy(F.desc("count"), F.asc("content_id")).limit(5).collect()]
            rdd_t5 = [x[0] for x in
                cv.select("content_id").rdd.map(lambda r: (int(r.content_id), 1))
                  .reduceByKey(lambda a,b: a+b)
                  .takeOrdered(5, key=lambda x: (-x[1], x[0]))]
            if df_t5 != rdd_t5:
                errors.append(f"RDD<->DF top5 mismatch: {df_t5} vs {rdd_t5}")
            else:
                print(f"[validate] RDD<->DF top5 OK: {df_t5}")

            cs = (ctl.filter(F.col("event_type") == "search")
                     .filter(F.col("query").isNotNull())
                     .withColumn("day", F.to_date(F.col("event_time")))
                     .withColumn("q", F.lower(F.col("query"))))

            df_w = [(str(r.day), r.word, int(r.cnt)) for r in
                cs.select("day", F.explode(F.split("q", r"\s+")).alias("word"))
                  .filter(F.length("word") > 4)
                  .groupBy("day","word").agg(F.count("*").alias("cnt"))
                  .orderBy("day","word").collect()]

            def emit(r): return [(str(r.day), wd) for wd in r.q.split() if len(wd) > 4]
            rdd_w = (cs.select("day","q").rdd.flatMap(emit)
                       .map(lambda kv: (kv, 1)).reduceByKey(lambda a,b: a+b)
                       .map(lambda kv: (kv[0][0], kv[0][1], int(kv[1])))
                       .sortBy(lambda x: (x[0], x[1])).collect())

            if df_w != rdd_w:
                errors.append(f"RDD<->DF words mismatch: {len(df_w)} vs {len(rdd_w)}")
            else:
                print(f"[validate] RDD<->DF words OK: {len(df_w)} entries")
    except Exception as e:
        errors.append(f"control_set check failed: {e}")

    if errors:
        print("[validate] FAILED:")
        for e in errors: print("  !", e)
        sys.exit(1)
    print("[validate] OK")

def stage_publish(spark, run_id, interval_start, interval_end):
    client = ch_client()
    for t in ["batch_views_per_minute","batch_views_by_country_tier",
              "batch_top5_content_daily","batch_search_word_freq"]:
        client.command(f"ALTER TABLE {t} DELETE WHERE run_id = '{run_id}'")

    rows = [(r.minute, int(r.content_id), int(r.views_count), int(r.total_seconds),
             run_id, interval_start, interval_end)
            for r in spark.read.parquet(work(run_id, "mart_minute")).collect()]
    if rows: client.insert("batch_views_per_minute", rows,
        column_names=["minute","content_id","views_count","total_seconds",
                      "run_id","calc_interval_start","calc_interval_end"])
    print(f"[publish] minute={len(rows)}")

    rows = [(r.day, r.country, r.tier, int(r.views_count), int(r.total_seconds),
             run_id, interval_start, interval_end)
            for r in spark.read.parquet(work(run_id, "mart_daily")).collect()]
    if rows: client.insert("batch_views_by_country_tier", rows,
        column_names=["day","country","subscription_tier","views_count","total_seconds",
                      "run_id","calc_interval_start","calc_interval_end"])
    print(f"[publish] daily={len(rows)}")

    rows = [(r.day, int(r.rank), int(r.content_id), int(r.views_count),
             run_id, interval_start, interval_end)
            for r in spark.read.parquet(work(run_id, "mart_top5")).collect()]
    if rows: client.insert("batch_top5_content_daily", rows,
        column_names=["day","rank","content_id","views_count",
                      "run_id","calc_interval_start","calc_interval_end"])
    print(f"[publish] top5={len(rows)}")

    rows = [(r.day, r.word, int(r.freq), run_id, interval_start, interval_end)
            for r in spark.read.parquet(work(run_id, "mart_words")).collect()]
    if rows: client.insert("batch_search_word_freq", rows,
        column_names=["day","word","freq","run_id","calc_interval_start","calc_interval_end"])
    print(f"[publish] words={len(rows)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True,
                    choices=["prepare","scd2","marts","validate","publish"])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--interval-start", default="1970-01-01T00:00:00Z")
    ap.add_argument("--interval-end",   default="2105-01-01T00:00:00Z")
    args = ap.parse_args()
    start = datetime.fromisoformat(args.interval_start.replace("Z","+00:00"))
    end   = datetime.fromisoformat(args.interval_end.replace("Z","+00:00"))
    spark = make_spark()
    try:
        if args.stage=="prepare":   stage_prepare(spark, args.run_id)
        elif args.stage=="scd2":    stage_scd2(spark, args.run_id)
        elif args.stage=="marts":   stage_marts(spark, args.run_id, start, end)
        elif args.stage=="validate":stage_validate(spark, args.run_id)
        elif args.stage=="publish": stage_publish(spark, args.run_id, start, end)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
