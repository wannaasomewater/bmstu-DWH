import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--interval-start", required=True)
    parser.add_argument("--interval-end", required=True)
    parser.add_argument("--snapshot-date", required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    spark = (SparkSession.builder
        .appName("build_user_scd2")
        .config("spark.driver.extraClassPath", "/opt/airflow/s3-jars/*")
        .config("spark.executor.extraClassPath", "/opt/airflow/s3-jars/*")
        .config("spark.hadoop.fs.s3a.list.version", "1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
        .config("spark.hadoop.fs.s3a.access.key", "hw23student")
        .config("spark.hadoop.fs.s3a.secret.key", "hw23student-secret")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    events = (
        spark.read.option("header", True).csv("s3a://hw23/raw/demo/events/*.csv")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("user_id", F.col("user_id").cast("bigint"))
        .filter(F.col("event_time") < F.lit(args.interval_end))
    )

    w = Window.partitionBy("user_id").orderBy("event_time")
    events = events.withColumn("prev_fio", F.lag("user_fio").over(w)) \
                   .withColumn("prev_email", F.lag("user_email").over(w)) \
                   .withColumn("prev_login", F.lag("user_login").over(w))

    changed = (
        F.col("prev_fio").isNull()
        | (F.col("user_fio") != F.col("prev_fio"))
        | (F.col("user_email") != F.col("prev_email"))
        | (F.col("user_login") != F.col("prev_login"))
    )

    versions = events.filter(changed).select(
        "user_id", "user_fio", "user_email", "user_login",
        F.col("event_time").alias("valid_from")
    )

    wv = Window.partitionBy("user_id").orderBy("valid_from")
    scd2 = versions.withColumn("valid_to", F.lead("valid_from").over(wv)) \
                   .withColumn("is_current", F.col("valid_to").isNull())

    loaded_at = F.current_timestamp().cast("timestamp")
    scd2 = scd2.select(
        "user_id", "user_fio", "user_email", "user_login",
        "valid_from", "valid_to", "is_current",
        F.lit(args.snapshot_date).cast("date").alias("snapshot_date"),
        F.lit(args.dag_id).alias("_dag_id"),
        F.lit(args.run_id).alias("_run_id"),
        F.lit(args.task_id).alias("_task_id"),
        F.lit(args.interval_start).cast("timestamp").alias("_data_interval_start"),
        F.lit(args.interval_end).cast("timestamp").alias("_data_interval_end"),
        loaded_at.alias("_loaded_at"),
    )

    out_path = f"s3a://hw23/results/demo/user_scd2/snapshot_date={args.snapshot_date}/"
    scd2.write.mode("overwrite").parquet(out_path)
    print(f"SCD2 rows written: {scd2.count()} -> {out_path}")

if __name__ == "__main__":
    main()
