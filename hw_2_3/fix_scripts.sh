# 1. build_user_scd2.py
cat > jobs/build_user_scd2.py << 'EOF'
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
        .getOrCreate())
        
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "hw23student")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "hw23student-secret")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

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
EOF

# 2. build_campaign_recipients.py
cat > jobs/build_campaign_recipients.py << 'EOF'
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--interval-start", required=True)
    parser.add_argument("--interval-end", required=True)
    parser.add_argument("--calculation-date", required=True)
    return parser.parse_args()

def main():
    args = parse_args()
    
    spark = (SparkSession.builder
        .appName("build_campaign_recipients")
        .config("spark.driver.extraClassPath", "/opt/airflow/s3-jars/*")
        .config("spark.executor.extraClassPath", "/opt/airflow/s3-jars/*")
        .getOrCreate())
        
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "hw23student")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "hw23student-secret")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    scd2_path = f"s3a://hw23/results/demo/user_scd2/snapshot_date={args.calculation_date}/"
    scd2 = spark.read.parquet(scd2_path)

    campaigns = (
        spark.read.option("header", True).csv("s3a://hw23/raw/demo/campaigns.csv")
        .withColumn("send_at", F.to_timestamp("send_at"))
        .withColumn("campaign_id", F.col("campaign_id").cast("bigint"))
        .filter((F.col("send_at") >= F.lit(args.interval_start)) &
                (F.col("send_at") <  F.lit(args.interval_end)))
    )

    scd2 = scd2.withColumn("email_domain_lower", F.lower(F.regexp_extract("user_email", "@(.+)$", 1)))
    campaigns = campaigns.withColumn("email_domain_lower", F.lower("email_domain"))

    joined = campaigns.join(
        scd2,
        on=[
            campaigns["email_domain_lower"] == scd2["email_domain_lower"],
            scd2["valid_from"] <= campaigns["send_at"],
            (scd2["valid_to"].isNull()) | (campaigns["send_at"] < scd2["valid_to"]),
        ],
        how="inner",
    )

    loaded_at = F.current_timestamp().cast("timestamp")
    result = joined.select(
        campaigns["campaign_id"],
        campaigns["campaign_name"],
        campaigns["send_at"],
        campaigns["email_domain"],
        scd2["user_id"],
        scd2["user_fio"],
        scd2["user_email"],
        scd2["user_login"],
        scd2["valid_from"].alias("user_valid_from"),
        scd2["valid_to"].alias("user_valid_to"),
        F.lit(args.calculation_date).cast("date").alias("calculation_date"),
        F.lit(args.dag_id).alias("_dag_id"),
        F.lit(args.run_id).alias("_run_id"),
        F.lit(args.task_id).alias("_task_id"),
        F.lit(args.interval_start).cast("timestamp").alias("_data_interval_start"),
        F.lit(args.interval_end).cast("timestamp").alias("_data_interval_end"),
        loaded_at.alias("_loaded_at"),
    )

    result = result.dropDuplicates(["campaign_id", "user_id"])
    out_path = f"s3a://hw23/results/demo/campaign_recipients/calculation_date={args.calculation_date}/"
    result.write.mode("overwrite").parquet(out_path)
    print(f"Recipients rows written: {result.count()} -> {out_path}")

if __name__ == "__main__":
    main()
EOF

# 3. run_checks.py
cat > checks/run_checks.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (SparkSession.builder
        .appName("verify_hw23")
        .config("spark.driver.extraClassPath", "/opt/airflow/s3-jars/*")
        .config("spark.executor.extraClassPath", "/opt/airflow/s3-jars/*")
        .getOrCreate())
        
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "hw23student")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "hw23student-secret")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    dates = ["2026-01-23", "2026-01-24", "2026-01-25"]

    print("=" * 50)
    print("ПРОВЕРКА SCD2")
    print("=" * 50)
    for d in dates:
        try:
            df = spark.read.parquet(f"s3a://hw23/results/demo/user_scd2/snapshot_date={d}/")
            rows = df.count()
            users = df.select("user_id").distinct().count()
            cur = df.filter("is_current = true").groupBy("user_id").count().filter("count != 1").count()
            dup = df.groupBy("user_id", "valid_from").count().filter("count > 1").count()
            null_meta = df.filter(F.col("_dag_id").isNull() | F.col("_run_id").isNull() | F.col("_task_id").isNull() | F.col("_loaded_at").isNull()).count()
            print(f"[{d}] Rows: {rows}, Users: {users}, BadCurrent: {cur}, DupKeys: {dup}, NullMeta: {null_meta}")
        except Exception as e:
            print(f"[{d}] Ошибка: {str(e).splitlines()[0]}")

    print("\n" + "=" * 50)
    print("ПРОВЕРКА ВИТРИНЫ")
    print("=" * 50)
    for d in dates:
        try:
            df = spark.read.parquet(f"s3a://hw23/results/demo/campaign_recipients/calculation_date={d}/")
            rows = df.count()
            dup = df.groupBy("campaign_id", "user_id").count().filter("count > 1").count()
            bad_logic = df.filter(
                (F.lower(F.regexp_extract("user_email", "@(.+)$", 1)) != F.lower("email_domain")) |
                (F.col("send_at") < F.col("user_valid_from")) |
                (F.col("user_valid_to").isNotNull() & (F.col("send_at") >= F.col("user_valid_to")))
            ).count()
            print(f"[{d}] Rows: {rows}, DupCampUser: {dup}, BadLogic: {bad_logic}")
            if rows > 0:
                df.groupBy("campaign_id").count().orderBy("campaign_id").show()
        except Exception as e:
            print(f"[{d}] Ошибка: {str(e).splitlines()[0]}")

if __name__ == "__main__":
    main()
EOF

echo "Скрипты обновлены (добавлен extraClassPath)!"
