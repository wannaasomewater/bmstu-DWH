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
        .config("spark.hadoop.fs.s3a.list.version", "1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000")
        .config("spark.hadoop.fs.s3a.access.key", "hw23student")
        .config("spark.hadoop.fs.s3a.secret.key", "hw23student-secret")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())

    spark.conf.set("spark.sql.session.timeZone", "UTC")

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
