from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (SparkSession.builder
        .appName("verify_hw23")
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
