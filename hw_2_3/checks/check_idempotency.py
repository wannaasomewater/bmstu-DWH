import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) < 3:
        print("Usage: python check_idempotency.py <old_path> <new_path>", flush=True)
        sys.exit(1)
    
    old_path = sys.argv[1]
    new_path = sys.argv[2]
    print(f"Проверка путей:\nСтарый: {old_path}\nНовый: {new_path}", flush=True)

    try:
        spark = (SparkSession.builder
            .appName("check_idempotency")
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

        print("Spark инициализирован.", flush=True)

        df_old = spark.read.parquet(old_path)
        df_new = spark.read.parquet(new_path)

        print(f"Строк в старом: {df_old.count()}, в новом: {df_new.count()}", flush=True)

        meta_cols = ["_dag_id", "_run_id", "_task_id", "_data_interval_start", "_data_interval_end", "_loaded_at"]
        business_cols = [c for c in df_old.columns if c not in meta_cols]

        diff_old_new = df_old.select(business_cols).exceptAll(df_new.select(business_cols)).count()
        diff_new_old = df_new.select(business_cols).exceptAll(df_old.select(business_cols)).count()

        print(f"Разница (old - new): {diff_old_new}", flush=True)
        print(f"Разница (new - old): {diff_new_old}", flush=True)

        if diff_old_new == 0 and diff_new_old == 0:
            print("РЕЗУЛЬТАТ: Идемпотентность подтверждена. Бизнес-данные идентичны.", flush=True)
        else:
            print("РЕЗУЛЬТАТ: ОШИБКА. Данные отличаются!", flush=True)

    except Exception as e:
        print(f"ОШИБКА ПРИ ВЫПОЛНЕНИИ: {e}", flush=True)

if __name__ == "__main__":
    main()
