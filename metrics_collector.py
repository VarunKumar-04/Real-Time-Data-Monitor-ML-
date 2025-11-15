#!/usr/bin/env python3
import os, time, json
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

parent_dir = Path(__file__).resolve().parent.parent
env_path = parent_dir / ".env"
load_dotenv(env_path)

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
CONTAINER = os.getenv("CONTAINER")
METRICS_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta/system_metrics"

spark = (
    SparkSession.builder
    .appName("ml_table_monitor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
    .config(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    .config(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", os.getenv("AZ_CLIENT_ID"))
    .config(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", os.getenv("AZ_CLIENT_SECRET"))
    .config(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{os.getenv('AZ_TENANT_ID')}/oauth2/v2.0/token")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def record_metric(metric_name, value):
    df = spark.createDataFrame(
        [(metric_name, value, )],
        ["metric_name", "metric_value"]
    ).withColumn("timestamp", current_timestamp())
    df.write.format("delta").mode("append").save(METRICS_PATH)

if __name__ == "__main__":
    print("📊 Starting metrics collector...")
    while True:
        # In real scenario, hook into Kafka lag or Spark listener
        record_metric("mock_batch_latency", round(time.time() % 10, 2))
        record_metric("mock_throughput", round(time.time() % 100, 2))
        print("✅ Metrics recorded")
        time.sleep(30)
