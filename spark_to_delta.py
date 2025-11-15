#!/usr/bin/env python3
# spark_table_to_delta.py
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

parent_dir = Path(__file__).resolve().parent.parent
env_path = parent_dir / ".env"
load_dotenv(env_path)

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
CONTAINER = os.getenv("CONTAINER")
if not STORAGE_ACCOUNT or not CONTAINER:
    raise SystemExit("STORAGE_ACCOUNT and CONTAINER must be set in .env")

TOPIC = os.getenv("TABLE_TOPIC", "table-batches")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DELTA_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta/table_batches"
CHECKPOINT = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/table_batches_spark"

# Schema of incoming JSON
schema = StructType([
    StructField("batch_id", StringType()),
    StructField("run_id", IntegerType()),
    StructField("table_id", IntegerType()),
    StructField("table_name", StringType()),
    StructField("table_timestamp", StringType()),
    StructField("load_timestamp", StringType()),
    StructField("process_stage", StringType()),
    StructField("process_status", StringType()),
    StructField("error_message", StringType()),
    StructField("source_record_count", IntegerType()),
    StructField("batch_start_timestamp", StringType()),
    StructField("batch_end_timestamp", StringType()),
])

spark = (
    SparkSession.builder
    .appName("table_to_delta")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "org.apache.kafka:kafka-clients:2.8.1,"
            "io.delta:delta-core_2.12:2.3.0,"
            "org.apache.hadoop:hadoop-azure:3.3.2,"
            "com.microsoft.azure:azure-storage:8.6.6")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# If ADLS OAuth is needed, set same configs as other jobs (AZ_TENANT_ID/AZ_*)
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
if STORAGE_ACCOUNT:
    spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", os.getenv("AZ_CLIENT_ID"))
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", os.getenv("AZ_CLIENT_SECRET"))
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{os.getenv('AZ_TENANT_ID')}/oauth2/v2.0/token")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP"))
    .option("subscribe", os.getenv("TABLE_TOPIC", "table-batches"))
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", os.getenv("TRUSTSTORE_PATH"))
    .option("kafka.ssl.truststore.password", os.getenv("TRUSTSTORE_PASS"))
    .option("kafka.ssl.keystore.location", os.getenv("KEYSTORE_PATH"))
    .option("kafka.ssl.keystore.password", os.getenv("KEYSTORE_PASS"))
    .option("kafka.ssl.key.password", os.getenv("KEYSTORE_PASS"))
    .load()
)


# value is JSON string
parsed = raw.select(from_json(col("value").cast("string"), schema).alias("j")).select("j.*")

# write to delta
query = (parsed.writeStream
         .format("delta")
         .option("checkpointLocation", CHECKPOINT)
         .outputMode("append")
         .trigger(processingTime="15 seconds")
         .start(DELTA_PATH))

print("Started spark_table_to_delta, writing to:", DELTA_PATH)
query.awaitTermination()
