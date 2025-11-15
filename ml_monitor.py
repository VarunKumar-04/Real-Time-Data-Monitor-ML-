#!/usr/bin/env python3
# ml_monitor.py - unified anomaly detection (rules + IsolationForest)
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.ensemble import IsolationForest

from email_utils import send_anomaly_email

# -----------------------
# Load env
# -----------------------
parent_dir = Path(__file__).resolve().parent.parent
env_path = parent_dir / ".env"
load_dotenv(env_path)

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
CONTAINER = os.getenv("CONTAINER")
BATCH_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta/table_batches"
ANOMALY_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta/table_anomalies"
CHECKPOINT = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/ml_monitor"

WINDOW_N = int(os.getenv("ML_WINDOW_N", "10"))
Z_THRESH = float(os.getenv("ML_Z_THRESH", "2.5"))

ENABLE_RULES = os.getenv("ENABLE_RULES", "true").lower() == "true"
ENABLE_ISOFOREST = os.getenv("ENABLE_ISOFOREST", "false").lower() == "true"

# -----------------------
# Spark session
# -----------------------
spark = (
    SparkSession.builder
    .appName("ml_monitor")
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

try:
    stream = spark.readStream.format("delta").load(BATCH_PATH)
except Exception as e:
    print("⚠️ Batches path may not be initialized. Run spark_table_to_delta.py first.")
    raise


# Rule-based detector
def rule_based(pdf, batch_id):
    anomalies = []

    arr = pdf["source_record_count"].astype(float).values
    if arr.size > 0:
        arr = arr[-WINDOW_N:] if arr.size > WINDOW_N else arr
        mean = float(arr.mean())
        std = float(arr.std(ddof=0)) if arr.std(ddof=0) > 0 else 1.0
        latest = float(arr[-1])
        z = (latest - mean) / std

        if abs(z) >= Z_THRESH:
            print(f"🚨 [RULE] ZSCORE anomaly batch {batch_id}: latest={latest}, mean={mean:.2f}, z={z:.2f}")
            pdf["anomaly_type"] = "zscore"
            pdf["zscore"] = z
            anomalies.append(pdf.tail(1))  # last row only

    failed_rows = pdf[pdf["process_status"] == "failed"].copy()
    if not failed_rows.empty:
        print(f"🚨 [RULE] {len(failed_rows)} failed loads in batch {batch_id}")
        failed_rows["anomaly_type"] = "failed"
        failed_rows["zscore"] = None
        anomalies.append(failed_rows)

    return anomalies

# Isolation Forest detector
def iso_forest(pdf, batch_id):
    anomalies = []
    if pdf.empty:
        return anomalies
    try:
        X = pdf[["source_record_count"]].astype(float).values
        model = IsolationForest(n_estimators=50, contamination=0.1, random_state=42)
        preds = model.fit_predict(X)
        pdf["iso_pred"] = preds
        iso_anoms = pdf[pdf["iso_pred"] == -1].copy()
        if not iso_anoms.empty:
            print(f"🚨 [ISOFOREST] {len(iso_anoms)} anomalies in batch {batch_id}")
            iso_anoms["anomaly_type"] = "iso_forest"
            iso_anoms["zscore"] = None
            anomalies.append(iso_anoms)
    except Exception as e:
        print(f"[ISOFOREST] error: {e}")
    return anomalies

# Batch processor
def process_batch(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            print(f"[ml_monitor] batch {batch_id} empty")
            return
        pdf = batch_df.select(
            "batch_id", "process_status", "source_record_count", "table_name", "load_timestamp"
        ).orderBy(col("batch_id").asc()).toPandas()
        all_anoms = []
        if ENABLE_RULES:
            all_anoms.extend(rule_based(pdf, batch_id))
        if ENABLE_ISOFOREST:
            all_anoms.extend(iso_forest(pdf, batch_id))
        if all_anoms:
            combined = pd.concat(all_anoms, ignore_index=True)
            if "zscore" in combined.columns:
                combined["zscore"] = pd.to_numeric(combined["zscore"], errors="coerce").fillna(-9999.0)
            if "batch_id" in combined.columns:
                combined["batch_id"] = combined["batch_id"].astype(str)   # always string
            if "anomaly_type" in combined.columns:
                combined["anomaly_type"] = combined["anomaly_type"].astype(str).fillna("unknown")
            if "process_status" in combined.columns:
                combined["process_status"] = combined["process_status"].astype(str).fillna("unknown")
            if "table_name" in combined.columns:
                combined["table_name"] = combined["table_name"].astype(str).fillna("unknown")
            if "load_timestamp" in combined.columns:
                combined["load_timestamp"] = combined["load_timestamp"].astype(str).fillna("")
            
            rows = combined.to_dict(orient="records")
            if rows:
                spark.createDataFrame(rows).write.format("delta") \
                    .mode("append").option("mergeSchema", "true").save(ANOMALY_PATH)
            send_anomaly_email(combined, subject="🚨 Table Batch Anomalies Detected")
    except Exception as e:
        print("[ml_monitor] process_batch error:", e)
        
query = (stream.writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="30 seconds")  # every 2 minutes
         .start())

print("✅ Started ml_monitor with RULES={} ISOFOREST={}".format(ENABLE_RULES, ENABLE_ISOFOREST))
query.awaitTermination()
