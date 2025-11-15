# Project Overview

This project demonstrates a **real-time data streaming pipeline** designed for:
- Ingesting continuously generated data
- Processing via **Apache Spark**
- Storing results using **Delta Lake**
- Monitoring with **ML-based anomaly detection models**
- End-to-end automation: data ingestion, monitoring, alerting (email), and SSL-secured Kafka communication

> The repository is structured as a production-grade system, suitable for showcasing to recruiters and hiring managers.

---

## 🧩 Key Features

- **Kafka Producer & Consumer:** Live data streaming
- **Spark Structured Streaming:** Real-time ETL
- **Delta Lake:** Reliability & ACID transactions
- **ML-Based Anomaly Detection:** On streaming tables
- **Automated Email Alerts:** On anomalies
- **Monitoring Scripts:** Track drift, schema changes, and metric degradation
- **SSL-Secured Communication:** Enterprise-level integration

---

**Pipeline Flow:**
```
Kafka Producer → Kafka Topic → Spark Structured Streaming → Delta Lake Tables → ML Monitoring → Alerts (Email)
```

---

## 🔄 End-to-End Workflow Explanation

### 1️⃣ Kafka Producer (Data Generation)
- **`producer.py`:**
  - Simulates or retrieves real-time data
  - Serializes each record to JSON and pushes to Kafka topic
  - Uses SSL certificates for secure message transport
  - Technologies: `kafka-python`, SSL encryption

### 2️⃣ Kafka Consumer (Optional Testing)
- **`consumer.py`:**
  - Tests Kafka stream by reading messages directly from the topic

### 3️⃣ Spark Structured Streaming → Delta Lake
- **`spark_table_to_delta.py`:**
  - Reads Kafka streams in micro-batches
  - Parses JSON, applies schema validation
  - Writes to Delta Lake with checkpointing
  - Ensures **exactly-once** processing

- **Benefits:**
  - ACID transactions
  - Time Travel (Delta Tables)
  - Scalable, large-scale ingestion

### 4️⃣ ⭐ ML Monitoring & Anomaly Detection
Two monitoring scripts:
- **`ml_monitor.py`:**
  - ML-based anomaly detection: Isolation Forest, Z-score, etc.
  - Flags spikes, missing patterns, or outliers
  - Generates labeled anomaly logs

- **`ml_table_monitor.py`:**
  - Monitors drift, schema changes, and degradation in metrics

#### **Evaluation Metrics Used**

| Metric                  | Purpose                                                        |
|-------------------------|------------------------------------------------------------------|
| Z-Score Thresholding    | Detects statistical outliers                                     |
| Isolation Forest Scores | Identifies irregular patterns in multidimensional data            |
| Moving Average Deviation| Detects drifts in streaming data                                 |
| Data Drift %            | Measures deviation in distribution                               |
| Concept Drift Check     | Ensures model remains valid over time                            |

### 5️⃣ Email Alerts (Automated Notification)
- **`email_utils.py`:**
  - Sends automated email alerts on anomaly detection
  - Includes summary of anomaly type, timestamp, and severity
  - Uses SMTP & environment variables stored in `.env`

---

## ✔️ Evaluation Metrics Explained (Deep Technical)

### 🔍 1. Statistical Metrics

- **Z-Score**
  - How many standard deviations a data point is from the mean
  - Points with \|z\| > threshold are considered anomalies
  - Formula: `Z = (x - μ) / σ`

- **IQR Outlier Detection**
  - Useful for non-normal distributions
  - Formula:
    - `IQR = Q3 – Q1`
    - `Upper Bound = Q3 + 1.5 × IQR`
    - `Lower Bound = Q1 - 1.5 × IQR`

### 🤖 2. Machine Learning Metrics

- **Isolation Forest**
  - Detects anomalies by isolating points in random trees
  - Efficient for high-dimensional data
  - Outputs anomaly score: `Score < threshold` → anomaly

### 📉 3. Drift Metrics (in `ml_table_monitor.py`)

| Metric                           | Purpose                                           |
|-----------------------------------|---------------------------------------------------|
| Population Stability Index (PSI)  | Detects shifts in feature distribution            |
| Kolmogorov–Smirnov Test           | Measures feature drift statistically              |
| Null Percentage Change            | Checks for sudden increases in missing values     |
| Feature Mean/Std Deviation Drift  | Tracks data consistency                           |
