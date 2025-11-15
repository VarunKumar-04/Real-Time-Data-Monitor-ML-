import os
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

EMAIL_MODE = os.getenv("EMAIL_MODE", "hourly").lower()
_last_sent = None   # track last send time (for hourly mode)

def send_anomaly_email(rows, subject="🚨 Table Batch Anomalies Detected"):
    """Send anomalies as email with CSV attachment (safe for Pandas >=2.0)"""
    global _last_sent
    if rows is None:
        return
    if isinstance(rows, pd.DataFrame):
        df = rows.copy()
    elif hasattr(rows, "toPandas"):  # PySpark DataFrame
        df = rows.toPandas()
    elif isinstance(rows, (list, tuple)):
        try:
            df = pd.DataFrame([
                r.asDict() if hasattr(r, "asDict") else dict(r)
                for r in rows
            ])
        except Exception:
            df = pd.DataFrame(rows)
    else:
        print(f"⚠️ Unsupported anomaly rows type: {type(rows)}")
        return
    if df.empty:
        print("⚠️ No anomalies to send")
        return
    if EMAIL_MODE == "hourly":
        now = datetime.utcnow()
        if _last_sent and (now - _last_sent) < timedelta(hours=1):
            print("⏳ Hourly mode active, skipping email until 1 hour passes")
            return
        _last_sent = now
        subject = f"🚨 Hourly Anomaly Report ({now.strftime('%Y-%m-%d %H:%M UTC')})"

    # Save anomalies to CSV
    csv_file = "/tmp/anomalies.csv"
    df.to_csv(csv_file, index=False)
    sender = os.getenv("SMTP_USER")
    recipients = os.getenv("ALERT_RECIPIENTS", sender)
    password = os.getenv("SMTP_PASS")
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipients
    msg["Subject"] = subject
    body = (
        f"🚨 Anomalies detected in table batch ingestion.\n"
        f"Total anomalies: {len(df)}\n"
        f"See attached CSV for details."
    )
    msg.attach(MIMEText(body, "plain"))
    with open(csv_file, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename=anomalies.csv")
        msg.attach(part)
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients.split(","), msg.as_string())
    print(f"📧 Anomaly email sent to {recipients} (mode={EMAIL_MODE})")