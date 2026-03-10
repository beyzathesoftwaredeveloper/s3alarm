import os
import sys
import json
import time
import hashlib
import smtplib
from typing import Dict, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import boto3


def env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(f"Missing env var: {name}")
    return str(value)


def state_key(bucket: str) -> str:
    return hashlib.sha256(bucket.encode()).hexdigest()[:16]


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(path: str, state: Dict[str, Any]) -> None:
    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def should_notify(last_sent: Optional[float], cooldown: int) -> bool:
    if last_sent is None:
        return True
    return (time.time() - last_sent) >= cooldown * 60


def send_mail(subject: str, body: str) -> None:
    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587", required=False))
    smtp_user = env("SMTP_USERNAME", "", required=False)
    smtp_pass = env("SMTP_PASSWORD", "", required=False)
    mail_from = env("MAIL_FROM")
    mail_to = env("MAIL_TO")
    use_tls = env("SMTP_USE_TLS", "true", required=False).lower() == "true"

    recipients = [x.strip() for x in mail_to.split(",") if x.strip()]
    if not recipients:
        raise RuntimeError("MAIL_TO is empty")

    msg = MIMEMultipart()
    msg["From"] = mail_from
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        if use_tls:
            server.starttls()
        if smtp_user:
            server.login(smtp_user, smtp_pass)
        server.sendmail(mail_from, recipients, msg.as_string())


def get_bucket_usage() -> Dict[str, Any]:
    bucket_name = env("S3_BUCKET_NAME")
    endpoint_url = env("S3_ENDPOINT_URL")
    access_key = env("S3_ACCESS_KEY")
    secret_key = env("S3_SECRET_KEY")
    region = env("AWS_REGION", "us-east-1", required=False)
    total_bytes = float(env("S3_BUCKET_QUOTA_BYTES"))

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    used_bytes = 0.0
    object_count = 0

    for page in pages:
        for obj in page.get("Contents", []):
            used_bytes += float(obj.get("Size", 0))
            object_count += 1

    return {
        "bucket_name": bucket_name,
        "used_bytes": used_bytes,
        "total_bytes": total_bytes,
        "object_count": object_count,
    }


def main() -> int:
    threshold = float(env("S3_THRESHOLD_PERCENT", "80", required=False))
    cooldown = int(env("ALARM_COOLDOWN_MINUTES", "30", required=False))
    state_path = env("ALARM_STATE_PATH", ".alarm_state/state.json", required=False)

    usage = get_bucket_usage()

    bucket_name = usage["bucket_name"]
    used_bytes = usage["used_bytes"]
    total_bytes = usage["total_bytes"]
    object_count = usage["object_count"]

    if total_bytes <= 0:
        print("Total capacity is zero, skipping.")
        return 0

    percent = (used_bytes / total_bytes) * 100

    st = load_state(state_path)
    key = state_key(bucket_name)
    last_sent = st.get(key, {}).get("last_sent_ts")

    print(
        json.dumps(
            {
                "bucket_name": bucket_name,
                "used_bytes": used_bytes,
                "total_bytes": total_bytes,
                "object_count": object_count,
                "percent": round(percent, 2),
                "threshold": threshold,
            }
        )
    )

    if percent >= threshold:
        if should_notify(last_sent, cooldown):
            subject = f"ALARM: {bucket_name} utilization {percent:.2f}%"
            body = (
                f"S3 Bucket Alarm\n\n"
                f"Bucket: {bucket_name}\n"
                f"Used: {percent:.2f}%\n"
                f"Threshold: {threshold}%\n"
                f"Used bytes: {used_bytes}\n"
                f"Total bytes: {total_bytes}\n"
                f"Object count: {object_count}\n"
            )
            send_mail(subject, body)
            st[key] = {"last_sent_ts": time.time()}
            save_state(state_path, st)
            print("Alarm email sent.")
        else:
            print("Cooldown active, not sending.")
    else:
        print("OK")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
