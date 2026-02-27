import os
import sys
import json
import time
import hashlib
import smtplib
from typing import Dict, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing env var: {name}")
    return str(v)


def state_key(bucket: str) -> str:
    return hashlib.sha256(bucket.encode()).hexdigest()[:16]


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def should_notify(last_sent: Optional[float], cooldown: int) -> bool:
    if last_sent is None:
        return True
    return (time.time() - last_sent) >= cooldown * 60


def send_mail(subject: str, body: str):
    smtp_host = env("SMTP_HOST")
    smtp_port = int(env("SMTP_PORT", "587", required=False))
    smtp_user = env("SMTP_USERNAME", "", required=False)
    smtp_pass = env("SMTP_PASSWORD", "", required=False)
    mail_from = env("MAIL_FROM")
    mail_to = env("MAIL_TO")
    use_tls = env("SMTP_USE_TLS", "true", required=False).lower() == "true"

    msg = MIMEMultipart()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        if use_tls:
            server.starttls()
        if smtp_user:
            server.login(smtp_user, smtp_pass)
        server.sendmail(mail_from, [mail_to], msg.as_string())


def main():
    threshold = float(env("S3_THRESHOLD_PERCENT", "80", required=False))
    cooldown = int(env("ALARM_COOLDOWN_MINUTES", "30", required=False))
    state_path = env("ALARM_STATE_PATH", ".alarm_state/state.json", required=False)

    # Ansible JSON input
    data = json.load(sys.stdin)

    bucket_name = data.get("bucket_name")
    used_bytes = float(data.get("used_bytes", 0))
    total_bytes = float(data.get("total_bytes", 0))

    if total_bytes <= 0:
        print("Total capacity is zero, skipping.")
        return 0

    percent = (used_bytes / total_bytes) * 100

    st = load_state(state_path)
    key = state_key(bucket_name)
    last_sent = st.get(key, {}).get("last_sent_ts")

    print(f"Bucket={bucket_name} Used={percent:.2f}% Threshold={threshold}%")

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
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
