from datetime import datetime
import os
import socket

import boto3
import psutil

from airflow import DAG
from airflow.operators.python import PythonOperator


# SNS topic used to send heartbeat notifications.
# Falls back to placeholder to prevent accidental silent misconfiguration.
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "REPLACE_ME")

# AWS region for the SNS client.
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")


def send_system_health():
    """
    Collect basic system health metrics and publish a heartbeat message to SNS.

    This provides lightweight monitoring of the Airflow worker/node by reporting:
      - CPU usage
      - Memory usage
      - Disk usage

    If any metric crosses a threshold, the message is flagged as a warning.
    """
    hostname = socket.gethostname()

    # Collect system resource usage.
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent
    disk_percent = psutil.disk_usage("/").percent

    issues = []

    # Simple threshold-based checks for potential system pressure.
    if cpu_percent >= 85:
        issues.append(f"High CPU: {cpu_percent}%")
    if memory_percent >= 85:
        issues.append(f"High memory: {memory_percent}%")
    if disk_percent >= 85:
        issues.append(f"High disk: {disk_percent}%")

    # Overall status is healthy unless at least one issue is detected.
    status = "healthy" if not issues else "warning"
    icon = "🟢" if status == "healthy" else "🟡"

    # Use UTC for consistent, timezone-independent monitoring logs.
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Build a human-readable heartbeat message for SNS/email/alerts.
    message = (
        f"{icon} SYSTEM HEALTH HEARTBEAT\n"
        f"host: {hostname}\n"
        f"status: {status}\n"
        f"cpu_percent: {cpu_percent}\n"
        f"memory_percent: {memory_percent}\n"
        f"disk_root_percent: {disk_percent}\n"
        f"issues: {', '.join(issues) if issues else 'none'}\n"
        f"timestamp: {timestamp}"
    )

    # Fail fast if SNS is not configured to avoid silent monitoring gaps.
    if SNS_TOPIC_ARN == "REPLACE_ME":
        raise ValueError("SNS_TOPIC_ARN is not set.")

    # Publish heartbeat to SNS so it can fan out to email, Slack, etc.
    sns = boto3.client("sns", region_name=AWS_REGION)
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Airflow System Health Heartbeat",
        Message=message,
    )

    # Also print locally for visibility in Airflow logs.
    print(message)


with DAG(
    dag_id="system_health_heartbeat",
    start_date=datetime(2026, 3, 22),
    schedule="@hourly",  # Run hourly to continuously monitor system health
    catchup=False,
    tags=["monitoring", "health"],
) as dag:

    # Single-task DAG that sends a periodic system health heartbeat.
    send_heartbeat = PythonOperator(
        task_id="send_system_health",
        python_callable=send_system_health,
    )