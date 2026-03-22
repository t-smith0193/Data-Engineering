from datetime import datetime
import os
import socket

import boto3
import psutil

from airflow import DAG
from airflow.operators.python import PythonOperator


SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")


def send_system_health():
    hostname = socket.gethostname()

    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent
    disk_percent = psutil.disk_usage("/").percent

    issues = []

    if cpu_percent >= 85:
        issues.append(f"High CPU: {cpu_percent}%")
    if memory_percent >= 85:
        issues.append(f"High memory: {memory_percent}%")
    if disk_percent >= 85:
        issues.append(f"High disk: {disk_percent}%")

    status = "healthy" if not issues else "warning"
    icon = "🟢" if status == "healthy" else "🟡"

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

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

    sns = boto3.client("sns", region_name=AWS_REGION)
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Airflow System Health Heartbeat",
        Message=message,
    )

    print(message)


with DAG(
    dag_id="system_health_heartbeat",
    start_date=datetime(2026, 3, 22),
    schedule="@hourly",
    catchup=False,
    tags=["monitoring", "health"],
) as dag:

    send_heartbeat = PythonOperator(
        task_id="send_system_health",
        python_callable=send_system_health,
    )
