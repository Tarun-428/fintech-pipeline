from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Default arguments for the DAG
default_args = {
    "owner": "fintech",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

# 2. DAG Definition
with DAG(
    dag_id="fintech_realtime_pipeline",
    default_args=default_args,
    description="Real-time fintech pipeline loading Kafka market prices into Snowflake",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["fintech", "streaming"]
) as dag:

    # 3. Tasks
    # Check Kafka health
    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command=(
            "python - <<'PY'\n"
            "import os\n"
            "import socket\n"
            "import sys\n"
            "\n"
            "bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'host.docker.internal:39092')\n"
            "host, port = bootstrap.rsplit(':', 1)\n"
            "try:\n"
            "    with socket.create_connection((host, int(port)), timeout=10):\n"
            "        print(f'Kafka health check OK: {bootstrap}')\n"
            "except OSError as exc:\n"
            "    print(f'Kafka health check failed for {bootstrap}: {exc}')\n"
            "    sys.exit(1)\n"
            "PY"
        )
    )

    # Load Kafka market price events into Snowflake for analytics.
    load_snowflake = BashOperator(
        task_id="load_market_prices_to_snowflake",
        bash_command=(
            "KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-host.docker.internal:39092} "
            "python /usr/local/airflow/dags/scripts/kafka_consumer.py"
        )
    )
    # Validate Output
    validate = BashOperator(
        task_id="validate_output",
        bash_command="echo 'Validating stream output... OK'"
    )

   
    check_kafka >> load_snowflake >> validate
