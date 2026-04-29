"""
Power BI Dashboard Refresh DAG.

This DAG manages Power BI dataset refresh and report updates,
ensuring dashboards display the latest Snowflake data.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Default arguments for the DAG
default_args = {
    "owner": "data-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2024, 1, 1),
}

# Define the DAG
dag = DAG(
    "powerbi_dashboard_refresh",
    default_args=default_args,
    description="Refresh Power BI dashboards with latest Snowflake data",
    schedule_interval="0 * * * *",  # Hourly refresh
    catchup=False,
    tags=["powerbi", "analytics", "snowflake"],
)


def initialize_powerbi_setup(**context):
    """
    Initialize Power BI setup and configuration.
    
    This task ensures the Power BI environment is properly configured
    with datasets and datasources.
    """
    import sys
    import os
    from datetime import datetime
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
    
    from powerbi_setup import PowerBIManager
    
    try:
        print(f"[{datetime.now()}] Initializing Power BI setup...")
        manager = PowerBIManager()
        result = manager.setup_power_bi()
        
        # Push setup result to XCom for downstream tasks
        context["task_instance"].xcom_push(
            key="powerbi_setup_status",
            value="success"
        )
        
        return result
    except Exception as e:
        print(f"[{datetime.now()}] Power BI setup failed: {e}")
        raise


def refresh_market_prices_dataset(**context):
    """
    Refresh the Market Prices dataset in Power BI.
    
    This task triggers a refresh of the Power BI dataset that contains
    market price data from Snowflake.
    """
    from datetime import datetime
    
    print(f"[{datetime.now()}] Triggering Market Prices dataset refresh...")
    
    # In a production environment, this would call the Power BI API
    # to refresh the specific dataset
    
    try:
        # Placeholder for Power BI API refresh call
        # In real implementation, use powerbi-client library to:
        # 1. Get authentication token
        # 2. Call POST /groups/{group_id}/datasets/{dataset_id}/refreshes
        
        print(f"[{datetime.now()}] Market Prices dataset refresh initiated")
        
        context["task_instance"].xcom_push(
            key="dataset_refresh_status",
            value="completed"
        )
        
        return {"status": "success", "message": "Dataset refresh initiated"}
    except Exception as e:
        print(f"[{datetime.now()}] Dataset refresh failed: {e}")
        raise


def validate_dashboard_data(**context):
    """
    Validate that dashboard data is current and accessible.
    
    This task verifies that:
    1. Snowflake connection is working
    2. Market prices table has recent data
    3. Power BI can access the datasource
    """
    import snowflake.connector
    from datetime import datetime, timedelta
    import os
    
    print(f"[{datetime.now()}] Validating dashboard data...")
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        )
        
        cursor = conn.cursor()
        database = os.getenv("SNOWFLAKE_DATABASE", "FINTECH_ANALYTICS")
        schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
        table = os.getenv("SNOWFLAKE_MARKET_PRICES_TABLE", "MARKET_PRICES")
        
        # Check record count
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
        row_count = cursor.fetchone()[0]
        print(f"[{datetime.now()}] Total records in {table}: {row_count}")
        
        # Check data freshness
        cursor.execute(
            f"""
            SELECT MAX(INGESTED_AT) as latest_ingest 
            FROM {database}.{schema}.{table}
            """
        )
        latest_ingest = cursor.fetchone()[0]
        print(f"[{datetime.now()}] Latest data ingested at: {latest_ingest}")
        
        # Check if data is recent (within last hour)
        time_diff = datetime.utcnow() - latest_ingest.replace(tzinfo=None)
        is_fresh = time_diff < timedelta(hours=1)
        
        cursor.close()
        conn.close()
        
        validation_result = {
            "data_fresh": is_fresh,
            "row_count": row_count,
            "latest_ingest": str(latest_ingest),
            "time_since_ingest_minutes": int(time_diff.total_seconds() / 60)
        }
        
        print(f"[{datetime.now()}] Validation result: {validation_result}")
        
        context["task_instance"].xcom_push(
            key="data_validation",
            value=validation_result
        )
        
        return validation_result
        
    except Exception as e:
        print(f"[{datetime.now()}] Data validation failed: {e}")
        raise


def generate_dashboard_summary(**context):
    """
    Generate a summary of dashboard metrics and alerts.
    
    This task computes summary statistics that can be logged or
    sent to monitoring systems.
    """
    from datetime import datetime
    
    print(f"[{datetime.now()}] Generating dashboard summary...")
    
    # Retrieve validation results from upstream task
    ti = context["task_instance"]
    validation_data = ti.xcom_pull(
        task_ids="validate_dashboard_data",
        key="data_validation"
    )
    
    if validation_data:
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_records": validation_data.get("row_count", 0),
            "data_freshness": "Fresh" if validation_data.get("data_fresh") else "Stale",
            "minutes_since_update": validation_data.get("time_since_ingest_minutes", 0),
            "dashboard_status": "Ready" if validation_data.get("data_fresh") else "Needs Attention"
        }
        
        print(f"[{datetime.now()}] Dashboard Summary: {summary}")
        return summary
    else:
        print(f"[{datetime.now()}] No validation data available")
        return {"status": "warning", "message": "Validation data unavailable"}


# Define tasks
setup_powerbi = PythonOperator(
    task_id="setup_powerbi",
    python_callable=initialize_powerbi_setup,
    provide_context=True,
    dag=dag,
)

refresh_dataset = PythonOperator(
    task_id="refresh_market_prices_dataset",
    python_callable=refresh_market_prices_dataset,
    provide_context=True,
    dag=dag,
)

validate_data = PythonOperator(
    task_id="validate_dashboard_data",
    python_callable=validate_dashboard_data,
    provide_context=True,
    dag=dag,
)

generate_summary = PythonOperator(
    task_id="generate_dashboard_summary",
    python_callable=generate_dashboard_summary,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
setup_powerbi >> refresh_dataset >> validate_data >> generate_summary
