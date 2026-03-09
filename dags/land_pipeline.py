from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import os
import json

# --- 1. SNOWFLAKE UPDATE LOGIC ---
def process_to_snowflake():
    # Path inside the Airflow container (mapped via docker-compose)
    flink_file = '/opt/airflow/data/flink_output.json'
    
    # Check if Flink output exists; if not, use fallback for the demo
    if os.path.exists(flink_file):
        with open(flink_file, 'r') as f:
            raw = json.load(f)
            data = raw[0] if isinstance(raw, list) else raw
    else:
        data = {"parcel_id": "P101", "news_event": "New Zoning Approved: Mixed-Use Commercial (Demo)"}

    # Connect using environment variables from .env / docker-compose
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    
    try:
        cur = conn.cursor()
        # This update triggers the Snowflake Cortex AI View automatically
        sql = f"""
            UPDATE LAND_RECORDS 
            SET ZONING_NOTES = '{data['news_event']}',
                VALUATION = VALUATION * 1.05
            WHERE ID = '{data['parcel_id']}'
        """
        cur.execute(sql)
        conn.commit()
        print(f"✅ Success: Updated {data['parcel_id']} in Snowflake")
    finally:
        cur.close()
        conn.close()

# --- 2. DAG DEFINITION ---
with DAG(
    'terra_agent_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['gis', 'ai']
) as dag:

    # Task 1: Spark Batch Job
    # We include the 'conf' and 'env_vars' to prevent the "Exit Code 1" error
    spark_batch =  SparkSubmitOperator(
        task_id='spark_historical_analysis',
        application='/opt/airflow/dags/scripts/spark_job.py',
        conn_id='spark_default',
        name='TerraAgent_Batch_Job',
        total_executor_cores=1,
        executor_memory='1g',
        conf={
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
            "spark.driver.host": "airflow",      
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.daemon.pymalloc": "false"
        },
        env_vars={
            "PYSPARK_PYTHON": "python3",
            "PYSPARK_DRIVER_PYTHON": "python3"
        }
    )

    # Task 2: Snowflake Sync
    sync_snowflake = PythonOperator(
        task_id='sync_to_snowflake',
        python_callable=process_to_snowflake
    )

    # Define Workflow
    spark_batch >> sync_snowflake