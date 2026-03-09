from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime
import snowflake.connector
import os
import json

def process_flink_to_snowflake():
    # 1. Read Flink's output
    file_path = '/opt/airflow/data/flink_output.json'
    with open(file_path, 'r') as f:
        raw_data = json.load(f)
    
    data = raw_data[0] if isinstance(raw_data, list) else raw_data

    # 2. Safety Check: Print variables (Do not print password!)
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    print(f"DEBUG: Connecting to Snowflake Account: {account} as User: {user}")

    if not account or not user:
        raise ValueError("Snowflake credentials missing! Check your docker-compose env_file.")

    # 3. Connect
    conn = snowflake.connector.connect(
        user=user,
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=account,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE LAND_RECORDS SET VALUATION = VALUATION * 1.10 WHERE ID = '{data['parcel_id']}'")
        conn.commit()
        print(f"✅ Snowflake Updated for {data['parcel_id']}")
    finally:
        cur.close()
        conn.close()
        
with DAG('land_intelligence_etl', start_date=datetime(2024, 1, 1), schedule='@daily', catchup=False) as dag:
    
    wait_for_flink = FileSensor(
        task_id='wait_for_flink_data',
        filepath='flink_output.json',
        fs_conn_id='fs_default',
        poke_interval=10
    )

    update_snowflake = PythonOperator(
        task_id='update_snowflake_from_stream',
        python_callable=process_flink_to_snowflake
    )

    wait_for_flink >> update_snowflake