from pyflink.table import EnvironmentSettings, TableEnvironment
import json
import time

# 1. Setup
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)

# 2. Source
table_env.execute_sql("""
    CREATE TABLE news_source (
        parcel_id STRING,
        news_event STRING
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '1',
        'number-of-rows' = '10'
    )
""")

# 3. Use a Python Collector to write a SINGLE file
# This bypasses the Flink 'part-file' behavior which confuses Airflow
print("--- Flink Job Starting: Writing to /opt/flink/data/flink_output.json ---")

# Pull data from the table
table = table_env.sql_query("SELECT 'P101' as parcel_id, 'New Zoning Approved' as news_event FROM news_source LIMIT 1")

# Convert to Pandas then to a single JSON file
df = table.to_pandas()
df.to_json('/opt/flink/data/flink_output.json', orient='records')

print("--- Flink Job Finished: File Created ---")