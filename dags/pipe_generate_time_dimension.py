from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from pendulum import datetime
import logging
import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def generate_time_dimension(start_date, end_date, freq='D'):
    """
    Generate Time Dimension Table (DataFrame)
    """
    df = pd.DataFrame()
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    df['timestamp'] = date_range
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    # Monday=0, Sunday=6
    df['weekday'] = df['timestamp'].dt.weekday
    return df

with DAG(
        dag_id='generate_time_dimension',
        start_date=datetime(2025, 1, 1),
        max_active_runs=1, catchup=False,
) as dag:
    """
    DAG for generating time table 
    """
    conn_string = "conn_analytic_db"
    schema = "default"
    time_dim_table = "time_dimension"
    
    @task()
    def insert_time_values(df):
        hook = ClickHouseHook(clickhouse_conn_id=conn_string)
        chunks = np.array_split(df, 100)  # Split into 100 chunks
        for chunk in chunks:
            values =  [(row['timestamp'], row['year'],
                row['month'],
                row['day'],
                row['weekday'])
              for _, row in chunk.iterrows()]
            insert_sql = f"""
            INSERT INTO {schema}.{time_dim_table} (timestamp, year, month, day, weekday)
            VALUES
            """
            result = hook.execute(insert_sql, values)
            logger.info(f"Inserted {schema}.{result} rows into {time_dim_table} table.")
    
    df = generate_time_dimension('01-01-2020', '01-01-2027')
    insert_time_values(df)

    





