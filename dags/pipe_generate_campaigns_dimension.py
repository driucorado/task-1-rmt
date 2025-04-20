from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from pendulum import datetime
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with DAG(
        dag_id='generate_campaigns_dimension',
        start_date=datetime(2025, 1, 1),
        max_active_runs=3, schedule="@daily", catchup=False,
) as dag:
    conn_string = "conn_production_db"
    conn_string_analytic = "conn_analytic_db"
    schema_production = "postgres"
    schema_analytic = "default"
    campaign_dimension = "campaign_dimension"

    @task()
    def check_campaigns():
        """
        Check the last day of data in Clickhouse
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        sql = f"SELECT campaign_id FROM {schema_analytic}.{campaign_dimension}"
        return  hook.execute(sql)
    
    @task()
    def get_campaigns(ids_list):
        """
        Get daily clicks from production database
        
        """
        hook = PostgresHook(conn_string)
        id_str = ",".join([id_[0] for id_ in ids_list])
        where_sql = " where c.id not in ({id_str})" if id_str else ""
        results = hook.get_records(f"select c.id, c.bid, c.budget from campaign c {where_sql}")
        return results

    @task()
    def create_campaigns_dimension():
        """
        Create Fact Impressions Table (Clickhouse format)
        """
        sql_table_ = f"""CREATE TABLE IF NOT EXISTS {campaign_dimension} (
            bid Float32,
            budget Float32,
            campaign_id Int32
        ) 
        ENGINE = MergeTree()
        ORDER BY (campaign_id)
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        hook.execute(sql_table_)    
    
    @task()
    def insert_new_campaigns(data_rows):
        """
        Insert clicks into analytic table
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        insert_query = f"INSERT INTO {campaign_dimension} (campaign_id, bid, budget) VALUES"
        # TODO paginate properly
        batch_size = 50
        for i in range(0, len(data_rows), batch_size):
            batch = data_rows[i:i + batch_size]
            # Transform to tuple array
            values = [(row[0], row[1], row[2]) for row in batch]
            hook.execute(insert_query, values)
            logger.info(f"Inserted {len(values)} rows into {campaign_dimension} table.")

    # Setup Schema

    create_campaigns_dimension() >> \
    insert_new_campaigns(data_rows=get_campaigns(ids_list=check_campaigns()))
    







