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
        dag_id='transfer_clicks_from_prod_to_analytic',
        start_date=datetime(2025, 1, 1),
        max_active_runs=3, schedule="@daily", catchup=False,
) as dag:
    conn_string = "conn_production_db"
    conn_string_analytic = "conn_analytic_db"
    schema_production = "postgres"
    schema_analytic = "default"
    fact_table = "fact_clicks"

    @task()
    def check_last_day_of_data():
        """
        Check the last day of data in Clickhouse
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        sql = f"SELECT max(timestamp) FROM {schema_analytic}.{fact_table}"
        result = hook.execute(sql)
        if result:
            last_day = result[0][0]
            logger.info(f"Last day of data in Clickhouse: {last_day}")
            return last_day + timedelta(seconds=1)
        else:
            logger.info("No data found in Clickhouse.")
            return None 

    @task()
    def get_clicks_from_production(from_date):
        hook = PostgresHook(conn_string)
        # Filter by Dates (daily?)
        results = hook.get_records("SELECT " \
        "c.advertiser_id, " \
        "c.id campaign_id, " \
        "ci.created_at " \
        "FROM campaign c join " \
        " clicks ci on c.id = ci.campaign_id where ci.created_at > %s", parameters=(from_date,))
        return results
    
    @task()
    def insert_clicks_to_analytic_db(data_rows):
        """
        Insert clicks into analytic table
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        insert_query = f"INSERT INTO {schema_analytic}.{fact_table} (advertiser_id, campaign_id, timestamp) VALUES"
        # TODO paginate properly
        batch_size = 50
        for i in range(0, len(data_rows), batch_size):
            batch = data_rows[i:i + batch_size]
            # Transform to tuple array
            values = [(row[0], row[1], row[2]) for row in batch]
            hook.execute(insert_query, values)
            logger.info(f"Inserted {len(values)} rows into {fact_table} table.")

    # Setup Schema    
    # Get the last day of data in Clickhouse
    last_day = check_last_day_of_data()
    rs = get_clicks_from_production(last_day)
    insert_clicks_to_analytic_db(rs)







