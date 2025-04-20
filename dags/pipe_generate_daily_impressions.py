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
        dag_id='generate_daily_impressions',
        start_date=datetime(2025, 1, 1),
        max_active_runs=3, schedule="@daily", catchup=False,
) as dag:
    conn_string = "conn_production_db"
    conn_string_analytic = "conn_analytic_db"
    schema_production = "postgres"
    schema_analytic = "default"
    daily_click_table = "daily_impressions"

    @task()
    def check_last_day_of_data():
        """
        Check the last day of data in Clickhouse
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        sql = f"SELECT max(created_at) FROM {daily_click_table}"
        result = hook.execute(sql)
        if result:
            last_day = result[0][0]
            logger.info(f"Last day of data in Clickhouse: {last_day}")
            return last_day + timedelta(days=1)
        else:
            logger.info("No data found in Clickhouse.")
            return None

    @task()
    def get_daily_impressions(from_date):
        """
        Get daily impressions from production database
        
        """
        hook = PostgresHook(conn_string)
        results = hook.get_records("select c.advertiser_id, i.campaign_id, " \
        "DATE_TRUNC ('day', i.created_at) as day, " \
        "count(1) count_impressions " \
        "from impressions i " \
        "join campaign c on i.campaign_id = c.id " \
        "where i.created_at > %s "
        "group by i.campaign_id, c.advertiser_id, DATE_TRUNC ('day', i.created_at) order  by day",  parameters=(from_date,))
        return results

    
    @task()
    def insert_into_daily_impressions(data_rows):
        """
        Insert clicks into analytic table
        """
        hook = ClickHouseHook(clickhouse_conn_id=conn_string_analytic)
        insert_query = f"INSERT INTO {daily_click_table} (advertiser_id, campaign_id, created_at, count_of_impressions) VALUES"
        # TODO paginate properly
        batch_size = 50
        for i in range(0, len(data_rows), batch_size):
            batch = data_rows[i:i + batch_size]
            # Transform to tuple array
            values = [(row[0], row[1], row[2], row[3]) for row in batch]
            hook.execute(insert_query, values)
            logger.info(f"Inserted {len(values)} rows into {daily_click_table} table.")

    # Setup Schema    
    # Get the last day of data in Clickhouse
    last_date = check_last_day_of_data()
    rs = get_daily_impressions(last_date)
    insert_into_daily_impressions(rs)







