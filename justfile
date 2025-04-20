# Trigger the airflow scheduler
setup_1: infra install_python setup_airflow 

# Creates the connections and move the dags to the airflow folder
setup_2: create_connections_production create_analytic_connections install_dag 

# Start Airflow
setup_3: start_airflow

install_python:
    uv sync

list_dags:
    uv run airflow dags list

run_dags:
    uv run airflow dags trigger generate_time_dimension
    uv run airflow dags trigger generate_campaigns_dimension
    uv run airflow dags trigger generate_daily_clicks
    uv run airflow dags trigger generate_daily_impressions

infra:
    docker-compose up -d

generate_data:
    uv run python main.py batch
    # Generate a complete batch of test data
    uv run python main.py batch --advertisers 5 --campaigns 3 --impressions 1000 --ctr 0.08

setup_airflow:
    uv run airflow db init
    uv run airflow users  create --role Admin --username admin1 --email admin --firstname admin --lastname admin --password admin
    uv run airflow scheduler
    
create_connections_production:
    uv run	airflow connections add 'conn_production_db' \
    --conn-type 'postgresql' \
    --conn-host 'localhost' \
    --conn-schema 'postgres' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-port '5432'

create_analytic_connections:
    uv run	airflow connections add 'conn_analytic_db' \
    --conn-type 'sqlite' \
    --conn-host 'localhost' \
    --conn-schema 'default' \
    --conn-login 'admin' \
    --conn-password 'admin' \
    --conn-port '8123'



install_dag:
	rm ~/airflow/dags/*.py
	cp -f ./dags/*.py ~/airflow/dags/

start_airflow:
    uv run airflow webserver -p 8080


delete_infra:
    docker rm -vf $(docker ps -aq)
