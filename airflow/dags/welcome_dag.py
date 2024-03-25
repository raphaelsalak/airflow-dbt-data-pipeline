from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
import psycopg2
import os
import json
import csv
from config import api_key
'''Change in project: use nasa asteroid api to store asteroid data and find which 
asteroids are closest to far and perform window functions on the data. 
It'd be cool if we connected a ui with an image of a animated asteroid video
and the closest asteroid'''
def print_hello_world():
    print('Hello World!')

dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 11 * * *',
    catchup=False
)

task1 = PostgresOperator(
    task_id = 'create_asteroid_table',
    postgres_conn_id ='postgres_localhost',
    sql="""
        create table if not exists asteroid (
                id VARCHAR(50) PRIMARY KEY,
                neo_reference_id VARCHAR(50),
                name VARCHAR(255),
                nasa_jpl_url VARCHAR(255),
                absolute_magnitude_h FLOAT,
                estimated_diameter_min_km FLOAT,
                estimated_diameter_max_km FLOAT,
                estimated_diameter_min_m FLOAT,
                estimated_diameter_max_m FLOAT,
                estimated_diameter_min_miles FLOAT,
                estimated_diameter_max_miles FLOAT,
                estimated_diameter_min_feet FLOAT,
                estimated_diameter_max_feet FLOAT,
                is_potentially_hazardous BOOLEAN,
                is_sentry_object BOOLEAN
            
        );
        """
)

task2 = PostgresOperator(
    task_id = 'create_asteroid_distance_table',
    postgres_conn_id ='postgres_localhost',
    sql="""
          create table if not exists CloseApproach (
            id serial PRIMARY KEY,
            neo_id VARCHAR(50),
            close_approach_date DATE,
            close_approach_date_full VARCHAR(100),
            epoch_date_close_approach BIGINT,
            relative_velocity_kmps FLOAT,
            relative_velocity_kmph FLOAT,
            relative_velocity_mph FLOAT,
            miss_distance_astronomical FLOAT,
            miss_distance_lunar FLOAT,
            miss_distance_km FLOAT,
            miss_distance_miles FLOAT,
            orbiting_body VARCHAR(50),
            FOREIGN KEY (neo_id) REFERENCES asteroid(id)
            );  
        """
)

test_task2 = PostgresOperator(
    task_id = 'insert_data',
    postgres_conn_id ='postgres_localhost',
    sql="""
        insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        on conflict (dt, dag_id) do nothing;
        """
)

test_task3 = PostgresOperator(
    task_id = 'delete_data',
    postgres_conn_id ='postgres_localhost',
    sql="""
        delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'; 
        """
)

print_hello_world_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag  # Changed dag2 to dag
)

@task
def get_data():
    # NOTE: configure this as appropriate for your airflow environment
    data_path = "/opt/airflow/dags/files/asteroid.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    url = "https://api.nasa.gov/neo/rest/v1/feed?start_date=2015-09-07&end_date=2015-09-08&api_key=" + api_key
    postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    response = requests.request("GET", url)

    if response.status_code == 200:
        json_data = response.json()
        '''array of neo objects so the data looks like this
        date, 1st entry
        date, 2nd entry
        date, 3rd entry
        needs date or _ to map over the values'''
        for _, near_earth_objects in json_data['near_earth_objects'].items():
            for entry in near_earth_objects:
                # Extract relevant information from the entry
                id = entry['id']
                name = entry['name']
                # Extract other fields as needed

                # Check if the entry already exists in the database
                cur.execute("SELECT id FROM asteroid WHERE id = %s", (id,))
                existing_asteroid = cur.fetchone()

                # If the entry doesn't exist, insert it into the database
                if not existing_asteroid:
                    cur.execute("""
                        INSERT INTO asteroid (id, name)
                        VALUES (%s, %s)
                    """, (id, name))
        conn.commit()
[print_hello_world_task, get_data()] >> task1 >> task2 >> test_task3 >> test_task2