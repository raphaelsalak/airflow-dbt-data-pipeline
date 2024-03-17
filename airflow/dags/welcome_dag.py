from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
import psycopg2
import os

#put in config file
'''Change in project: use nasa asteroid api to store asteroid data and find which 
asteroids are closest to far and perform window functions on the data. 
It'd be cool if we connected a ui with an image of a animated asteroid video
and the closest asteroid'''
hostname = os.environ.get('host')
database = os.environ.get('database')
username = os.environ.get('username')
pwd = os.environ.get('password')
port_id = os.environ.get('port_id')

def get_market_data():
    # No keys required for crypto data
    client = CryptoHistoricalDataClient()
    # Creating request object
    request_params = CryptoBarsRequest(
        symbol_or_symbols=["BTC/USD"],
        timeframe=TimeFrame.Day,
        start="2022-09-01",
        end="2022-09-07"
    )

    # Retrieve daily bars for Bitcoin in a DataFrame and printing it
    btc_bars = client.get_crypto_bars(request_params)

    # Convert to dataframe
    print(btc_bars.df)
def transform_data():
    pass
def connect_to_db():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
                    host = hostname,
                    dbname = database,
                    user = username,
                    password = pwd,
                    port = port_id)
        #cur = conn.cursor()
        print("successfully connected to postgres db")
    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()



def print_welcome():
    print('Welcome to Airflow!')

def print_hello_world():
    print('Hello World!')

dag = DAG(
    'welcome_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 11 * * *',
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_hello_world_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag  # Changed dag2 to dag
)
get_market_data_task = PythonOperator(
    task_id='get_market_data',
    python_callable=get_market_data,
    dag=dag
)
connect_to_db_task = PythonOperator(
    task_id='connect_to_db',
    python_callable=connect_to_db,
    dag=dag
)

print_welcome_task >> print_hello_world_task >> get_market_data_task >> connect_to_db_task
