# Import necessary modules from Airflow to create a DAG and task
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import time 
import logging 

# Define a Python function to perform as a task in our DAG
def fetch_data():

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res 

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, 20, 00),
}

def stream_data():

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms = 5000)
    
    curr_time = time.time()

    producer = KafkaProducer(bootstrap_servers=["broker:29092"],
                             max_block_ms=5000) 

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            data = format_data(fetch_data())
            print(json.dumps(data, indent=5))

            producer.send("users_created", json.dumps(data).encode("utf-8"))

            time.sleep(2)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data',
        python_callable=stream_data,
    )

    streaming_task