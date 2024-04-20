from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 30, 10, 00),
}


def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res["location"]
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res['gender']
    data["address"] = str(location["street"]["number"]) + " " + location["street"]["name"] + ", " + location[
        "city"] + ", " + location["state"] + ", " + location["country"]
    data["postcode"] = res["location"]["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import logging
    import time

    curr_time = time.time()

    producer = KafkaProducer(bootstrap_servers=["broker:29092"],
                             max_block_ms=5000)  # set a timeout

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            data = format_data(get_data())
            print(json.dumps(data, indent=4))

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