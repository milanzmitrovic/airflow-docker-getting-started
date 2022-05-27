
import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# It is installed into docker container
import pandas as pd
from sqlalchemy import create_engine
from test1 import f1

# How can we know what is working directory of container that is downloaded from repository

dag = DAG(
    dag_id="listing_2_02",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="mkdir -p /test_folder && curl -o /test_folder/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag,
)


def _get_pictures():

    f1()

    # Ensure directory exists
    pathlib.Path("/test_folder/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/test_folder/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/test_folder/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

    df = pd.DataFrame({'name': ['User 1', 'User 2', 'User 3']})

    engine1 = create_engine('postgresql://admin:admin@192.168.8.104:8081/default_database')

    df.to_sql('users', con=engine1, if_exists='append')


get_pictures = PythonOperator(
    task_id="get_pictures", python_callable=_get_pictures, dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /test_folder/images/ | wc -l) images."',
    dag=dag
)

download_launches >> get_pictures >> notify

