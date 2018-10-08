import os
import json
import requests

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def download_kelp(**context):
    execution_date_dt = context['execution_date']
    execution_date_str = execution_date_dt.isoformat()

    file_name = os.path.join('/tmp', 'kelp_drones_{}.json'.format(execution_date_str))

    with open(file_name, 'w+') as f:
        r = requests.get('https://kelp.saildrone.com/inventory/drones')
        json.dump(r.json(), f, indent=2)


dag = DAG('download_kelp', description='Simple DAG to download all drones from Kelp',
          schedule_interval='*/15 * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

download_operator = PythonOperator(task_id='download_kelp',
                                   python_callable=download_kelp,
                                   dag=dag,
                                   provide_context=True)
