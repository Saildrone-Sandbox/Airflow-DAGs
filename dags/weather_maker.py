import os
import json
import requests

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import WeatherFileSensor


NAM_BASE_DIR = '/data/weatherdata/nam'


def download_kelp(**context):
    execution_date_dt = context['execution_date']
    execution_date_str = execution_date_dt.isoformat()

    file_name = os.path.join('/tmp', 'kelp_drones_{}.json'.format(execution_date_str))

    with open(file_name, 'w+') as f:
        r = requests.get('https://kelp.stage.saildrone.com/inventory/drones')
        json.dump(r.json(), f, indent=2)


dag = DAG('weather_maker', description='Extract weather input data and trigger forecasts',
          schedule_interval='0 * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

metgrib_op = PythonOperator(task_id='metgrid',
                            python_callable=download_kelp,
                            dag=dag,
                            provide_context=True)

ungrib_operators = []

for i in range(1, 29):
    forecast_hour = i * 3

    check_file_op = WeatherFileSensor(forecast_type='nam',
                                      forecast_hour=forecast_hour,
                                      task_id='weather_file_sensor_{}'.format(forecast_hour),
                                      poke_interval=5,
                                      dag=dag)

    ungrib_op = PythonOperator(task_id='run_ungrib_{}'.format(forecast_hour),
                               python_callable=download_kelp,
                               dag=dag,
                               provide_context=True)

    ungrib_op.set_upstream(check_file_op)

    ungrib_operators.append(ungrib_op)

metgrib_op.set_upstream(ungrib_operators)