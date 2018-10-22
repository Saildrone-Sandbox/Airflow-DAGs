import os
import json
import requests

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import WeatherFileSensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume

from kubernetes import client, config

volume_mount = VolumeMount('test-dir',
                           mount_path='/data',
                           sub_path=None,
                           read_only=True)

volume_config = {
    'persistentVolumeClaim': {'claimName': 'airflow'}
    }
volume = Volume(name='test-dir', configs=volume_config)

NAM_BASE_DIR = '/data/weatherdata/nam'
EXECUTE_DIR = '/tmp'
POD_PREFIX= 'run-ungrib'

def clean_completed_pods(**context):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    body = client.V1DeleteOptions()
    ret = v1.list_namespaced_pod('airflow')

    for pod in ret.items:
        if pod.metadata.name.startswith('run-ungrib'):
            v1.delete_namespaced_pod(pod.metadata.name, 'airflow', body)


def download_kelp(**context):
    execution_date_dt = context['execution_date']
    execution_date_str = execution_date_dt.isoformat()

    file_name = os.path.join('/tmp', 'kelp_drones_{}.json'.format(execution_date_str))

    with open(file_name, 'w+') as f:
        r = requests.get('https://kelp.saildrone.com/inventory/drones')
        json.dump(r.json(), f, indent=2)


dag = DAG('weather_maker', description='Extract weather input data and trigger forecasts',
          schedule_interval='0 * * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

metgrib_op = PythonOperator(task_id='metgrid',
                            python_callable=download_kelp,
                            dag=dag,
                            provide_context=True)

kubernetes_cleanup_op = PythonOperator(task_id='kubernetes_gc',
                                       python_callable=clean_completed_pods,
                                       dag=dag,
                                       provide_context=True)

ungrib_operators = []

for i in range(1, 29):
    forecast_hour = i * 3

    # dir_template = '{{params.base_dir}}/{{execution_date.strftime("%Y/%m/%d/%H")}}/native/'
    # Use above for real workflow, but need to figure out 00, 06, 12, etc.
    dir_template = '{{params.base_dir}}/2018/09/10/06/native/'
    filename_template = '{{params.f_type}}.t06z.awip32.0p25.f003.{{execution_date.strftime("%Y")}}.09.10'
    file_path = dir_template + filename_template

    check_file_op = WeatherFileSensor(file_path=file_path,
                                      task_id='weather_file_sensor_{}'.format(forecast_hour),
                                      poke_interval=5,
                                      timeout=30,
                                      dag=dag,
                                      params={'base_dir': NAM_BASE_DIR,
                                              'f_type': 'nam'})

    # ungrib.exe must run in the same dir that the three input files are in
    pod_args = ['ln -sf ' + file_path + ' ' + os.path.join(EXECUTE_DIR, 'GRIBFILE.AAA') + ';' +
                'ln -sf /data/ungrib_test/ungrib/Vtable /tmp/Vtable;' +
                'ln -sf /data/ungrib_test/ungrib/namelist.wps /tmp/namelist.wps;' +
                'cd ' + EXECUTE_DIR + ';' +
                '/wrf/WPS-3.9.1/ungrib.exe']
    ungrib_op = KubernetesPodOperator(namespace='airflow',
                                      name='{}-{}'.format(POD_PREFIX, forecast_hour),
                                      task_id='{}_{}'.format(POD_PREFIX, forecast_hour),
                                      dag=dag,
                                      volumes=[volume],
                                      volume_mounts=[volume_mount],
                                      image='quay.io/sdtechops/wrf:latest',
                                      cmds=['bash', '-cx'],
                                      arguments=pod_args,
                                      params={'base_dir': NAM_BASE_DIR,
                                              'f_type': 'nam'},
                                      startup_timeout_seconds=240)

    ungrib_op.set_upstream(check_file_op)

    ungrib_operators.append(ungrib_op)

metgrib_op.set_upstream(ungrib_operators)
kubernetes_cleanup_op.set_upstream(metgrib_op)
