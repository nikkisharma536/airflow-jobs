from airflow import DAG
import airflow

import os
from common.s3_utils import copy_to_s3
from common.system_utils import execute_local, del_local_file
import datetime


###############################################################
#  constants and configs
###############################################################
location = 's3://nikita-ds-playground/raw/access-log/'

log_generator_repo_path = "/Users/nikki/work/code/Fake-Apache-Log-Generator/"

###############################################################
#  Utility functions
###############################################################


def run(**kwargs):
    date = str(kwargs['ds'])
    log_files = generate_data()
    copy_to_s3(location + date + '/', log_files)
    del_local_file(log_files)


def generate_data():
    os.chdir(log_generator_repo_path)

    cmd = ["./venv/bin/python", "apache-fake-log-gen.py", "-n", "1000", "-o", "GZ"]
    execute_local(cmd)

    # List files in current directory
    files = os.listdir(os.curdir)
    # particular files from a directories
    list = [log_generator_repo_path + k for k in files if 'access_log' in k]
    print(list)
    return list

###############################################################
#  Airflow dag
###############################################################

args = {
    'owner': 'nikita',
    'start_date': datetime.datetime(2019, 1, 25),
}

dag = DAG(
    'generate_access_logs',
    schedule_interval="20 5 * * *",
    default_args=args
)

start = airflow.operators.DummyOperator(
    task_id='start',
    dag=dag
)

python_task = airflow.operators.PythonOperator(
    task_id='generate_data',
    provide_context=True,
    python_callable=run,
    dag=dag
)

end = airflow.operators.DummyOperator(
    task_id='end',
    dag=dag
)

start.set_downstream(python_task)
python_task.set_downstream(end)
