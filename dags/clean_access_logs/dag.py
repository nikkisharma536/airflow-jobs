from airflow import DAG
import airflow
import os
import datetime
from common.airflow_job_config import key_path, ip

dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(dir_path, "./scripts/incremental_etl_with_param.hql")

args = {
    'owner': 'nikita',
    'start_date': datetime.datetime(2019, 1, 25),
}

dag = DAG(
    'clean_access_logs',
    schedule_interval="20 5 * * *",
    default_args=args
)

start = airflow.operators.DummyOperator(
    task_id='start',
    dag=dag
)

hive_task = airflow.operators.HiveCustomOperator(
    task_id='run_on_hive',
    script_path=file_path,
    run_date='{{ds}}',
    pem_file_path=key_path,
    emr_master_ip=ip,
    dag=dag
)

end = airflow.operators.DummyOperator(
    task_id='end',
    dag=dag
)

start.set_downstream(hive_task)
hive_task.set_downstream(end)
