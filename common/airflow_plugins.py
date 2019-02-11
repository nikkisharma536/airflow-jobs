from airflow.plugins_manager import AirflowPlugin
from hive_custom_operator import HiveCustomOperator


class MyAirflowPlugin(AirflowPlugin):
  name = 'my_namespace'
  operators = [HiveCustomOperator]
