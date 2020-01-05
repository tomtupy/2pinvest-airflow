#/usr/bin/python3
# -*- coding: utf-8 -*-
 
import logging
import airflow
import requests
from airflow import DAG
from airflow.utils import dates as date
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow import AirflowException
from airflow.models import Variable


""" define DAG arguments which can be override on a per-task basis during operator initialization """
default_args = {
  'owner': 'Tom',
  'depends_on_past': False,
  'start_date': datetime(2020, 1, 4),
  'email': ['tomtupy@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'provide_context': True # Provide_context is required when we're using XComs Airflow's concept to push and pull function results into an other task.
}

""" Parameters sent to the BashOperator command """
import_parameters = {
  'db_host': 'slave2.mycluster',
  'db_port': '5047',
  'db_name': 'imdb',
  'db_schema': 'DEV',
  'username': 'Tom',
  'mappers': 3,
  'target_dir': '/user/airflow/',
  'compression_type': 'snappy'
}

def check_mds_health(**kwargs):
  mds_host = Variable.get("mds_url")
  health_check_url = f"{mds_host}/status"
  request = None
  try:
    request = requests.get(url = health_check_url) 
    # extracting data in json format
    data = request.json()
    if "result" in data:
      return True
  except Exception as error:
    if request is not None:
      print(request.text)
  raise AirflowException('MDS health check failed!')

def process_companies(**kwargs):
  logging.info("######## Starting process_companies() function ########")
  mds_host = Variable.get("mds_url")
  ipo_exp_companies_url = f"{mds_host}/ipos/lockup_expirations/companies"
  price_history_queue_put_url = f"{mds_host}/queue/price_history/put"

  request = None
  try:
    get_params = {}
    while True:
      get_request = requests.get(url = ipo_exp_companies_url, params = get_params) 
      # extracting data in json format
      data = get_request.json()
      if "result" not in data:
        request = get_request
        break
      companies = data["result"]["companies"]
      print(f"Got {len(companies)} companies")
      for company in companies:
        post_data = {"symbol": company["symbol"], 
                      "force_recheck": "false", 
                      "use_cached_stats": "true"}
        print("Posting", post_data)
        post_request = requests.post(url = price_history_queue_put_url, data = post_data)
        print("Post result", post_request.text)
        if post_request.status_code != 200 or "result" not in post_request.json():
          request = post_request
          raise Exception('Post request failed!')
      if "nextkey" in data["result"]:
        get_params["lastkey"] = data["result"]["nextkey"]
      else:
        # done
        return True
  except Exception as error:
    if request is not None:
      print(request.text)
  raise AirflowException('Processing of companies failed!')


"""
Define a daily DAG using the default arguments by changing the "schedule_interval" arguments the "dag_daily" should be launched every day to process bunch of tables from db2 to HDFS
"""
dag_lockup_exp_price_data = DAG(
  'lockup_period_expirations_price_data',
  default_args=default_args,
  description='Importing daily data from DB2 IBM database into HDFS as parquet files',
  schedule_interval='@once',
  catchup=False
)

# PythonOperator to get the list of the tables.
op_check_mds_health = PythonOperator(
        task_id="check_mds_health",
        python_callable=check_mds_health,
        dag=dag_lockup_exp_price_data)

# PythonOperator to get the list of the tables.
op_process_companies = PythonOperator(
        task_id="process_companies",
        python_callable=process_companies,
        dag=dag_lockup_exp_price_data)

# DummyOperator to check if it works!
op_complete = DummyOperator(
        task_id="complete",
        dag=dag_lockup_exp_price_data)

# Create DAG
op_check_mds_health >> op_process_companies >> op_complete
