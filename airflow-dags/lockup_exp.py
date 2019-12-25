#/usr/bin/python3
# -*- coding: utf-8 -*-
 
import logging
import pandas as pd
import airflow
from airflow import DAG
from airflow.utils import dates as date
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.decorators import apply_defaults



""" define DAG arguments which can be override on a per-task basis during operator initialization """
default_args = {
  'owner': 'Tom',
  'depends_on_past': False,
  'start_date': datetime(2019, 11, 27),
  'email': ['tomtupy@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'provide_context': True # Provide_context is required when we're using XComs Airflow's concept to push and pull function results into an other task.
}
 

def get_data(table_file="/tmp/daily", **kwargs):
  logging.info("######## Starting get_data() function ########")
  logging.info("######## Load the table file into a new Pandas DataFrame ########")
  # df_tables = pd.read_csv(table_file, names=["TABLES"])
  # df_tables["TABLES"] = df_tables["TABLES"].str.strip()
  # lst_tables_sqoop = df_tables["TABLES"].tolist()
  return ["AAPL", "MSFT"]


with DAG('lockup_period_expirations', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    get_data_op = DockerOperator(
                task_id='docker_command',
                #image='ipo_lockup_period_exp_scraper:latest',
                image='registry.gitlab.com/tomtupy/ipo_lockup_period_exp_scraper:latest',
                api_version='auto',
                #auto_remove=True,
                command='pipenv run python scraper/scraper.py --kafka-config kafka-config.yml',
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge",
                xcom_push=True,
                xcom_all=True
        )

    # DummyOperator to check if it works!
    complete = DummyOperator(
            task_id="complete")

    # Create DAG
    get_data_op >> complete

