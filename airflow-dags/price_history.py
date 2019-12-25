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
from airflow.utils.decorators import apply_defaults
 
SP100 = ["AAPL", "ABBV", "ABT", "ACN", "ADBE", "AGN", "AIG",
          "ALL", "AMGN", "AMZN", "AXP", "BA", "BAC", "BIIB", "BK", 
          "BKNG", "BLK", "BMY", "BRK.B", "C", "CAT", "CELG", "CHTR", 
          "CL", "CMCSA", "COF", "COP", "COST", "CSCO", "CVS", "CVX", 
          "DD", "DHR", "DIS", "DOW", "DUK", "EMR", "EXC", "F", "FB", 
          "FDX", "GD", "GE", "GILD", "GM", "GOOG", "GOOGL", "GS", "HD", 
          "HON", "IBM", "INTC", "JNJ", "JPM", "KHC", "KMI", "KO", "LLY", 
          "LMT", "LOW", "MA", "MCD", "MDLZ", "MDT", "MET", "MMM", "MO", 
          "MRK", "MS", "MSFT", "NEE", "NFLX", "NKE", "NVDA", "ORCL", 
          "OXY", "PEP", "PFE", "PG", "PM", "PYPL", "QCOM", "RTN", "SBUX", 
          "SLB", "SO", "SPG", "T", "TGT", "TXN", "UNH", "UNP", "UPS", "USB", 
          "UTX", "V", "VZ", "WBA", "WFC", "WMT", "XOM"]

""" define DAG arguments which can be override on a per-task basis during operator initialization """
default_args = {
  'owner': 'Tom',
  'depends_on_past': False,
  'start_date': datetime(2018, 4, 15),
  'email': ['tom-kun@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'provide_context': True # Provide_context is required when we're using XComs Airflow's concept to push and pull function results into an other task.
}
 
"""
Define a daily DAG using the default arguments by changing the "schedule_interval" arguments the "dag_daily" should be launched every day to process bunch of tables from db2 to HDFS
"""
dag_daily = DAG(
  'daily',
  default_args=default_args,
  description='Importing daily data from DB2 IBM database into HDFS as parquet files',
  schedule_interval='@once'
)
 
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

def get_tables(table_file="/tmp/daily", **kwargs):
  logging.info("######## Starting get_tables() function ########")
  logging.info("######## Load the table file into a new Pandas DataFrame ########")
  # df_tables = pd.read_csv(table_file, names=["TABLES"])
  # df_tables["TABLES"] = df_tables["TABLES"].str.strip()
  # lst_tables_sqoop = df_tables["TABLES"].tolist()
  return ["AAPL", "MSFT"]

def sqoop_commands(table, **kwargs):
  """
  Returning a BashOperator using the list previously returned and use the table name when importing data from RGDBM into HDFS through Sqoop.
  """
  lst_tables_mapping = "{{ task_instance.xcom_pull(task_ids='get_tables') }}"
  return BashOperator(task_id="sqoop_import_{}_table".format(table),
    bash_command="echo " + table,
    params=import_parameters,
    dag=dag_daily)

# PythonOperator to get the list of the tables.
push_tables_list = PythonOperator(
        task_id="get_tables",
        python_callable=get_tables,
        dag=dag_daily)

# DummyOperator to check if it works!
complete = DummyOperator(
        task_id="complete",
        dag=dag_daily)

for table in get_tables():
  push_tables_list >> sqoop_commands(table) >> complete