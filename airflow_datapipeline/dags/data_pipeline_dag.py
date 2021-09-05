from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from tasks.load_into_popular_destinations import populate_popular_dest_data
from tasks.load_into_staging import perform_staging_etl
from tasks.load_into_history_table import populate_history_data

"""
This dag file contains all the pipeline tasks to ingest, transform and push data into a database

DAG : New_york_taxi_dag

Task 1: Check if the new csv file exists in the location.
Task 2: Extract, transform and Load the csv data into a "staging_table".
Task 3: Read from the staging table, transform and insert data into the "popular_destination_history" table.
Task 4: Read the latest data inserted into "popular_destination_history" table and load into "popular_destination_current_month" table.
Task 5: Once all the above tasks are successfully completed, delete the staging data from "staging_table" table.

Data Flow : csv -> staging_table-> popular_destination_history->popular_destination_current_month

"""

# Default arguments with default parameters
default_args = {
    'owner': 'Ram',
    'start_date': datetime(2021, 9, 3),
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}

# creating DAG object and using it in all tasks within it
with DAG('New_york_taxi_dag', default_args=default_args, schedule_interval='@monthly',
         template_searchpath=['/usr/local/airflow/sql_files'], catchup=True) as dag:
    # Task to check if the 'green_tripdata.csv' file exists in the data folder
    check_if_file_exists = BashOperator(task_id='check_file',
                                        bash_command='shasum ~/data_files_airflow/green_tripdata.csv', retries=2,
                                        retry_delay=timedelta(seconds=15))

    # Task to load the csv data into a staging table
    prepare_staging_table = PythonOperator(task_id='insert_into_staging_table', provide_context=True,
                                           python_callable=perform_staging_etl)

    # Task to load data from staging into table containing historic data
    prepare_history_table = PythonOperator(task_id='insert_into_history_table', python_callable=populate_history_data,
                                           provide_context=True)

    # Task to load only the current month data for popular destinations
    prepare_current_month_data = PythonOperator(task_id='populate_current_month_data',
                                                python_callable=populate_popular_dest_data, provide_context=True)

    # Task to clean up and delete the staging table
    delete_staging_table = PostgresOperator(task_id='delete_staging_table',
                                            postgres_conn_id="postgres_conn", sql="delete_staging_table.sql")

    # Executing the tasks in order
    check_if_file_exists >> prepare_staging_table >> prepare_history_table >> [prepare_current_month_data,
                                                                               delete_staging_table]
