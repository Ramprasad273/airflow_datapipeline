import pandas as pd
from sqlalchemy import String, Integer
from .db_utils import insert_into_table, get_history_sql_data_into_df
import logging

"""
This file contains all the operations needed to pull data from staging into popular_destination_history table.
Operations:
    - Use the keys and query staging table.
    - Pre process and clean the dataframe.
    - Insert data into the popular_destination_history
    
"""

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def populate_history_data(**kwargs):
    """
    This function is the driver function.
    Step 1: pull the current_month and previous_month values from context
    Step 2 : use the keys and query staging table.
    Step 3 : Pre process and clean the table
    Step 4 : Insert the data into the popular_destination_history table

    :param kwargs: context value holding the current_month and previous_month values
    :return: None
    """
    logging.info("Starting process to load data into history table")
    ti = kwargs['ti']
    current_month = ti.xcom_pull(key='current_month', task_ids='insert_into_staging_table')
    previous_month = ti.xcom_pull(key='previous_month', task_ids='insert_into_staging_table')
    logging.info('Month_field: {} '.format(current_month))
    sql_query_data = get_staging_data(current_month, previous_month)
    logging.info('Data fetched from staging table')
    df = preprocess_dataframe(sql_query_data)
    logging.info('Data cleaned and pre processed')
    insert_into_table('popular_destination_history', df, get_staging_dtypes())
    logging.info('Data loaded into history table')
    kwargs['ti'].xcom_push(key='current_month', value=current_month)
    logging.info("ETL for loading data into history table completed")


def get_staging_data(current_month, previous_month):
    """

    :param previous_month: previous month data for querying
    :param current_month: current month data for querying
    :return: dataframe from the sql query executed
    """
    sql_query = '''
select month,pick_up,drop_off,rank
from (
      select month, pick_up,drop_off,rank, count(1) over (partition by pick_up,drop_off,rank) rank_changed_records
      from(
           select *
           from (
                 select month,pick_up,drop_off,rank() over (order by total_travelled desc)
                 from (
                 select month,pick_up,drop_off, count(passenger_count) total_travelled
	                 from staging_table
	                 group by pick_up,drop_off,month
                 ) t order by month,pick_up ,drop_off
                ) t2
       union
       select * 
       from popular_destination_history where month = %(previous_month)s
               ) t1
       ) changed_ranks where rank_changed_records = 1 and month = %(current_month)s;'''

    return get_history_sql_data_into_df(sql_query, previous_month, current_month)


def preprocess_dataframe(sql_query_df):
    """

    :param sql_query_df: pandas dataframe
    :return: cleaned dataframe
    """
    df = pd.DataFrame(sql_query_df, columns=['month', 'pick_up', 'drop_off', 'rank'])
    df.month = df.month.astype(str)
    return df


def get_staging_dtypes():
    """
    :return: dt type of the popular_destination_history table
    """
    return {"month": String(), "pick_up": String(), "drop_off": String(), "rank": Integer()}


def write_to_history_table(df):
    """
    :param df: processed pandas dataframe which will be loaded into DB
    :return: None
    """
    insert_into_table('popular_destination_history', df, get_staging_dtypes())
