"""
This file contains all the operations needed to pull data from popular_destination_history
into popular_destination_current_month table.
Operations:
    - Use the keys and query popular_destination_history table.
    - Pre process and clean the dataframe.
    - Insert data into the popular_destination_current_month

"""
import logging
import pandas as pd
from sqlalchemy import Integer, String
from .db_utils import insert_into_table, get_sql_data_into_df, delete_data_from_table


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def populate_popular_dest_data(**kwargs):
    """
    This function is the driver function.
    Step 1: pull the current_month values from context
    Step 2 : use the keys and query staging table.
    Step 3 : Pre process and clean the table
    Step 4 : Insert the data into the popular_destination_current_month table

    :param kwargs: context value holding the current_month and previous_month values
    :return: None

    """
    logging.info("Starting process to load data into table")
    ti = kwargs['ti']
    current_month = ti.xcom_pull(key='current_month', task_ids='insert_into_history_table')
    logging.info("Current: {}".format(current_month))
    sql_query_data = get_data(current_month)
    logging.info('Month_field:{} '.format(current_month))
    del_records_from_table_before_insert()
    logging.info('Data deleted before inserting fresh data')
    df = get_dataframe(sql_query_data)
    logging.info('pandas data frame loaded from sql query')
    insert_into_table('popular_destination_current_month', df, get_staging_dtypes())
    logging.info('Data loaded into the table')


def get_data(current_month):
    """

    :param current_month: current month filter
    :return: pandas data frame with the result of query
    """

    sql_query = '''select "month",pick_up,drop_off,"rank" 
    from popular_destination_history where month = %(current_month)s;'''
    return get_sql_data_into_df(sql_query, current_month)


def del_records_from_table_before_insert():
    """
    :return: delete records from popular_destination_current_month table
    """
    sql_query = '''Delete from popular_destination_current_month;'''
    delete_data_from_table(sql_query)


def get_dataframe(sql_query_df):
    """
    :param sql_query_df: pandas data frame from result of query
    :return: pandas data frame with required columns
    """
    return pd.DataFrame(sql_query_df, columns=['pick_up', 'drop_off', 'rank'])


def get_staging_dtypes():
    """
    :return: dt type of the popular_destination_history table
    """
    return {"pick_up": String(), "drop_off": String(), "rank": Integer()}
