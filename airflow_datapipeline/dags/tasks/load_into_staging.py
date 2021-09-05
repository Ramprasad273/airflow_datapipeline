from statistics import mode
import pandas as pd
from sqlalchemy import String, Integer
from .db_utils import insert_into_table, get_sql_data_into_df, get_df_from_db
import logging

"""
This file contains the task to read the csv file and load the data into staging_table.

Operations:
    - Read csv file from file location.
    - Pre process and clean the data in the data frame.
    - based on the location look_up table, data in pick_up and drop_off columns are updated
    - insert the processed data frame into DB.
    - Push xcom value which is the month field as it will be used by other task for queries.  

"""

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def perform_staging_etl(**kwargs):
    """
     This is the driver function to load data into the staging_table.
     Step 1: Load the data frame from file location
     Step 2: Clean and pre process the data frame
     Step 3: Insert the data frame into staging_table
     Step 4: Push the month field in dataframe into context.
             This value will be used in where clause of other tasks
    :param kwargs: push the month field into context to be used by other tasks
    :return: None
    """
    logging.info("Starting process to load data into staging")
    staging_df = load_dataframe()
    logging.info("Data frame loaded")
    preprocessed_df, prev_month = preprocess_dataframe(staging_df)
    logging.info("Data frame cleaned and pre processed")

    loc_df = get_location_data()
    df = update_location_with_lookup(preprocessed_df, loc_df)

    insert_into_table('staging_table', df, get_staging_dtypes())
    logging.info("Data loaded into table")
    kwargs['ti'].xcom_push(key='current_month', value=preprocessed_df['month'][0])
    kwargs['ti'].xcom_push(key='previous_month', value=prev_month)
    logging.info("ETL for staging table completed")


def get_location_data():
    """

    :return: pandas location data frame
    """
    location_sql_query = ''' select location_id,zone from location_lookup;'''
    return get_df_from_db(location_sql_query)


def update_location_with_lookup(df, loc_df):
    """

    :param df: dataframe containing the taxi data
    :param loc_df: location dataframe
    :return: dataframe updated with lookup location data
    """

    loc_df['location_id'] = loc_df['location_id'].apply(str)
    df['pick_up'] = df['pick_up'].apply(str)
    df['drop_off'] = df['drop_off'].apply(str)
    print(df.head())

    pick_up_list = list(df['pick_up'])
    drop_off_list = list(df['drop_off'])
    location_dict = dict(zip(loc_df['location_id'], loc_df['zone']))

    df['pick_up'] = [location_dict[item] for item in pick_up_list]
    df['drop_off'] = [location_dict[item] for item in drop_off_list]
    return df


def load_dataframe():
    """
    Read the csv file from location and return the pandas dataframe
    :return:  pandas data frame which will be loaded into DB
    """
    parse_dates = ['lpep_pickup_datetime']
    return pd.read_csv('~/data_files_airflow/green_tripdata.csv',
                       usecols=["lpep_pickup_datetime", "passenger_count", "PULocationID",
                                "DOLocationID"], parse_dates=parse_dates)


def preprocess_dataframe(df):
    """
    :param df: pandas data frame which will be processed and cleaned
    :return: processed data frame which is to be loaded in DB
    """
    df.rename(columns={'PULocationID': 'pick_up', 'DOLocationID': 'drop_off', 'lpep_pickup_datetime': 'month'},
              inplace=True)
    df['month'] = df['month'].dt.to_period('M')
    df['month'] = mode(df['month'])
    # filling missing value of passenger count with average value (1.3)
    df = df.fillna(df.mean())
    df.passenger_count = df.passenger_count.astype(int)
    # updating the date value with mode of the data frame value
    df['month'] = mode(df['month'])
    prev_month = mode(df['month'] - 1)
    df.month = df.month.astype(str)
    return df, prev_month.strftime('%Y-%m')


def get_staging_dtypes():
    """
    :return: dt type which will be used to insert data into the DB
    """
    return {"month": String(), "pick_up": String(), "drop_off": String(),
            "passenger_count": Integer()}
