import pandas as pd
import logging
import mysql.connector
import json
import os
import db_queries
import pydrive
import transformations


def read_csv():
    script_dir = os.path.dirname(__file__)
    csv_file = os.path.join(script_dir, '../../data/spotify_dataset.csv')
    df = pd.read_csv(csv_file, sep=",")

    logging.info("df: ", df.head())
    logging.info(f"df shape: {df.shape}") 

    return df.to_json(orient='records')


def read_db():
    grammys_df = db_queries.get_table()
    logging.info(f"Df shape: {grammys_df.shape}, Df cols: {grammys_df.columns}")
    logging.info(f"Head: {grammys_df.head()}")

    return grammys_df.to_json(orient='records')

def transform_csv(**kwargs):

    logging.info("kwargs are: ", kwargs.keys())
    # getting task image
    ti = kwargs['ti']
    logging.info("ti: ",ti)
    str_data = ti.xcom_pull(task_ids="read_csv_task")
    logging.info(f"str_data: {str_data}")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataset intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    # dropping unused columns
    df = transformations.drop_csv_cols(df)
    logging.info(f"Dataset new shape: {df.shape[0]} Rows and {df.shape[1]} Columns")
    
    # drop null row
    df = transformations.drop_null(df)

    # transform duration col
    df = transformations.transform_duration(df)

    # dropping old duration_ms col
    df = transformations.drop_ms(df)
    logging.info(f"dataset cols: {df.columns}")

    # transforming the unit of cols that range from 0 to 1
    df = transformations.transform_unit(df)

    # putting trackname in lowercase
    df['track_name'] = transformations.lowercase(df['track_name'])
    logging.info(f"lowercase applied: {df['track_name'][:5]}")

    # choosing the highest popularity song
    df = transformations.groupby(df)
    logging.info(f"Desired shape: (81206, 13), actual shape: {df.shape}")

    return df.to_json(orient='records')

def transform_db(**kwargs):
    
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="read_db_task")

    logging.info(f"str_data: {str_data}")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataset shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    df = transformations.drop_db_cols(df)
    logging.info(f"Dataset columns: {df.columns}")

    df['nominee'] = transformations.lowercase(df['nominee'])
    logging.info(f"lowercase applied: {df['nominee'][:5]}")

    df = transformations.get_songs(df)
    logging.info(f"Song-only dataset desired shape (2203, 5), actual shape: {df.shape}")

    df = transformations.filter_unknown(df)
    logging.info(f"Known artist-only dataset desired shape: (1409, 5), actual: {df.shape}")

    return df.to_json(orient='records')

def merge(**kwargs):

    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids= ["transform_csv_task","transform_db_task"])
    #logging.info(f"str data: {str_data}")
    
    spotify_str = str_data[0]
    grammys_str = str_data[1]
    # taking spotify info    
    logging.info(f"spotify data: {spotify_str}")
    json_data_csv = json.loads(spotify_str)
    spotify_df = pd.json_normalize(data=json_data_csv)
    logging.info(f"SPOTIFY DF: {spotify_df.shape}")

    # taking grammys info
    logging.info(f"grammys data: {grammys_str}")
    json_data_db = json.loads(grammys_str)
    grammys_df = pd.json_normalize(data=json_data_db)
    logging.info(f"GRAMMYS DF: {grammys_df.shape}")
    
    # merging
    merge = grammys_df.merge(spotify_df, how='inner', left_on=['nominee', 'artist'], right_on=['track_name', 'artists'])
    logging.info(f"Merge shape: {merge.shape}")

    merge = transformations.drop_merge_cols(merge)
    logging.info(f"Merge cols: {merge.columns}")
    logging.info(f"Merge final shape: {merge.shape}")

    return merge.to_json(orient='records')

def load(**kwargs):

    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="merge_task")

    logging.info(f"str_data: {str_data}")
    #logging.info(f"str_data type {type(str_data)}")
    
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    
    db_queries.load_data(df)


def store(**kwargs):

    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="merge_task")

    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    # writing the csv in my data directory
    relative_path = '../../data/merge.csv'
    csv_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))
    df.to_csv(csv_file_path, index=False)

    # recovering the csv to upload it
    script_dir = os.path.dirname(__file__)
    csv_file = os.path.join(script_dir, '../../data/merge.csv')

    pydrive.upload_csv(csv_file,'1SDs0jrOvtndTzh7bz7xIuowAtkafGGIT')

