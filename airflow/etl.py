import pandas as pd
import logging
import mysql.connector
import json
import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError

def read_csv():
    script_dir = os.path.dirname(__file__)
    csv_file = os.path.join(script_dir, '../../data/spotify_dataset.csv')
    df = pd.read_csv(csv_file, sep=",")

    logging.info("df: ", df.head())
    logging.info(f"df shape: {df.shape}") 

    return df.to_json(orient='records')


def read_db():
    script_dir = os.path.dirname(__file__)
    json_file = os.path.join(script_dir, '../../db_config.json')
    with open(json_file) as config_json:
        config = json.load(config_json)

    conx = mysql.connector.connect(**config)
    cursor = conx.cursor()

    get_table = "select * from grammys"
    cursor.execute(get_table)

    results = cursor.fetchall()

    grammys_df = pd.DataFrame(results, columns=["year","title","published_at","updated_at","category","nominee","artist","workers","img","winner"])
    logging.info(f"Df shape: {grammys_df.shape}, Df cols: {grammys_df.columns}")
    logging.info(f"Head: {grammys_df.head()}")

    conx.commit()
    cursor.close()

    return grammys_df.to_json(orient='records')

def transform_csv(**kwargs):

    logging.info("kwargs are: ", kwargs.keys())
    ti = kwargs['ti']
    logging.info("ti: ",ti)
    str_data = ti.xcom_pull(task_ids="read_csv_task")
    logging.info(f"str_data: {str_data}")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataset intial shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    # deleting row with nulls
    df=df.dropna()
    # deleting unused cols
    
    del_cols = ['Unnamed: 0','key', 'mode', 'speechiness','instrumentalness', 'liveness', 'tempo', 'time_signature']
    df = df.drop(del_cols, axis=1)

    logging.info(f"Dataset new shape: {df.shape[0]} Rows and {df.shape[1]} Columns")
    
    # turning duration to secs
    df['duration_min'] = df['duration_ms'] / 60000
    df['duration_min'] = df['duration_min'].apply(lambda x: round(x, 2))
    
    # cols from 0 to 100
    float_cols = ['danceability', 'energy', 'acousticness', 'valence']
    df[float_cols] = df[float_cols]*100
    df[float_cols] = df[float_cols].apply(lambda x: round(x, 2))

    # deleting the old duration col
    df = df.drop('duration_ms', axis=1)

    # putting track names in lowercase for future merge
    df['track_name'] = df['track_name'].str.lower()

    # grouping the repeated cols and chooisng the most popular song 
    popular_songs_df = df.groupby(['track_name','artists'])['popularity'].idxmax() 
    df = df.loc[popular_songs_df]

    logging.info(f"Dataset cols: {df.columns}")
    logging.info(f"Desired shape: (81206, 13), actual shape: {df.shape}")

    return df.to_json(orient='records')

def transform_db(**kwargs):
    logging.info("kwargs are: ", kwargs.keys())
    ti = kwargs['ti']
    logging.info("ti: ",ti)
    str_data = ti.xcom_pull(task_ids="read_db_task")
    logging.info(f"str_data: {str_data}")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info(f"data is: {df.head()}")
    logging.info(f"Dataset shape: {df.shape[0]} Rows and {df.shape[1]} Columns")

    # drop unused cols
    df = df.drop(['published_at','updated_at','img','winner', 'workers'], axis=1)
    logging.info(f"Dataset columns: {df.columns}")
    # putting nominees in lowercase for furute merge
    df['nominee'] = df['nominee'].str.lower()
    # taking only song nominees
    mask = (df['category'].str.contains('song', case=False, na=False) |
            df['category'].str.contains('performance', case=False, na=False) |
            df['category'].str.contains(r'record\sor[^i]', case=False, na=False, regex=True))
    # filtering dataset
    result_df = df[mask]
    logging.info(f"Song-only dataset shape: {result_df.shape}")
    filter = result_df['artist'] != 'unknown'
    result_df = result_df[filter]

    logging.info(f"Known artist-only dataset desired shape: (1340,5), actual: {result_df.shape}")

    return result_df.to_json(orient='records')

def merge(**kwargs):
    
    logging.info("kwargs are: ", kwargs.keys())
    ti = kwargs['ti']
    logging.info("ti: ",ti)
   
    str_data = ti.xcom_pull(task_ids= ["transform_csv_task","transform_db_task"])
    logging.info(f"str data: {str_data}")
    
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

    return merge.to_json(orient='records')

def load(**kwargs):

    logging.info("kwargs are: ", kwargs.keys())
    ti = kwargs['ti']
    logging.info("ti: ",ti)
    str_data = ti.xcom_pull(task_ids="merge_task")
    logging.info(f"str_data: {str_data}")
    logging.info(f"str_data type {type(str_data)}")
    
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)
    
    script_dir = os.path.dirname(__file__)
    json_file = os.path.join(script_dir, '../../db_config.json')
    with open(json_file) as config_json:
        config = json.load(config_json)

    conx = mysql.connector.connect(**config)

    # create table
    cursor = conx.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS analysis( 
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        year VARCHAR(255),
                        title VARCHAR(255), 
                        category VARCHAR(255),
                        nominee VARCHAR(255),
                        artist VARCHAR(255), 
                        track_id VARCHAR(255), 
                        artists VARCHAR(255),
                        album_name VARCHAR(255), 
                        track_name VARCHAR(255), 
                        popularity int, 
                        explicit boolean, 
                        danceability float, 
                        energy float,
                        loudness float, 
                        acousticness float, 
                        valence float,
                        track_genre VARCHAR(255), 
                        duration_min float)""")
    cursor.close()

    # insert data
    cursor = conx.cursor()

    for index, row in df.iterrows():
            insert = """INSERT INTO analysis(year,title,category,nominee,artist,track_id,artists,album_name,track_name,
                        popularity,explicit,danceability,energy,loudness,acousticness,valence,track_genre,duration_min)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            data = (row['year'], row['title'], row['category'], row['nominee'], row['artist'], row['track_id'],
                    row['artists'], row['album_name'], row['track_name'], row['popularity'], row['explicit'],
                    row['danceability'], row['energy'], row['loudness'], row['acousticness'], row['valence'],
                    row['track_genre'], row['duration_min'])
            cursor.execute(insert, data)
    conx.commit()
    cursor.close()
    conx.close()
    
    relative_path = '../../data/merge.csv'
    csv_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))
    df.to_csv(csv_file_path, index=False)

    return

def login():
    
    script_dir = os.path.dirname(__file__)
    directorio_credenciales = os.path.join(script_dir, '../../credentials_module.json')

    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = directorio_credenciales
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(directorio_credenciales)

    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
                    
    gauth.SaveCredentialsFile(directorio_credenciales)
    credenciales = GoogleDrive(gauth)
    return credenciales

def upload(ruta_archivo,id_folder):
    
    credenciales = login()
    archivo = credenciales.CreateFile({'parents': [{"kind": "drive#fileLink",\
                                                            "id": id_folder}]})
    archivo['title'] = ruta_archivo.split("/")[-1]
    archivo.SetContentFile(ruta_archivo)
    archivo.Upload()

def store():

    script_dir = os.path.dirname(__file__)
    csv_file = os.path.join(script_dir, '../../data/merge.csv')

    upload(csv_file,'1SDs0jrOvtndTzh7bz7xIuowAtkafGGIT')

    return
