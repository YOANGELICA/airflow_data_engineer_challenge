import pandas as pd
import logging
import mysql.connector
import json

def read_csv():
    csv_file = "data/spotify_dataset.csv"
    df = pd.read_csv(csv_file, sep=",")

    logging.info("df: ", df.head())
    
    return df.to_json(orient='records')


def read_db():
    with open('db_config.json') as config_json:
            config = json.load(config_json)

    conx = mysql.connector.connect(**config)
    cursor = conx.cursor()

    get_table = "select * from grammys"
    cursor.execute(get_table)

    results = cursor.fetchall()

    grammys_df = pd.DataFrame(results)

    conx.commit()
    cursor.close()

    return grammys_df.to_json(orient='records')

def transform_csv():
    pass

def transform_db():
     pass

def merge():
     pass

def load():
     pass

def store():
     pass