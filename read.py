import pandas as pd
import logging

def read_csv():
    csv_file = "data/spotify_dataset.csv"
    df = pd.read_csv(csv_file, sep=",")

    logging.info("df: ", df.head())
    
    return df.to_json(orient='records')


def read_db():
    #grammys
    #TODO: read from db, put it in a csv and return df

    pass

def extract_api():
    #TODO: api not chosen yet.

    pass

read_csv()