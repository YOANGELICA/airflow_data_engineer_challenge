import mysql.connector
import pandas as pd
import os
import json


def connection():
    script_dir = os.path.dirname(__file__)
    json_file = os.path.join(script_dir, '../secrets/db_config.json')

    with open(json_file) as config_json:
        config = json.load(config_json)

    conx = mysql.connector.connect(**config)
    return conx
    

def get_table():
    
    conx = connection()
    cursor = conx.cursor()

    get_table = "select * from grammys"
    cursor.execute(get_table)

    results = cursor.fetchall()
    grammys_df = pd.DataFrame(results, columns=["year","title","published_at","updated_at","category","nominee","artist","workers","img","winner"])

    conx.commit()
    cursor.close()
    connection.close()
    return grammys_df


def create_table():

    conx = connection()
    cursor = conx.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS songs( 
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        year VARCHAR(255),
                        title VARCHAR(255), 
                        category VARCHAR(255),
                        nominee VARCHAR(255),
                        artist VARCHAR(255),
                        popularity int, 
                        explicit boolean, 
                        danceability float, 
                        energy float,
                        loudness float, 
                        acousticness float, 
                        valence float,
                        track_genre VARCHAR(255), 
                        duration_min float)""")
    
  
    conx.commit()
    cursor.close()
    connection.close()

def load_data(df):

    create_table()

    conx = connection()
    cursor = conx.cursor()

    for index, row in df.iterrows():
            insert = """INSERT INTO analysis(year,title,category,nominee,artist,track_id,artists,album_name,track_name,
                        popularity,explicit,danceability,energy,loudness,acousticness,valence,track_genre,duration_min)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            data = (row['year'], row['title'], row['category'], row['nominee'], row['artist'],
                    row['popularity'], row['explicit'], row['danceability'], row['energy'], 
                    row['loudness'], row['acousticness'], row['valence'], row['track_genre'], row['duration_min'])
            cursor.execute(insert, data)

    conx.commit()
    cursor.close()
    conx.close()