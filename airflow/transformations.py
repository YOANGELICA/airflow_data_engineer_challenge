import pandas as pd

# grammy transformations

def drop_db_cols(df):
    df = df.drop(['published_at','updated_at','img','winner', 'workers'], axis=1)
    return df

def get_songs(df):
    # taking only song nominees
    mask = (df['category'].str.contains('song', case=False, na=False) |
            df['category'].str.contains('performance', case=False, na=False) |
            df['category'].str.contains(r'\brecord\b', case=False, na=False, regex=True))
    # filtering dataset
    return df[mask]

def filter_unknown(df):
    filter = df['artist'] != 'unknown'
    return df[filter]

# ---

def lowercase(col):
    return col.str.lower()

# -- 

# spotify transformations :

def drop_csv_cols(df):
    df = df.drop(['Unnamed: 0','key', 'mode', 'speechiness','instrumentalness', 'liveness', 'tempo', 'time_signature'], axis=1)
    return df

def drop_null(df):
    return df.dropna()

def transform_duration(df):
    # turning duration to secs
    df['duration_min'] = (df['duration_ms'] / 60000).round(2)
    return df

def drop_ms(df):
    df = df.drop('duration_ms', axis=1)
    return df

def transform_unit(df):
    # cols from 0 to 100
    cols = ['danceability', 'energy', 'acousticness', 'valence']
    df[cols] = (df[cols]*100).round(2)
    return df

def groupby(df):
    popular_songs_df = df.groupby(['track_name','artists'])['popularity'].idxmax() 
    df = df.loc[popular_songs_df]
    return df


# merge transformations :

def drop_merge_cols(df):
    df = df.drop(['track_id','artists','album_name','track_name'], axis=1)
    return df