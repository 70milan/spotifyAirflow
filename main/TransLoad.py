import pandas as pd
from sqlalchemy import create_engine
import Extract as e
from configparser import ConfigParser
import os
import hashlib




# Read configuration from file
config = ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__name__)), 'database.ini'))

try:
    # Get Postgres connection details from config
    postgres_user = config.get('postgres', 'user')
    postgres_password = config.get('postgres', 'password')
    postgres_host = config.get('postgres', 'host')
    postgres_port = config.get('postgres', 'port')
    postgres_database = config.get('postgres', 'database')
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')
    print("Connected to Postgres database successfully!")
except Exception as e:
    print("Failed to connect to Postgres database: ", e)







def dataframes_transformer(e):

    df_original = pd.DataFrame({"track_id": e.track_ids,"track_list":e.song_list,"album_name" : e.album_list})
    
    ##Date##
    #manipulating date and time
    df_date = pd.DataFrame({"datetime" : pd.to_datetime(e.add)})
    df_date["date_added"] = df_date["datetime"].dt.date
    df_date["time_added"] = df_date["datetime"].dt.time
    df_date['timezone'] = df_date['datetime'].dt.tz
    df_date.insert(loc=0, column='track_id', value=e.track_ids) # Add the 'id' column to df_artists
    df_date["datetime_track_id"] = df_date["datetime"].astype(str) + "_" + df_date["track_id"].astype(str)
    df_date["datetime_track_id_hash"] = df_date["datetime_track_id"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    df_date["datetime_track_id"] = df_date["datetime"].astype(str) + "_" + df_date["track_id"].astype(str)
    df_date = df_date.drop(columns=['datetime_track_id'])
    # Set the new column as the primary key
    df_date.set_index("datetime_track_id_hash", inplace=True)
        
    ###artist##
    #separating the artists and merging the df_artist to final-1 df
    df_artists = pd.
    df_artists = pd.DataFrame({"artist_name": e.artist_list })
    df_artists = df_artists['artist_name'].str.split(',', expand=True).add_prefix('col')
    df_artists.columns = [f"artist_{i}" for i in range(1, len(df_artists.columns) + 1)]
    df_artists.insert(loc=0, column='track_id', value=e.track_ids) # Add the 'id' column to df_artists
    df_artists.insert(loc = 1, column= 'artist_id', value=e.artist_id)

    df_original_merged = pd.merge(df_original, df_artists, on='track_id') # Merge df and df_artists on 'id'

    #add columns to df_genre(separate table for genre)
    df_genre = pd.DataFrame(e.genre_list)
    #max colmns
    max_num_cols = df_genre.shape[1]
    col_names = [f"genre_{i+1}" for i in range(0, max_num_cols)]
    df_genre.columns = col_names
    df_genre.insert(loc=0, column='track_id', value=e.track_ids) # Add the 'id' column to df_artists

    #(separate table for features)
    df_features = pd.DataFrame(e.track_features)
    df_features.columns=['track_id','danceability','energy','key','loudness','mode','speechiness','acousticness', 'instrumentalness','liveness','valence', 'tempo']
    #df_features.insert(loc=0, column='track_list', value=song_list) # Add the 'id' column to df_artists

    mega_merged_df = pd.merge(pd.merge(df_original_merged, df_features, on=['track_id']), df_genre, on=['track_id'])

    return mega_merged_df, df_features, df_artists, df_genre, df_original_merged, df_date

mega_merged_df, df_features, df_artists, df_genre, df_original_merged, df_date = dataframes_transformer(e)


def load_to_psgdb(df_features, df_artists, df_genre, mega_merged_df, df_original_merged):
    connection = engine.connect()
    connection.execute("drop table if exists master_sp.dim_details_large cascade;")
    #dim_everythin_part_one
    df_original_merged.to_sql('dim_details_small', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_everything_bloated_table
    mega_merged_df.to_sql('dim_details_large', engine, schema='master_sp', if_exists='replace', index=False)
    #fact_features
    df_features.to_sql('fact_track_features', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_track_artists
    df_artists.to_sql('dim_track_artists', engine, schema='master_sp', if_exists='replace', index=False)
    #dim_genres
    df_genre.to_sql('dim_track_genres', engine, schema='master_sp', if_exists='replace', index=False)

load_to_psgdb(df_features, df_artists, df_genre, mega_merged_df, df_original_merged)


# Define the function for executing the SQL views
def execute_views(sql_file_path):
    with engine.connect() as connection:
        with open(sql_file_path, "r") as file:
            sql_file = file.read()
        connection.execute(sql_file)
    print("Views created successfully!!")


sql_file_path = 'D:/spotify/data/sequel.sql'
execute_views(sql_file_path)

print("Done!!")


'''


df_original = pd.DataFrame({"track_id": e.track_ids, "date_added":e.add,"track_list":e.song_list,"album_name" : e.album_list})

#seperating the artists and merging the df_artist to final-1 df
df_artists = pd.DataFrame({"artist_name": e.artist_list })
df_artists = df_artists['artist_name'].str.split(',', expand=True).add_prefix('col')
df_artists.columns = [f"artist_{i}" for i in range(1, len(df_artists.columns) + 1)]
df_artists.insert(loc=0, column='track_id', value=e.track_ids) # Add the 'id' column to df_artists
df_artists.insert(loc = 1, column= 'artist_id', value=e.artist_id)

df_original_merged = pd.merge(df_original, df_artists, on='track_id') # Merge df and df_artists on 'id'

#add columns to df_genre(seperate table for genre)
df_genre = pd.DataFrame(e.genre_list)
max_num_cols = df_genre.shape[1]
col_names = [f"genre_{i+1}" for i in range(0, max_num_cols)]
df_genre.columns = col_names
df_genre.insert(loc=0, column='track_id', value=e.track_ids) # Add the 'id' column to df_artists

#(seperate table for features)
df_features = pd.DataFrame(e.track_features)
df_features.columns=['track_id','danceability','energy','key','loudness','mode','speechiness','acousticness', 'instrumentalness','liveness','valence', 'tempo']
#df_features.insert(loc=0, column='track_list', value=song_list) # Add the 'id' column to df_artists

mega_merged_df = pd.merge(pd.merge(df_original_merged, df_features, on=['track_id']), df_genre, on=['track_id'])
#mega_merged_df.to_csv('d:/all_songs_history.csv')



# Define the function for merging and loading data to Postgres
def merge_and_load_to_postgres(ids, add, song_list, album_list, artist_list, diff_scores, genre_list, engine):
    


connection = engine.connect()
connection.execute("drop table if exists master_sp.dim_details_1 cascade;")
#dim_everything_bloated_table
mega_merged_df.to_sql('dim_details_1', engine, schema = 'master_sp', if_exists='replace', index=False)
#fact_features
df_features.to_sql('fact_track_features', engine, schema = 'master_sp', if_exists='replace', index=False)
#dim_track_artists
df_artists.to_sql('dim_track_artists', engine, schema = 'master_sp', if_exists='replace', index=False)
#dim_genres
df_genre.to_sql('dim_track_genres', engine, schema = 'master_sp', if_exists='replace', index=False)




    #droping tables since views depends on them
    connection = engine.connect()
    connection.execute("drop table if exists master_sp.dim_details_1 cascade;")
    df_merge_2.to_sql('dim_details_1', engine, schema = 'master_sp', if_exists='replace', index=False)
    df.to_sql('dim_track_details', engine, schema = 'master_sp', if_exists='replace', index=False)
    df2.to_sql('fact_track_features', engine, schema = 'master_sp', if_exists='replace', index=False)
    connection.close()

# Call the function to merge and load the data to Postgres
merge_and_load_to_postgres(e.ids, e.add, e.song_list, e.album_list, e.artist_list, e.diff_scores, e.genre_list, engine)

# Define the function for executing the SQL views
def execute_views(sql_file_path):
    with engine.connect() as connection:
        with open(sql_file_path, "r") as file:
            sql_file = file.read()
        connection.execute(sql_file)

    print("Views created successfully!!")

# Call the function to execute the SQL views
sql_file_path = 'C:/projects/de/py/apicalls/spotify/data/sequel.sql'
execute_views(sql_file_path)

print("Done!!")

'''