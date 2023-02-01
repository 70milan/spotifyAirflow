from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
import pandas as pd
import os.path
import Extract as e

'''
CREATING ENGINE FOR POSTGRES DB CONNECTION

('postgresql+psycopg2://user:password@hostname/database_name')
'''
engine = create_engine('postgresql://postgres:3231@localhost:5432/de')
sql_file_path = 'C:/projects/de/py/apicalls/spotify/data/sequel.sql'
#gathering and loading the data

def merge_and_load_to_postgres(ids, add, song_list, album_list, artist_list, diff_scores, genre_list, engine):
    df = pd.DataFrame({"id": ids, "date_added":add,"track_list":song_list,"album_name" : album_list, "artists_list": artist_list})
    df2 = pd.DataFrame(diff_scores)
    df2.columns=['id','danceability','energy','key','loudness','mode','speechiness','acousticness', 'instrumentalness','liveness','valence', 'tempo']
    cols = df2.columns.difference(df.columns)
    df_merge_1 = pd.concat([df, df2[cols]], join = 'outer',axis=1)
    df3 = pd.DataFrame(genre_list)
    df3.columns=['genre1','genre2','genre3','genre4','genre5','genre6','genre7','genre8', 'genre9','genre10','genre11', 'genre12', 'genre13', 'genre14', 'genre15']
    df_merge_2 = pd.concat([df_merge_1, df3], join='outer', axis=1)

    #droping tables since views depends on them
    connection = engine.connect()
    connection.execute("drop table if exists master_sp.dim_details_1 cascade;")
    df_merge_2.to_sql('dim_details_1', engine, schema = 'master_sp', if_exists='replace', index=False)
    df.to_sql('dim_track_details', engine, schema = 'master_sp', if_exists='replace', index=False)
    df2.to_sql('fact_track_features', engine, schema = 'master_sp', if_exists='replace', index=False)
    connection.close()

merge_and_load_to_postgres(e.ids, e.add, e.song_list, e.album_list, e.artist_list, e.diff_scores, e.genre_list, engine)
print("tables loaded successfully!!")


def execute_views(sql_file_path):
    connection = engine.connect()
    with open(sql_file_path, "r") as file:
        sql_file = file.read()
    connection.execute(sql_file)
    connection.close()

execute_views(sql_file_path)

print("Views created successfully!!")

print("Done!!")
#postgres DB connection#




'''
connection = engine.connect()
sql_file_path = 'C:/projects/de/py/apicalls/spotify/data/sequel.sql'
def execute_views(sql_file_path):
    connection = engine.connect()
    with open(sql_file_path, "r") as file:
        sql_file = file.read()
    connection.execute(sql_file)
    connection.close()
execute_views(sql_file_path)
connection = engine.connect()
with open('C:/projects/de/py/apicalls/spotify/data/sequel.sql', "r") as file:
    sql_file = file.read()
connection.execute(sql_file)
connection.close()
import pathlib
cwd = pathlib.Path().resolve()
sql_file_path = os.path.join(cwd, "data", "test.sql")
path = os.path.join(os.getcwd(), "..sequel.sql")
sql_file_path = os.path.normpath(path)
sql_file_path = sql_file_path.replace("//", "/")
sql_file_path = os.path.join(cwd, "data", "test.sql")
sql_file_path = sql_file_path.replace("//", "/")
'''