import requests
import pandas as pd
from urllib.parse import urlencode
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
import base64
import os
import spotipy
import spotipy.util as util



'''
CREATING ENGINE FOR POSTGRES DB CONNECTION

('postgresql+psycopg2://user:password@hostname/database_name')
'''
engine = create_engine('postgresql://postgres:3231@localhost:5432/de')

#postgres DB connection#

"""spotify api connection process"""

client_id = os.environ.get("SP_CLIENT_ID")
client_secret = os.environ.get("SP_CLIENT_SECRET")


limit = 20
offset = 0

all_items = []
add = []
hrefs=[]
song_list =[]
artist_list = []
genre_list = []
ids = []
diff_scores = []
album_list=[]

# Get the authorization token for the user
username = 'x5raulz6ufun7mia2v0s6oqeq'
scope = "user-library-read"
redirect_uri = "http://localhost:7777/callback"

access_token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)


headers = {
'Authorization': 'Bearer {}'.format(access_token)
}

response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()

total = response['total'] 
 #total
print("Total 'liked songs' found: ", total)
##$ to save the json object ##

def get_liked_songs(total,headers,all_items):
    for offset in range(0, total, 20):
        url = "https://api.spotify.com/v1/me/tracks?offset="+str(offset) + "&limit=20" 
        response1 = requests.get(url, headers=headers).json()
        getter = response1['items']
        all_items.extend(getter)

get_liked_songs(total,headers,all_items)

print("Processed all", total ,"songs !!")

def get_all_items(all_items, headers, add, ids, song_list,album_list,artist_list,genre_list):
    for j in all_items:
        dateAddd = j['added_at'] 
        dateAdd = dateAddd[0:10]#added date
        add.append(dateAdd)
        s_n = [j['track']['name']]
        Id = [j['track']['id']] #id
        identif = ' '.join(str(v) for v in Id) 
        ids.append(identif) 
        song_name = ','.join(str(v) for v in s_n) 
        song_list.append(song_name) #tracks
        album = [j['track']['album']['name']]
        album1 = ' '.join(str(v) for v in album) 
        album_list.append(album1) #albums
        artist = [j['track']['album']['artists']]
        for g in artist:
            if len(g) > 1:
                art = g[0]['name'], g[1]['name']
                artist_name = ', '.join(str(v) for v in art) 
            else:
                artist_name = g[0]['name']
            artist_list.append(artist_name) #artist name
            href = g[0]['href']
            response2 = requests.get(href,headers=headers).json()
            g_n = response2['genres']
            #genress = ', '.join(str(v) for v in g_n) 
            genre_list.append(g_n) 

get_all_items(all_items, headers, add, ids,song_list,album_list,artist_list,genre_list)

#########Get Audio features###########

def get_song_features(ids, headers, diff_scores):
    url = "https://api.spotify.com/v1/audio-features/"
    for i in ids:
        urls = url + i
        res = requests.get(urls, i, headers=headers).json()
        dance_score = [res['id'],res['danceability'], res['energy'],res['key']
        ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
        ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo']]
        diff_scores.append(dance_score)

get_song_features(ids,headers,diff_scores)

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
    engine.execute("drop table if exists master_sp.dim_details_1 cascade;")
    df_merge_2.to_sql('dim_details_1', engine, schema = 'master_sp', if_exists='replace', index=False)
    df.to_sql('dim_track_details', engine, schema = 'master_sp', if_exists='replace', index=False)
    df2.to_sql('fact_track_features', engine, schema = 'master_sp', if_exists='replace', index=False)


merge_and_load_to_postgres(ids, add,song_list, album_list, artist_list, diff_scores, genre_list, engine)


print("Done!!")





