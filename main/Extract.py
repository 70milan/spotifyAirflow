import requests
import os
import spotipy
import spotipy.util as util
import pandas as pd

client_id = os.environ.get("SP_CLIENT_ID")
client_secret = os.environ.get("SP_CLIENT_SECRET")
username = 'x5raulz6ufun7mia2v0s6oqeq'
scope = "user-library-read"
redirect_uri = "http://localhost:7777/callback"

def extract_spotify_liked_songs():
    limit = 20
    offset = 0
    all_items = []
    add = []
    hrefs=[]
    song_list =[]
    artist_list = []
    artist_id = []
    album_list=[]
    track_ids=[]
    genre_list = []
    track_features = []
    # Get the authorization token for the user
    access_token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    headers = {
    'Authorization': 'Bearer {}'.format(access_token)
    }
    response = requests.get('https://api.spotify.com/v1/me/tracks', headers=headers).json()
    total = response['total'] 
    #total
    print("Total 'liked songs' found:", total)
    for offset in range(0, total, 20):
        url = "https://api.spotify.com/v1/me/tracks?offset="+str(offset) + "&limit=20" 
        response1 = requests.get(url, headers=headers).json()
        getter = response1['items']
        all_items.extend(getter)
    print("Processing all", total ,"songs !!")
    try:
        for j in all_items:
            dateAddd = j['added_at'] 
            #dateAdd = dateAddd[0:10]#added date
            add.append(dateAddd)
            s_n = [j['track']['name']]
            Id = [j['track']['id']] #id
            identif = ' '.join(str(v) for v in Id) 
            track_ids.append(identif)
            song_name = ','.join(str(v) for v in s_n) 
            song_list.append(song_name) #tracks
            album = [j['track']['album']['name']]
            album1 = ' '.join(str(v) for v in album) 
            album_list.append(album1) #albums
            artists = j['track']['album']['artists']
            artist_names = []
            artist_ids = []
            artist_genres = []
            for artist in artists:
                artist_names.append(artist['name'])
                artist_ids.append(artist['id'])
            artist_list.append(', '.join(artist_names)) #artist name
            if len(artist_ids) == 1:
                artist_id.append(artist_ids[0])
            else:
                artist_id.append(artist_ids)
            for id in artist_ids:
                url = "https://api.spotify.com/v1/artists/" + id
                response2 = requests.get(url, headers=headers)
                if response2.status_code == 200:
                    artist = response2.json()
                    g_n = artist['genres']
                    artist_genres.extend(g_n)
                else:
                    artist_genres.extend([])
            genre_list.append(artist_genres)    
        while len(genre_list) < len(song_list):
            genre_list.append([])# Ensure that the genre list has the same number of records as the other lists
        url = "https://api.spotify.com/v1/audio-features/"
        for i in track_ids:
            urls = url + i
            res = requests.get(urls, i, headers=headers).json()
            dance_score = [res['id'],res['danceability'], res['energy'],res['key']
            ,res['loudness'],res['mode'],res['speechiness'],res['acousticness']
            ,res['instrumentalness'],res['liveness'],res['valence'], res['tempo']]
            track_features.append(dance_score)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return song_list, album_list, artist_list, genre_list, track_features, add, track_ids, artist_ids, artist_id
    

song_list, album_list, artist_list, genre_list, track_features, add, track_ids, artist_ids, artist_id = extract_spotify_liked_songs()

#load
