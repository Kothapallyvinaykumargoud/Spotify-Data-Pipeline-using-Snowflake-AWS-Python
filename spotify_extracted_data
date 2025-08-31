import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
def lambda_handler(event, context):
  

  client_id=os.environ.get('client_id')
  client_secret=os.environ.get('client_secret')
  client_credentials_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)  
  sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
  spotify_playlist= "https://open.spotify.com/playlist/4zzUm9eZmeb4t4nUCaNoo5?utm_source"
  playlist_URI=spotify_playlist.split("/")[-1].split("?")[0]
  data=sp.playlist_tracks(playlist_URI)
  
  client=boto3.client("s3")

  filename="spotify_raw"+str(datetime.now())+".json"

  client.put_object( 
   Bucket="spotify-project-kothavinaygoud", 
   Key="raw_data/to_processed/"+filename, 
   Body=json.dumps(data) 
)
