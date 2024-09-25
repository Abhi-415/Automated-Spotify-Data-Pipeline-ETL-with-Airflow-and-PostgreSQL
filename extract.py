import requests
import time
import datetime
import pandas as pd


def return_dataframe():
    # Function to convert the current time to the UNIX timestamp for 'yesterday'
    def get_yesterday_unix_timestamp():
        # Get the current time
        current_time = datetime.datetime.now()
        # Subtract 1 day from the current time
        yesterday = current_time - datetime.timedelta(days=1)
        # Convert to UNIX timestamp (milliseconds)
        return int(yesterday.timestamp() * 1000)

    # Authorization URL and Headers for OAuth2 Authorization Code Flow (User-specific access)
    AUTH_URL = 'https://accounts.spotify.com/api/token'

    # Your client_id and client_secret from Spotify Dashboard
    client_id = 'Your Client Id'
    client_secret = 'Your Secrect client Id'
    refresh_token='your refresh token '  
    # Store your refresh token if you're using OAuth flow

    # Request access token using refresh token (replace with your token process if needed)
    auth_response = requests.post(AUTH_URL, {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
        }
                                  )

    # Parse response
    auth_response_data = auth_response.json()
    access_token = auth_response_data['access_token']

    # Set headers for authorized access
    headers = {
        'Authorization': f'Bearer {access_token}'
        }

    # Get yesterday's Unix timestamp
    yesterday_unix_timestamp = get_yesterday_unix_timestamp()

    # Request to get all recently played tracks from the past 24 hours (after yesterday)
    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix_timestamp}", headers=headers)

    # Parse JSON data
    data = r.json()
   
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
        }
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    return song_df





