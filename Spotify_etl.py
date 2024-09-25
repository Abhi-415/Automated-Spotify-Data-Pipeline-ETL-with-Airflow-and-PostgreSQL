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
        yesterday = current_time - datetime.timedelta(days=365)
        # Convert to UNIX timestamp (milliseconds)
        return int(yesterday.timestamp() * 1000)

    # Authorization URL and Headers for OAuth2 Authorization Code Flow (User-specific access)
    AUTH_URL = 'https://accounts.spotify.com/api/token'

    # Your client_id and client_secret from Spotify Dashboard
    client_id = 'your client id'
    client_secret = 'your secrect client id'
    refresh_token='your refresh token'  
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

# Set of Data Quality Checks Needed to Perform Before Loading
def Data_Quality(load_df):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Songs Extracted')
        return False
    
    #Enforcing Primary keys since we don't need duplicates
    if pd.Series(load_df['played_at']).is_unique:
       pass
    else:
        #The Reason for using exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception,Data Might Contain duplicates")
    
    #Checking for Nulls in our data frame 
    if load_df.isnull().values.any():
        raise Exception("Null values found")

# Writing some Transformation Queries to get the count of artist
def Transform_df(load_df):

    #Applying transformation logic
    Transformed_df=load_df.groupby(['timestamp','artist_name'],as_index = False).count()
    Transformed_df.rename(columns ={'played_at':'count'}, inplace=True)

    #Creating a Primary Key based on Timestamp and artist name
    Transformed_df['id'] = Transformed_df['timestamp'].astype(str) +"-"+ Transformed_df["artist_name"]

    return Transformed_df[['id','timestamp','artist_name','count']]

def spotify_etl():
    #Importing the songs_df from the Extract.py
    load_df=return_dataframe()
    Data_Quality(load_df)
    #calling the transformation
    Transformed_df=Transform_df(load_df)    
    print(load_df)
    return (load_df)

spotify_etl()
