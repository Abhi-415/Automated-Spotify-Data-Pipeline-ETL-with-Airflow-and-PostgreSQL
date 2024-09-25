import extract
import transform
import pandas as pd
import datetime
import psycopg2
import json
import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# Database connection details
hostname='localhost'
database='spotify_db'
username='postgres'
pwd='1234'
port=5432

#setup postgreSQL connection url
DATABASE_LOCATION= "postgresql+psycopg2://postgres:1234@localhost:5432/spotify_db"

if __name__=="__main__":
    
    #Importing songs from export.py
    load_df=extract.return_dataframe()
    if (transform.Data_Quality(load_df)==False):
        raise("Failed at data validation")
    Transformed_df=transform.Transform_df(load_df)
    
    #Connect to PostgrSQL
    engine=sqlalchemy.create_engine(DATABASE_LOCATION)
    conn=engine.connect()
    #SQL Query to Create Played Songs
    sql_query_1 = text("""
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT pk_constraint_tracks PRIMARY KEY (played_at)
        )
        """)
    #SQL Query to Create Most Listened Artist
    sql_query_2 = text("""
    CREATE TABLE IF NOT EXISTS fav_artist(
        timestamp VARCHAR(200),
        ID VARCHAR(200),
        artist_name VARCHAR(200),
        count VARCHAR(200),
        CONSTRAINT pk_constraints_artist PRIMARY KEY (ID)
        )
        """)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(sql_query_1)
            conn.execute(sql_query_2)
        print("Opened database successfully")
    
    #load data into table while avoiding duplicates
    try:
        #Insert my_played_trscks data
        load_df.to_sql("my_played_tracks",engine,index=False,if_exists='append')
    except sqlalchemy.exc.IntegrityError as e:
        print(f"IntegrityError: {e} - Data already exists in the 'my_played_tracks' table.")
    
    try:
        #insert fav_artist data
        Transformed_df.to_sql("fav_artist",engine,index=False,if_exists='append')
    except sqlalchemy.exc.IntegrityError as e:
        print(f"IntegrityError: {e} - Data already exists in the 'fav_artist' table.")
    
    conn.close()
    print("closed postgreSQL database successfully")
    