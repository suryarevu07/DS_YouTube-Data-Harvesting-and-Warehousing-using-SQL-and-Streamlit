from googleapiclient.discovery import build
import pymongo
import _mysql_connector
# BUILDING CONNECTION WITH YOUTUBE API
api_key ='AIzaSyB2WyHFpvg7O5CzlctF4cLFcA8bkm8pEhg' 
youtube = build('youtube','v3',developerKey=api_key)

def chan(channle_id):
    api_key ='AIzaSyC3Tk2zydyDZF0cQ3uFBjLNcukgrD8KXbg' 
    youtube = build('youtube','v3',developerKey=api_key)
    id=channle_id
    c_d=[]
    
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channle_id).execute()
    for i in response['items']:
        data=dict(channel_Id=i['id'],
                  channel_name=i['snippet']['title'],
                  subscribe_count=i['statistics']['subscriberCount'],
                  channle_view=i['statistics']['viewCount'],
                  channel_desbribtion=i['snippet']['description'],
                  playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
        c_d.append(data)
    return c_d
channle_id='UCbEd9lNwkBGLFGz8ZxsZdVA'
channels_detai_var1=chan(channle_id)

def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []

    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                PublishedAt=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
            )
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data
get_playlist_details(channle_id)
play_list_det=get_playlist_details(channle_id)


def vedio_de(channel_id):
    ved_ids=[]
    next_page_token=None
    res = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    play_li=res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    while True:
            request = youtube.playlistItems().list(
            part='snippet',
            playlistId=play_li,
            maxResults=50,
            pageToken=next_page_token).execute()
            
            for i in range(len(request['items'])):
                    ved_ids.append(request['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=request.get('nextPageToken')
            
            if next_page_token is None:
                    break
                    
    return ved_ids
vedio_id_variable=vedio_de(channle_id)
vedio_id_variable 

def ved_details(vid_ids):
    ved_list = []
    for i in range(len(vid_ids)):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vid_ids[i]
        )
        
        response = request.execute()
        
        for i in response['items']:
            data ={
                    'video_id': i.get('id'),
                    'video_name': i['snippet'].get('title', 'No title available'),
                    'video_description': i['snippet'].get('description', 'No description available'),
                    'tags': i['snippet'].get('tags', []),
                    "Channel_name":i['snippet'].get('channelTitle',0),
                    'channel_id':i['snippet'].get('channelId',0),
                    'published_at': i['snippet'].get('publishedAt', 'Unknown'),
                    'view_count': i['statistics'].get('viewCount', '0'),
                    'like_count': i['statistics'].get('likeCount', '0'),
                    'favorite_count': i['statistics'].get("favoriteCount", '0'),
                    'comment_count': i['statistics'].get('commentCount', '0'),
                    'duration': i['contentDetails'].get('duration', 'Unknown'),
                    'thumbnail': i['snippet']['thumbnails']['default'].get('url', 'No thumbnail'),
                    'caption_status': i['contentDetails'].get('caption', 'Unavailable')
                    }
            
            # Append the processed video data to the list
            ved_list.append(data)
    
    return ved_list

ved_details(vedio_id_variable)
vedios_details_var=ved_details(vedio_id_variable)
vedios_details_var

def comment_detail(video_id_list):  
    all_comments = [] 

    for video_id in video_id_list: 
        next_page_token = None  
        comments = []
        
        while True:
            try:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,  
                    maxResults=100,
                    pageToken=next_page_token
                )
                response = request.execute()

                
                for item in response['items']:
                    data = dict(
                        Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_id=video_id,
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt']
                    )
                    comments.append(data)

                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break

            except Exception as e:
                print(f"An error occurred for video ID {video_id}: {e}")
                break
        
        all_comments.extend(comments)  

    return all_comments   

commed_details_vari=comment_detail(vedio_id_variable)
commed_details_vari

def channel_deta():
    data=dict(
        channel_details=channels_detai_var1,
        playlist_details=play_list_det,
        vedio_details=vedios_details_var,
        commed_details=commed_details_vari
    )
    return data

channel_all_details=channel_deta()
channel_all_details
  
  
  
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://suryagaya07:spg1234@cluster0.lajvyca.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    

from pymongo import MongoClient
def mongo_push(channel_all_details):
   try:
      client = MongoClient("mongodb+srv://suryagaya07:spg1234@cluster0.lajvyca.mongodb.net/?retryWrites=true&w=majority")
      #channel_name = channel_all_details['channel_details'][0]['channel_name'].replace(' ','_')
     # channle_id=channel_all_details['channel_details'][0]['channel_Id']
      db = client['you_tube_db']
      channel_details_collection = db['channel_details']
      video_details_collection = db['video_details']
      playlist_details_collection=db['playlist_details']
      comment_details_collection = db['comment_details']
      if 'channel_details' in channel_all_details:
         channel_details_collection.insert_many(channel_all_details['channel_details'])
      if 'playlist_details' in channel_all_details:
         playlist_details_collection.insert_many(channel_all_details['playlist_details'])

      if 'vedio_details' in channel_all_details:
         video_details_collection.insert_many(channel_all_details['vedio_details'])

      if 'commed_details' in channel_all_details:
         comment_details_collection.insert_many(channel_all_details['commed_details'])
      
        # print("Inserted comment details successfully.")
      print('insert successfully')
   except Exception as e:
      print('already databes exite',e)
   #print('insert successfully')

mongo_channel_id=mongo_push(channel_all_details)
mongo_channel_id


from pymongo import MongoClient
import pandas as pd

def get_data():
    try:
        client = MongoClient("mongodb+srv://suryagaya07:spg1234@cluster0.lajvyca.mongodb.net/?retryWrites=true&w=majority")

        db = client['you_tube_db']
            
        channel_details_collection = db['channel_details']
        playlist_details_collection = db['playlist_details']
        video_details_collection = db['video_details']
        comment_details_collection = db['comment_details']
        

        channel_details = list(channel_details_collection.find({}, {'_id': 0}))
        playlist_details =list(playlist_details_collection.find({}, {'_id': 0}))
        video_details = list(video_details_collection.find({}, {'_id': 0}))
        comment_details = list(comment_details_collection.find({}, {'_id': 0}))
        
        sql_channel_details = pd.DataFrame(channel_details)
        sql_playlist_details = pd.DataFrame(playlist_details)
        sql_video_details = pd.DataFrame(video_details)
        sql_comment_details = pd.DataFrame(comment_details)
        
      

        
        print('successfully retrived data')
        return sql_channel_details,sql_playlist_details, sql_video_details, sql_comment_details
    except:
        print("No matching channel found.")
        

sql_channel_details,sql_playlist_details, sql_video_details, sql_comment_details= get_data() 

 
import mysql.connector
def sql_push(sql_channel_details,sql_playlist_details,sql_video_details,sql_comment_details):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="", 
    )
    print(mydb)
    mycursor = mydb.cursor(buffered=True)

    mycursor.execute('CREATE DATABASE IF NOT EXISTS you_tube_db')
    mycursor.execute('USE you_tube_db')
    mycursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_details (
            channel_Id VARCHAR(50),
            channel_name VARCHAR(50),
            subscribe_count INT,
            channle_view INT,
            channel_desbribtion VARCHAR(100),
            playlist_id VARCHAR(50)
        )
        """)
    
    mydb.commit()
    for _, row in sql_channel_details.iterrows():
        sql='insert into channel_details (channel_Id,channel_name,subscribe_count,channle_view,channel_desbribtion,playlist_id)  VALUES (%s, %s, %s, %s, %s, %s)'
        val = tuple(row)
        mycursor.execute(sql, val)
    mydb.commit()


    mycursor.execute("""
            create table if not exists playlist_details(
            Playlist_Id VARCHAR(20),
            Title VARCHAR(50),
            Channel_Id VARCHAR(50),
            Channel_Name VARCHAR(50),
            PublishedAt DATETIME,
            Video_Count int
            )
            """)
    for _, row in sql_playlist_details.iterrows():
        sql='insert into playlist_details (Playlist_Id,Title,Channel_Id,Channel_Name,PublishedAt,Video_Count)  VALUES (%s, %s, %s, %s, %s, %s)'
        val = tuple(row)
        mycursor.execute(sql, val)
    mydb.commit()


    mycursor.execute("""
            CREATE TABLE IF NOT EXISTS video_details (
                video_id VARCHAR(50),
                video_name VARCHAR(50),
                video_description CHAR(50),
                tags CHAR(80),
                Channel_name VARCHAR(50),
                Channel_id VARCHAR(50),
                published_at CHAR(40),
                view_count INT(40),
                like_count INT(20),
                favorite_count INT(30),
                comment_count INT(50),
                duration VARCHAR(50),
                thumbnail CHAR(30),
                caption_status VARCHAR(50)
            )
        """)
    mydb.commit()

    for _, row in sql_video_details.iterrows():
        row = [",".join(item) if isinstance(item, list) else item for item in row] # convert list to str
        sql='insert into video_details (video_id,video_name,video_description,tags,Channel_name,Channel_id,published_at,view_count,like_count,favorite_count,comment_count,duration,thumbnail,caption_status)  VALUES (%s,%s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)'
        val = tuple(row)
        mycursor.execute(sql, val)
    mydb.commit()


    mycursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_details (
                Comment_Id VARCHAR(50),
                Video_id CHAR(50),
                Comment_Text CHAR(80),
                Comment_Author CHAR(40),
                Comment_PublishedAt DATETIME
                )
        """)
    mydb.commit()

    for _, row in sql_comment_details.iterrows():
        sql='insert into comment_details (Comment_Id,Video_id,Comment_Text,Comment_Author,Comment_PublishedAt)  VALUES (%s, %s, %s, %s, %s)'
        val = tuple(row)
        mycursor.execute(sql, val)
    mydb.commit()
    
def sql_questions():
    import mysql.connector


    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="", 
    )

    print(mydb)
    mycursor = mydb.cursor(buffered=True)  

    # What are the names of all the videos and their corresponding channels?
    mycursor.execute('select video_name, Channel_name from video_details')
    for i in mycursor:
        print(i)

    # Which channels have the most number of videos, and how many videos do
    #  they have?
    mycursor.execute('select * from video_details group by Channel_Name')
    for i in mycursor:
        print(i)
    # What are the top 10 most viewed videos and their respective channels?
    mycursor.execute('select view_count,video_name,Channel_name from video_details order by view_count desc limit 10' )
    for i in mycursor:
        print(i)
    # How many comments were made on each video, and what are their
    #  corresponding video names?

    mycursor.execute('select Channel_name,video_name,comment_count from video_details group by video_id')
    for i in mycursor:
        print(i)

    # Which videos have the highest number of likes, and what are their 
    # corresponding channel names?

    mycursor.execute('select Channel_name,like_count from video_details order by like_count desc limit 10')
    for i in mycursor:
        print(i)

    # What is the total number of likes and dislikes for each video, and what are 
    # their corresponding video names? 
    mycursor.execute('select Channel_name,video_name,like_count,favorite_count from video_details group by video_id')
    for i in mycursor:
        print(i)

    # What is the total number of views for each channel, and what are their 
    # corresponding channel names?
    mycursor.execute('select Channel_name,sum(view_count) from video_details group by Channel_name')
    for i in mycursor:
        print(i)

    # What are the names of all the channels that have published videos in the year
    #  2022?

    mycursor.execute('select Channel_name,published_at from video_details where year(published_at) = 2022 group by Channel_name')
    for i in mycursor:
        print(i)

    # What is the average duration of all videos in each channel, and what are their 
    # corresponding channel names?

    mycursor.execute('select Channel_name,avg(duration) from video_details group by Channel_name')
    for i in mycursor:
        print(i)

    # Which videos have the highest number of comments, and what are their 
    # corresponding channel names?
    mycursor.execute('select Channel_name,video_name,comment_count from video_details order by comment_count desc limit 1')
    for i in mycursor:
        print(i)
        

        
        
import streamlit as st
import mysql.connector
import pandas as pd


def connect_to_db():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  
        database="you_tube_db"
    )
    return mydb


def fetch_data(query):
    mydb = connect_to_db()
    mycursor = mydb.cursor()
    mycursor.execute(query)
    rows = mycursor.fetchall()
    columns = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df


st.title("YouTube Data Analysis Dashboard")


query_options = [
    "Video names and their corresponding channels",
    "Channels with the most videos",
    "Top 10 most viewed videos",
    "Comments count per video",
    "Videos with the highest likes",
    "Total likes and favorites per video",
    "Total views per channel",
    "Channels that published videos in 2022",
    "Average duration of videos per channel",
    "Videos with the highest number of comments"
]

query_dict = {
    query_options[0]: 'SELECT video_name, Channel_name FROM video_details',
    query_options[1]: 'SELECT Channel_name, COUNT(*) AS Video_Count FROM video_details GROUP BY Channel_name ORDER BY Video_Count DESC',
    query_options[2]: 'SELECT video_name, Channel_name, view_count FROM video_details ORDER BY view_count DESC LIMIT 10',
    query_options[3]: 'SELECT video_name, comment_count FROM video_details',
    query_options[4]: 'SELECT video_name, Channel_name, like_count FROM video_details ORDER BY like_count DESC LIMIT 10',
    query_options[5]: 'SELECT video_name, like_count, favorite_count FROM video_details',
    query_options[6]: 'SELECT Channel_name, SUM(view_count) AS Total_Views FROM video_details GROUP BY Channel_name',
    query_options[7]: "SELECT Channel_name FROM video_details WHERE YEAR(published_at) = 2022 GROUP BY Channel_name",
    query_options[8]: 'SELECT Channel_name, AVG(duration) AS Average_Duration FROM video_details GROUP BY Channel_name',
    query_options[9]: 'SELECT video_name, Channel_name, comment_count FROM video_details ORDER BY comment_count DESC LIMIT 1'
}

selected_query = st.selectbox("Select a query to analyze:", query_options)

if st.button("Run Query"):
    query = query_dict[selected_query]
    result_df = fetch_data(query)
    st.write(f"Results for: **{selected_query}**")
    st.dataframe(result_df)

