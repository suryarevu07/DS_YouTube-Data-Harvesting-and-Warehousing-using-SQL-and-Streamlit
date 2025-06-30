from quart import Quart, request, jsonify
from googleapiclient.discovery import build
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector
from mysql.connector import pooling
import pandas as pd
import aiohttp
import asyncio
import logging
import time
from cachetools import TTLCache
import re

app = Quart(__name__)

# logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


YOUTUBE_API_KEY = "AIzaSyC3Tk2zydyDZF0cQ3uFBjLNcukgrD8KXbg"  
MONGODB_URI = "mongodb+srv://suryagaya07:spg1234@cluster0.lajvyca.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MYSQL_CONFIG = {"host": "localhost", "user": "root", "password": "root", "database": "you_tube_db"}

#cache 
cache = TTLCache(maxsize=100, ttl=3600)

try:   #connections
    mysql_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **MYSQL_CONFIG) # why pool(Speeds up MySQL by reusing connections)
    logger.debug("MySQL pool created successfully")
except mysql.connector.Error as e:
    logger.error(f"Failed to create MySQL pool: {e}")
    mysql_pool = None

try:
    mongo_client = MongoClient(MONGODB_URI, server_api=ServerApi('1'))
    mongo_client.admin.command('ping')  # Test
    logger.debug("MongoDB connection established")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    mongo_client = None

try:
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    logger.debug("YouTube API client initialized")
except Exception as e:
    logger.error(f"Failed to initialize YouTube API: {e}")
    youtube = None

#channelID
def is_valid_channel_id(channel_id):
    pattern = r'^UC[a-zA-Z0-9_-]{22}$'
    return bool(re.match(pattern, channel_id))

# Async functions The async nature allows multiple requests to run concurrently,
#Asynchronously fetches channel details
#To fetch data without blocking other operations
async def fetch_channel_details(session, channel_id):# session: An aiohttp.ClientSession() for async HTTP requests.
    start_time = time.time()  # measure the time taken
    # start time to later calculate how long the fetch took
    if not is_valid_channel_id(channel_id):
        logger.error(f"Invalid channel ID format: {channel_id}")
        return {"error": f"Invalid channel ID: {channel_id}"}
    
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails,statistics&id={channel_id}&key={YOUTUBE_API_KEY}"
# snippet: title, description, etc.
# contentDetails: playlists (like uploads).
# statistics: subscriber and view counts.
    try:
        async with session.get(url) as response:
            data = await response.json()
            if 'error' in data:
                logger.error(f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})")
                return {"error": f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})"}
            
            channel_data = []
            for item in data.get('items', []):
                channel_data.append({
                    'channel_id': item['id'],
                    'channel_name': item['snippet']['title'],
                    'subscribe_count': int(item['statistics'].get('subscriberCount', 0)), # get or keep 0
                    'channel_views': int(item['statistics'].get('viewCount', 0)),
                    'channel_description': item['snippet'].get('description', ''),
                    'playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
                })
            if not channel_data:
# why logger It's used for logging debug info, errors, and status messages.
#Helps trace issues and logs useful debugging information
                logger.error(f"No data found for channel ID: {channel_id}")
                return {"error": f"No channel data found for ID: {channel_id}"}
            logger.debug(f"Channel fetch took {time.time() - start_time:.2f}s")
            return channel_data
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching channel: {str(e)}")
        return {"error": f"Network error: {str(e)}"}

async def fetch_playlist_details(session, channel_id):
    start_time = time.time() 
#this timer is used to measure the time taken to fetch details
    if not is_valid_channel_id(channel_id):
        logger.error(f"Invalid channel ID format: {channel_id}")
        return {"error": f"Invalid channel ID: {channel_id}"}
    
    url = f"https://www.googleapis.com/youtube/v3/playlists?part=snippet,contentDetails&channelId={channel_id}&maxResults=50&key={YOUTUBE_API_KEY}"
#part=snippet,contentDetails: request metadata and content info.
#maxResults=50: get up to 50 playlists (maximum allowed per page).
    try:
        async with session.get(url) as response:
            data = await response.json()
            if 'error' in data:
                logger.error(f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})")
                return {"error": f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})"}
            
            playlist_data = []
            for item in data.get('items', []):
                playlist_data.append({
                    'playlist_id': item['id'],
                    'channel_id': item['snippet']['channelId'],
                    'playlist_title': item['snippet']['title'],
                    'playlist_description': item['snippet'].get('description', ''),
                    'item_count': int(item['contentDetails'].get('itemCount', 0)),
                    'published_at': item['snippet'].get('publishedAt', '')
                })
            logger.debug(f"Playlist fetch took {time.time() - start_time:.2f}s")
            return playlist_data
    except aiohttp.ClientError as e:
#a network problem
        logger.error(f"Network error fetching playlists: {str(e)}")
        return {"error": f"Network error: {str(e)}"}
#time-consuming,
# async allows you to fetch many channels or playlists in parallel, saving time.
async def fetch_video_ids(session, channel_id):
# all video IDs from a channel’s uploads playlist
    start_time = time.time()  # measure the time taken
    if not is_valid_channel_id(channel_id):
        logger.error(f"Invalid channel ID format: {channel_id}")
        return {"error": f"Invalid channel ID: {channel_id}"}

    try:
        res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if not res.get('items'):
            logger.error(f"No channel found for ID: {channel_id}")
            return {"error": f"No channel found for ID: {channel_id}"}
        
        playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
#Extract the uploads playlist ID 
        video_ids = []
        next_page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={playlist_id}&maxResults=50&key={YOUTUBE_API_KEY}"
            if next_page_token:
                url += f"&pageToken={next_page_token}"
            async with session.get(url) as response:
                data = await response.json()
                if 'error' in data:
                    logger.error(f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})")
                    return {"error": f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})"}
                
                for item in data.get('items', []):
        #Extract videoId from each playlist item
                    video_ids.append(item['snippet']['resourceId']['videoId'])
                next_page_token = data.get('nextPageToken')
                if not next_page_token:
                    break
        logger.debug(f"Video IDs fetch took {time.time() - start_time:.2f}s")
        return video_ids
    except Exception as e:
        logger.error(f"Error fetching video IDs: {str(e)}")
        return {"error": f"Error fetching video IDs: {str(e)}"}

async def fetch_video_details(session, video_ids):
    start_time = time.time()
    if isinstance(video_ids, dict) and 'error' in video_ids:
        return video_ids
    
    video_list = []
    tasks = []
    for i in range(0, len(video_ids), 50):
        video_id_chunk = ','.join(video_ids[i:i+50])
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={video_id_chunk}&key={YOUTUBE_API_KEY}"
        tasks.append(session.get(url))
    
    responses = await asyncio.gather(*[task for task in tasks], return_exceptions=True)
#Collects all video detail  responses parallelly
    for response in responses:
        if isinstance(response, Exception):
            logger.error(f"Video fetch error: {response}")
            continue
#If a request raised an error (network issue, timeout), log it and skip.
        data = await response.json()
        if 'error' in data:
            logger.error(f"YouTube API error: {data['error']['message']} (code: {data['error']['code']})")
            continue
# IF YOUTUBE RETUREND error log and skip the request
        for item in data.get('items', []):
            video_list.append({
                'video_id': item['id'],
                'video_name': item['snippet'].get('title', 'No title'),
                'video_description': item['snippet'].get('description', 'No description'),
                'tags': ','.join(item['snippet'].get('tags', [])),
                'channel_name': item['snippet'].get('channelTitle', 'Unknown'),
                'channel_id': item['snippet'].get('channelId', ''),
                'published_at': item['snippet'].get('publishedAt', ''),
                'view_count': int(item['statistics'].get('viewCount', 0)),
                'like_count': int(item['statistics'].get('likeCount', 0)),
                'favorite_count': int(item['statistics'].get('favoriteCount', 0)),
                'comment_count': int(item['statistics'].get('commentCount', 0)),
                'duration': item['contentDetails'].get('duration', 'PT0S'),
                'thumbnail_url': item['snippet']['thumbnails']['default'].get('url', ''),
                'caption_status': 'True' if item['contentDetails'].get('caption', '') == 'true' else 'False'
            })
    logger.debug(f"Video details fetch took {time.time() - start_time:.2f}s")
    return video_list

async def fetch_comment_details(session, video_ids, max_comments=100):  #video comments (up to a max of 100)
    start_time = time.time()
    if isinstance(video_ids, dict) and 'error' in video_ids:
        return video_ids
    
    all_comments = []
    comment_count = 0 # count stop when max_count is reached 
    for video_id in video_ids[:10]: # limit for reduce time
        next_page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id}&maxResults=50&key={YOUTUBE_API_KEY}"
            if next_page_token:
                url += f"&pageToken={next_page_token}"
            try:
                async with session.get(url) as response:
    #session.get()	Makes async HTTP requests to YouTube.
                    data = await response.json()
                    if 'error' in data:
                        logger.error(f"YouTube API error for comments: {data['error']['message']} (code: {data['error']['code']})")
                        break
                    for item in data.get('items', []):
                        if comment_count >= max_comments:
                            break
                        all_comments.append({
                            'comment_id': item['snippet']['topLevelComment']['id'],
                            'video_id': video_id,
                            'comment_text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                            'comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_published': item['snippet']['topLevelComment']['snippet']['publishedAt']
                        })
                        comment_count += 1
                    next_page_token = data.get('nextPageToken')
                    if not next_page_token or comment_count >= max_comments:
                        break
            except aiohttp.ClientError as e:
                logger.error(f"Network error fetching comments: {str(e)}")
                break
    logger.debug(f"Comment fetch took {time.time() - start_time:.2f}s")
    return all_comments

# Async fetch wrapper
async def fetch_selected_data(channel_id, data_type):
    cache_key = f"{channel_id}_{data_type}"
    if cache_key in cache:
        logger.debug(f"Cache hit for {cache_key}")
        return cache[cache_key]
    
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        if data_type == 'channel':
#Pause execution until an asynchronous task finishes
            result = await fetch_channel_details(session, channel_id)
            data = {'channel_details': result} if not isinstance(result, dict) or 'error' not in result else result
        elif data_type == 'playlist':
            result = await fetch_playlist_details(session, channel_id)
            data = {'playlist_details': result} if not isinstance(result, dict) or 'error' not in result else result
        elif data_type == 'video':
            video_ids = await fetch_video_ids(session, channel_id)
            data = {'video_details': await fetch_video_details(session, video_ids)}
        elif data_type == 'comment':
            video_ids = await fetch_video_ids(session, channel_id)
            data = {'comment_details': await fetch_comment_details(session, video_ids)}
        elif data_type == 'all':
            video_ids = await fetch_video_ids(session, channel_id)
            tasks = [
                fetch_channel_details(session, channel_id),
                fetch_playlist_details(session, channel_id),
                fetch_video_details(session, video_ids),
                fetch_comment_details(session, video_ids)
            ] #Prepares 4 async tasks to run in parallel.
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            data = {
                'channel_details': results[0] if not isinstance(results[0], Exception) and not isinstance(results[0], dict) else [],
                'playlist_details': results[1] if not isinstance(results[1], Exception) and not isinstance(results[1], dict) else [],
                'video_details': results[2] if not isinstance(results[2], Exception) and not isinstance(results[2], dict) else [],
                'comment_details': results[3] if not isinstance(results[3], Exception) and not isinstance(results[3], dict) else []
            }
            if isinstance(results[0], dict) and 'error' in results[0]:
                data = results[0]  # Propagate channel error for type=all
        else:
            data = {'error': 'Invalid data type'}
        
        if not any(data.values()) and 'error' not in data:
            data = {'error': f"No {data_type} data found for ID: {channel_id}"}
        else:
            cache[cache_key] = data
        logger.debug(f"Total fetch for {data_type} took {time.time() - start_time:.2f}s")
        return data

def store_in_mongodb(data):  # mongodb 
    if not mongo_client:
        return {"status": "error", "message": "MongoDB not initialized"}
    start_time = time.time()
    db = mongo_client["you_tube_db"]
    for key, value in data.items():
        if value and not isinstance(value, dict):
            collection = db[key]
            channel_id = value[0].get('channel_id')
# This helps identify existing data to delete before inserting new data.
            if channel_id:
        #avoids duplicates when refreshing the data.
                collection.delete_many({'channel_id': channel_id})
            collection.insert_many(value)
    logger.debug(f"MongoDB storage took {time.time() - start_time:.2f}s")
    return {"status": "success", "message": "Data stored in MongoDB"}

def store_in_mysql(data):
    if not mysql_pool:
        return {"status": "error", "message": "MySQL pool not initialized"}
    start_time = time.time()
    conn = mysql_pool.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS you_tube_db')
        cursor.execute('USE you_tube_db')

#order of insertion ,tables with foreign keys
        table_order = ['channel_details', 'playlist_details', 'video_details', 'comment_details']
        

        for key in table_order:
            if key not in data or not data[key] or isinstance(data[key], dict):
                logger.debug(f"Skipping {key} due to no data or error")
                continue
                
            df = pd.DataFrame(data[key])# row by row insertion
            if 'published_at' in df.columns:
                df['published_at'] = pd.to_datetime(df['published_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

            if key == 'channel_details':
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS channel_details (
                        channel_id VARCHAR(50) PRIMARY KEY,
                        channel_name VARCHAR(100),
                        subscribe_count INT,
                        channel_views BIGINT,
                        channel_description TEXT,
                        playlist_id VARCHAR(50)
                    )
                """)
                values = [
                    (row['channel_id'], row['channel_name'], row['subscribe_count'],
                     row['channel_views'], row['channel_description'], row['playlist_id'])
                    for _, row in df.iterrows()
                ]
#Bulk insert using executemany() for efficiency.
#INSERT IGNORE prevents duplicate primary key erro
                cursor.executemany("""
                    INSERT IGNORE INTO channel_details 
                    (channel_id, channel_name, subscribe_count, channel_views, channel_description, playlist_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, values)
            elif key == 'playlist_details' and data.get('channel_details') and not isinstance(data['channel_details'], dict):
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS playlist_details (
                        playlist_id VARCHAR(50) PRIMARY KEY,
                        channel_id VARCHAR(50),
                        playlist_title VARCHAR(255),
                        playlist_description TEXT,
                        item_count INT,
                        published_at DATETIME,
                        FOREIGN KEY (channel_id) REFERENCES channel_details(channel_id)
                    )
                """)
                values = [
                    (row['playlist_id'], row['channel_id'], row['playlist_title'],
                     row['playlist_description'], row['item_count'], row['published_at'])
                    for _, row in df.iterrows()
                ]
                
                cursor.executemany("""
                    INSERT IGNORE INTO playlist_details 
                    (playlist_id, channel_id, playlist_title, playlist_description, item_count, published_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, values)
            elif key == 'video_details' and data.get('channel_details') and not isinstance(data['channel_details'], dict):
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS video_details (
                        video_id VARCHAR(50) PRIMARY KEY,
                        video_name VARCHAR(255),
                        video_description TEXT,
                        tags TEXT,
                        channel_name VARCHAR(100),
                        channel_id VARCHAR(50),
                        published_at DATETIME,
                        view_count BIGINT,
                        like_count INT,
                        favorite_count INT,
                        comment_count INT,
                        duration VARCHAR(20),
                        thumbnail_url VARCHAR(255),
                        caption_status VARCHAR(50),
                        FOREIGN KEY (channel_id) REFERENCES channel_details(channel_id)
                    )
                """)
                values = [
                    (row['video_id'], row['video_name'], row['video_description'], row['tags'],
                     row['channel_name'], row['channel_id'], row['published_at'], row['view_count'],
                     row['like_count'], row['favorite_count'], row['comment_count'], row['duration'],
                     row['thumbnail_url'], row['caption_status'])
                    for _, row in df.iterrows()
                ]
                cursor.executemany("""
                    INSERT IGNORE INTO video_details 
                    (video_id, video_name, video_description, tags, channel_name, channel_id,
                     published_at, view_count, like_count, favorite_count, comment_count,
                     duration, thumbnail_url, caption_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, values)
            elif key == 'comment_details' and data.get('video_details') and not isinstance(data['video_details'], dict):
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS comment_details (
                        comment_id VARCHAR(50) PRIMARY KEY,
                        video_id VARCHAR(50),
                        comment_text TEXT,
                        comment_author VARCHAR(100),
                        comment_published DATETIME,
                        FOREIGN KEY (video_id) REFERENCES video_details(video_id)
                    )
                """)
                values = [
                    (row['comment_id'], row['video_id'], row['comment_text'],
                     row['comment_author'], row['comment_published'])
                    for _, row in df.iterrows()
                ]
                cursor.executemany("""
                    INSERT IGNORE INTO comment_details 
                    (comment_id, video_id, comment_text, comment_author, comment_published)
                    VALUES (%s, %s, %s, %s, %s)
                """, values)

        conn.commit()
        logger.debug(f"MySQL storage took {time.time() - start_time:.2f}s")
        return {"status": "success", "message": "Data stored in MySQL"}
    except mysql.connector.Error as e:
        logger.error(f"MySQL storage error: {str(e)}")
        return {"status": "error", "message": f"MySQL error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected storage error: {str(e)}")
        return {"status": "error", "message": f"Unexpected error: {str(e)}"}
    finally:
        conn.close() # we need to close for pool operation 

# Endpoints
@app.route('/fetch/<channel_id>', methods=['GET'])
async def fetch_data_endpoint(channel_id):
    
    data_type = request.args.get('type', 'all')
    try:
        data = await fetch_selected_data(channel_id, data_type)
        if 'error' in data:
            return jsonify(data), 400
        return jsonify(data)
    except Exception as e:
        logger.error(f"Fetch error for {channel_id}: {str(e)}")
        return jsonify({"error": f"Failed to fetch {data_type} data: {str(e)}"}), 500

@app.route('/store/mongodb', methods=['POST']) # define post poin1
async def store_mongodb_endpoint():
#Asynchronously receives the incoming JSON data from the request body.
    data = await request.get_json()
    result = store_in_mongodb(data) # calls the store_in_mongodb function with the received data.
    return jsonify(result) #Returns a JSON response 

@app.route('/store/mysql', methods=['POST']) # define post point 2
async def store_mysql_endpoint():
    data = await request.get_json() # wait load income data
    result = store_in_mysql(data) # call the store_in_mysql function with the received data.
    return jsonify(result)

@app.route('/query', methods=['POST']) # define post point 3
async def run_query():
    query = (await request.get_json()).get('query')
    if not mysql_pool:
        return jsonify({"status": "error", "message": "MySQL pool not initialized"})
    start_time = time.time()
    conn = mysql_pool.get_connection()
    try:
        df = pd.read_sql(query, conn) # load data from mysql into pd
        logger.debug(f"Query took {time.time() - start_time:.2f}s")
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"Query error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})
    finally:
        conn.close()

if __name__ == '__main__': # start app
    # app connect all ip address and port 5000
    app.run(debug=True, host='0.0.0.0', port=5000)
