# 📺 YouTube Channel Data App (Flask-Async API Version)

This project is a backend web application built with **Quart** (an async version of Flask), designed to fetch, store, and query **YouTube Channel Data** using the **YouTube Data API**.

It extracts data like channel info, playlists, videos, and comments — and stores them in **MongoDB** (NoSQL) and **MySQL** (SQL) for querying and analysis.

---

## 🚀 Features

- 🔍 Extract YouTube channel data (channel, playlists, videos, comments)
- ⚡ Fully asynchronous API using `aiohttp` and `Quart`
- 🛢️ Store data in **MySQL** and **MongoDB**
- 📊 Run custom SQL queries via API
- 💾 Auto caching with TTL to reduce API calls
- ✅ Input validation and error handling

---

## 🛠️ Technologies Used

| Tool/Library       | Purpose                             |
|--------------------|--------------------------------------|
| **Quart**          | Async Flask-style web framework      |
| **aiohttp**        | Non-blocking API requests            |
| **Google API**     | Fetch YouTube data                   |
| **MySQL**          | Store normalized relational data     |
| **MongoDB Atlas**  | Store raw JSON-like channel data     |
| **Pandas**         | SQL querying + data formatting       |
| **Logging**        | For debugging and tracing            |

---

## 📁 Project Structure


## ⚙️ API Endpoints

| Endpoint             | Method | Description                                |
|----------------------|--------|--------------------------------------------|
| `/store/mongodb`     | POST   | Store YouTube data in MongoDB              |
| `/store/mysql`       | POST   | Store YouTube data in MySQL                |
| `/query`             | POST   | Run custom SQL query on MySQL              |


## Fetch & Store in MySQL
POST /store/mysql
{
  "channel_details": [...],
  "video_details": [...],
  ...
}
##  Query MySQL Table
POST /query
{
  "query": "SELECT channel_name, subscribe_count FROM channel_details"
}

 ## Sample SQL Queries

 -- All videos of a specific channel
SELECT * FROM video_details WHERE channel_id = 'UCxxxx';

-- Join channels with videos
SELECT c.channel_name, v.video_name
FROM channel_details c
JOIN video_details v ON c.channel_id = v.channel_id;

-- Most liked videos
SELECT video_name, like_count FROM video_details ORDER BY like_count DESC LIMIT 10;

 

