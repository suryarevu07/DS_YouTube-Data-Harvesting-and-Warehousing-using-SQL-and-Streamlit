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

