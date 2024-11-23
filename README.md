# YouTube Channel Data App 📺

This project is a **Streamlit-based web application** that leverages the **Google YouTube Data API** to extract, store, and visualize data about YouTube channels. Users can search for channel details, view insights, and join multiple tables for deeper analysis.

---

## 🚀 Features
1. Extract information from YouTube channels using the **Google API**.
2. Store data in an **SQL database** for efficient retrieval.
3. Search channel details and perform SQL joins in the Streamlit interface.

---

## 🛠️ Technologies Used
- **Python**: Core programming language.
- **Streamlit**: Web application framework.
- **Google API**: To fetch YouTube channel details.
- **MySQL**: Database for storing channel data.
- **Pandas**: Data manipulation and analysis.

---

## 📂 Project Structure



📊 Usage
Channel Search: Input a YouTube channel ID to fetch its details.
SQL Joins: Select tables and perform joins to view combined data.

📌 Example Queries
1) SELECT * FROM channel_details WHERE channel_id = 'UCxxxx';
2) SELECT c.channel_name, v.video_title
FROM channel_details c
JOIN video_details v ON c.channel_id = v.channel_id;

🌟 Acknowledgements
* Streamlit
* Google YouTube API
* MySQL
