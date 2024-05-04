# Importing the required libraries

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime
import mysql.connector as sql
import googleapiclient.discovery
from PIL import Image
import isodate

# setting the image and title of the page

icon = Image.open("youtube.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By AJAY K",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Ajay K!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Scrap and Import","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "10px", 
                                                "--hover-color": "#f5d3d3"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#c80122"}})
    
# creating a connection to connect the data and database
client = sql.connect(
    host = 'localhost',
    user='root',
    password='', 
    database='youtube_data'
)
cursor = client.cursor()

# building connection with youtube-api

api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCYB3NVzW2unX71X6SLckHzxlKhAysA70A"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Get channel information using channel id

def get_channel_data(c_id):
    ch_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=c_id
    )
    response = request.execute()
    for i in range(len(response['items'])):
        data = {
            "Channel_id":c_id,
            "Channel_name":response['items'][i]['snippet']['title'],
            "Channel_description":response['items'][i]['snippet']['description'],
            "Channel_PublishedAt":response['items'][i]['snippet']['publishedAt'],
            "Channel_playlistId":response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            "Channel_subscriberCount":response['items'][i]['statistics']['subscriberCount'],
            "Channel_viewcount":response['items'][i]['statistics']['viewCount'],
            "Channel_videoCount":response['items'][i]['statistics']['videoCount']
        }
        ch_data.append(data)
    return ch_data

# function to get video ids from playlist

def get_channel_video(c_id):
    video_ids=[]
    request = youtube.channels().list(
    part="contentDetails",
    id=c_id
    )
    response = request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token)
        response = request.execute()
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
            if len(video_ids) >= 50: # remove this line to consider whole videos
                break
        next_page_token = response.get('nextPageToken')
        if next_page_token is None or len(video_ids)>=50: # remove the 35 if for considering all the videos   or len(video_ids)>=35
            break
    return video_ids

#function to get video details

def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats

# function to get comment details


def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        count = 0  # Initialize a counter for the comments
        while True:
            response = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=v_id,
                maxResults=100,  # You can request up to 100 comments per page from the API
                pageToken=next_page_token
            ).execute()

            for cmt in response['items']:
                data = {
                    'Comment_id': cmt['id'],
                    'Video_id': cmt['snippet']['videoId'],
                    'Comment_text': cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_author': cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_posted_date': cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'Like_count': cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                    'Reply_count': cmt['snippet']['totalReplyCount']
                }
                comment_data.append(data)
                count += 1
                if count >= 50:  # Check if 50 comments have been collected
                    return comment_data  # Return early if 50 comments are reached

            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except :
        pass
    return comment_data

# """CREATE TABLE if not exists channels (
#     Channel_id VARCHAR(255) PRIMARY KEY,
#     Channel_name VARCHAR(255),
#     Channel_description TEXT,
#     Channel_PublishedAt DATETIME, 
#     Channel_playlistId varchar(255),
#     Channel_subscriberCount BIGINT, 
#     Channel_viewcount BIGINT, 
#     Channel_videoCount INT
# );

# CREATE TABLE if not exists videos (
#     Video_id VARCHAR(255) PRIMARY KEY,
#     Video_name varchar(255),
#     Channel_id VARCHAR(255),
#     Title VARCHAR(255),
#     Description TEXT,
#     Published_date DATETIME,
#     Duration VARCHAR(255),
#     Views BIGINT,
#     Likes BIGINT,
#     Comments BIGINT,
#     Thumbnail VARCHAR(255),
#     Tags TEXT,
#     Favorite_count BIGINT,
#     Definition VARCHAR(255),
#     Caption_status VARCHAR(255),
#     FOREIGN KEY (Channel_id) REFERENCES channels(Channel_id)
# );

# CREATE TABLE if not exists comments (
#     Comment_id VARCHAR(255) PRIMARY KEY,
#     Video_id VARCHAR(255),
#     Comment_text TEXT,
#     Comment_author VARCHAR(255),
#     Comment_posted_date DATETIME,
#     Like_count BIGINT,
#     Reply_count INT,
#     FOREIGN KEY (Video_id) REFERENCES videos(Video_id)
# );
# """
 

# Home Page

if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    st.title("Youtube Data Harvesting and Warehousing")
    st.markdown("""
    ## Domain: Social Media
    ## Technologies Used: Python, MySQL, YouTube Data API, Streamlit
    ## Overview: 
    This application retrieves YouTube channel data via the Google API, stores it in a MySQL database, 
    and displays queries within this Streamlit app.
    """)
    col2.markdown("#   ")
    col2.image("Utube1.png")

def insert_channel_details(channel_details):
    # st.write(channel_details)
    VALUES = [(row['Channel_id'][0],row['Channel_name'],row['Channel_description'],row['Channel_PublishedAt'],
    row['Channel_playlistId'],row['Channel_subscriberCount'],row['Channel_viewcount'],
    row['Channel_videoCount'])for row in channel_details]
    insert_query ="""
    INSERT INTO channels (Channel_id, Channel_name, Channel_description, Channel_PublishedAt, Channel_playlistId, Channel_subscriberCount, Channel_viewcount,Channel_videoCount)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    # st.write(VALUES)
    cursor.execute(insert_query, VALUES[0])
    #client.commit()

def insert_video_details(video_details):
    VALUES = [(row['Video_id'],row['Channel_name'],row['Channel_id'],
               row['Title'],row['Description'],row['Published_date'],row['Duration'],
               row['Views'],row['Likes'],row['Comments'],row['Thumbnail'],row['Tags'],
               row['Favorite_count'],row['Definition'],row['Caption_status']) for row in video_details] 
    insert_query ="""
    INSERT INTO videos (Video_id,Channel_name, Channel_id, Title, Description, Published_date, Duration, Views, Likes, Comments, Thumbnail, Tags, Favorite_count, Definition, Caption_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(insert_query, VALUES)
    #client.commit()

def insert_comment_details(comment_details):
    VALUES = [(row['Comment_id'],row['Video_id'],row['Comment_text'],row['Comment_author'],
               row['Comment_posted_date'],row['Like_count'],
               row['Reply_count'])for row in comment_details] 
    insert_query = """
    INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date, Like_count, Reply_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(insert_query, VALUES)
    client.commit()

def preprocess_Channel(ch_details): 
    pd=[]
    for i in ch_details:
        d = i['Channel_PublishedAt']
        od = datetime.fromisoformat(d.replace('Z','+00:00'))
        pr={ 
            'Channel_id': i['Channel_id'],
            'Channel_name': i['Channel_name'], 
            'Channel_description': i['Channel_description'],
            'Channel_PublishedAt': od,
            'Channel_playlistId': i['Channel_playlistId'],
            'Channel_subscriberCount': int(i['Channel_subscriberCount']) if i['Channel_subscriberCount'] is not None else 0, 
            'Channel_viewcount': int(i['Channel_viewcount']) if i['Channel_viewcount'] is not None else 0, 
            'Channel_videoCount': int(i['Channel_videoCount']) if i['Channel_videoCount'] is not None else 0
        }
        pd.append(pr)
    return pd
def pre_process_videos(v_det): 
    pd = []
    for i in v_det: 
        d = i['Published_date']
        od = datetime.fromisoformat(d.replace('Z','+00:00'))
        tags = ', '.join(i['Tags']) if i['Tags'] else None
        pub = i['Duration']
        a=isodate.parse_duration(pub)
        dur = str(a)
        pr={
            'Video_id': i['Video_id'],
            'Channel_name': i['Channel_name'], 
            'Channel_id': i['Channel_id'], 
            'Title': i['Title'], 
            'Description': i['Description'], 
            'Published_date': od,
            'Duration': dur, 
            'Views': int(i['Views']) if i['Views'] is not None else 0, 
            'Likes' : int(i['Likes']) if i['Likes'] is not None else 0,
            'Comments': int(i['Comments']) if i['Comments'] is not None else 0,
            'Thumbnail': i['Thumbnail'], 
            'Tags': tags, 
            'Favorite_count': int(i['Favorite_count']) if i['Favorite_count'] is not None else 0, 
            'Definition': i['Definition'], 
            'Caption_status' : i['Caption_status']
        }
        pd.append(pr)
    return pd 

def pre_process_comments(c_det): 
    pd = []
    for i in c_det: 
        d = i['Comment_posted_date']
        od = datetime.fromisoformat(d.replace('Z','+00:00'))
        pr={
            'Comment_id': i['Comment_id'], 
            'Video_id': i['Video_id'], 
            'Comment_text': i['Comment_text'], 
            'Comment_author': i['Comment_author'], 
            'Comment_posted_date': od,
            'Like_count': int(i['Like_count']) if i['Like_count'] is not None else 0, 
            'Reply_count':int(i['Reply_count']) if i['Reply_count'] is not None else 0
        }
        pd.append(pr)
    return pd 


if selected == "Scrap and Import":
    tabs = st.tabs(["🧠Scrap and Import"])
    tab1 = tabs[0]
    with tab1:
        st.write("## :orange[Scrap and Import]")
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_data(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)
        if st.button("Upload to MySQL"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_data(ch_id)
                pre_pro_ch = preprocess_Channel(ch_details)

                v_ids = get_channel_video(ch_id)
                vid_details = get_video_details(v_ids)
                pre_pro_vid = pre_process_videos(vid_details)

                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()
                pre_pro_com = pre_process_comments(comm_details)

            # try:
            insert_channel_details(pre_pro_ch)
            insert_video_details(pre_pro_vid)
            insert_comment_details(pre_pro_com)
            st.success("Transformation to MySQL Successful!!!")
            # except Exception as e:
            #     st.error(f"An error occurred: {e}")


if selected == "View":
    
    st.write("## :blue[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT Channel_name 
        AS Channel_Name, Channel_videoCount AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        # st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        # fig = px.bar(df,
        #              x=cursor.column_names[0],
        #              y=cursor.column_names[1],
        #              orientation='v',
        #              color=cursor.column_names[0]
        #             )
        # st.plotly_chart(fig,use_container_width=True) 

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        # st.write("### :green[Top 10 most viewed videos :]")
        # fig = px.bar(df,
        #              x=cursor.column_names[2],
        #              y=cursor.column_names[1],
        #              orientation='h',
        #              color=cursor.column_names[0]
        #             )
        # st.plotly_chart(fig,use_container_width=True)
    
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
    
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        # st.write("### :green[Top 10 most liked videos :]")
        # fig = px.bar(df,
        #              x=cursor.column_names[2],
        #              y=cursor.column_names[1],
        #              orientation='h',
        #              color=cursor.column_names[0]
        #             )
        # st.plotly_chart(fig,use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name, Channel_viewcount AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        # st.write("### :green[Channels vs Views :]")
        # fig = px.bar(df,
        #              x=cursor.column_names[0],
        #              y=cursor.column_names[1],
        #              orientation='v',
        #              color=cursor.column_names[0]
        #             )
        # st.plotly_chart(fig,use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM videos
                        ) AS subquery
                        GROUP BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names
                          )
        st.write(df)
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        # st.write("### :green[Videos with most comments :]")
        # fig = px.bar(df,
        #              x=cursor.column_names[1],
        #              y=cursor.column_names[2],
        #              orientation='v',
        #              color=cursor.column_names[0]
        #             )
        # st.plotly_chart(fig,use_container_width=True)