# YouTube_Data_Harvesting
Scrapping the data from youtube with the help of youtube_api then storing the data in mysql and processing the given queries.
# Project title:
# YouTube Data Harvesting and Warehousing using SQL and Streamlit
Skills used are : Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL  
# Problem Statement
The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:
1.	  Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
2.	 Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
3.	 Option to store the data in a MYSQL or PostgreSQL.
4.	Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details
# Steps followed to complete this project
1. Generate the youtube api key from google developers
2. Get the request and response for the channels,videos, playlistitems and comments code
3. Open a new python file
4. import the necessary libraries
5. establish the connection for MySQL
6. Firstly using the api key get Channel_details, Video_list, Video_details and Comment_details
7. Secondly create 3 tables for Channel, video and comments
8. Thirdly to insert values into the table, before we need to preprocess and convert it to usable format
9. Fourthly, convert the list type value to mysql values
10. Finally, Insert the values to mysql
11. Next, 10 mysql questions were give for that we need to write query to access the correct solution
12. All the above process are under streamlit, we are using streamlit for the front end part for user Interface to attract user and to provide a user friendly interface to the user.
13. Analyse the data and present the data.
14. 
