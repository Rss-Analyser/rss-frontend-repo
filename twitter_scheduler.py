import streamlit as st
import tweepy
from io import BytesIO
import requests
import time
import datetime
import uuid
from supabase import create_client, Client
import json
import psycopg2
import uuid


import base64

# Supabase setup
SUPABASE_URL = st.secrets["database"]["url"]
SUPABASE_API_KEY = st.secrets["database"]["api_key"]

ANALYSIS_COLUMN_NAME = "stocktraderanalysis"
ANALYSIS_COLUMN_NAME = ANALYSIS_COLUMN_NAME+"_ANALYSIS"
TWEET_COLUMN_NAME = ANALYSIS_COLUMN_NAME+"_GENERATED_TWEET"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def get_twitter_conn_v1(api_key, api_secret, access_token, access_token_secret) -> tweepy.API:
    """Get twitter conn 1.1"""
    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth)

def get_twitter_conn_v2(api_key, api_secret, access_token, access_token_secret) -> tweepy.Client:
    """Get twitter conn 2.0"""
    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )
    return client

st.title("Post a Tweet")

# Input fields for Twitter API credentials with unique keys
api_key = st.text_input("API Key", value=st.session_state.get("api_key", ""), key="api_key_input")
api_secret = st.text_input("API Secret", value=st.session_state.get("api_secret", ""), key="api_secret_input")
access_token = st.text_input("Access Token", value=st.session_state.get("access_token", ""), key="access_token_input")
access_token_secret = st.text_input("Access Token Secret", value=st.session_state.get("access_token_secret", ""), key="access_token_secret_input")

# Button to save API data
if st.button("Save API Data"):
    unique_id = str(uuid.uuid4())
    response = supabase.table('api_data').insert({
        'uuid': unique_id,
        'api_key': api_key,
        'api_secret': api_secret,
        'access_token': access_token,
        'access_token_secret': access_token_secret
    }).execute()
    if response != None:
        st.write(f"Error saving API data: {response.error}")
    else:
        st.write(f"Your API data has been saved with UUID: {unique_id}")

# Input field to load API data using UUID with a unique key
input_uuid = st.text_input("Enter UUID to load API data:", key="uuid_input")

if st.button("Load API Data"):
    response = supabase.table('api_data').select("*").eq('uuid', input_uuid).execute()
    data = response.data[0] if response.data else None
    if data:
        st.session_state["api_key"], st.session_state["api_secret"], st.session_state["access_token"], st.session_state["access_token_secret"] = data["api_key"], data["api_secret"], data["access_token"], data["access_token_secret"]
        st.write("API data loaded successfully!")
    else:
        st.write("No API data found for the provided UUID.")

# Dynamic input fields for chunks
num_of_chunks = st.number_input("Number of Tweet Chunks", min_value=1, max_value=25, step=1)
chunks = []

for i in range(num_of_chunks):
    chunk = st.text_area(f"Tweet Chunk {i+1}", key=f"chunk_{i}")
    
    # Check if chunk exceeds the 280 characters limit
    if len(chunk) > 280:
        # Split chunk by \n\n
        sub_chunks = chunk.split("\n\n")
        
        # Check each sub_chunk's length
        for sub_chunk in sub_chunks:
            if len(sub_chunk) <= 280:
                chunks.append(sub_chunk)
            else:
                st.warning(f"Chunk {i+1} contains a section that's too long! Please break it down further.")
                break
    else:
        chunks.append(chunk)
    
    st.write(f"Character count: {len(chunk)}")

# Image upload
image = st.file_uploader("Upload an image", type=['png', 'jpg', 'jpeg'])


if st.button("Send"):
    if api_key and api_secret and access_token and access_token_secret and chunks:
        def post_tweet_logic():
            client_v1 = get_twitter_conn_v1(api_key, api_secret, access_token, access_token_secret)
            client_v2 = get_twitter_conn_v2(api_key, api_secret, access_token, access_token_secret)

            # If there's an image, upload it
            media_id = None
            if image:
                image_bytes = image.getvalue()
                media = client_v1.media_upload(filename="image.png", file=BytesIO(image_bytes))
                media_id = media.media_id

            # Post the first chunk (with or without image)
            tweet = client_v2.create_tweet(text=chunks[0], media_ids=[media_id] if media_id else None)

            # Post the subsequent chunks as replies
            for reply_content in chunks[1:]:
                if reply_content:  # Ensure the reply_content is not an empty string
                    time.sleep(1.0)  # Pause for a second between tweets to avoid hitting rate limits
                    tweet = client_v2.create_tweet(text=reply_content, in_reply_to_tweet_id=tweet.data["id"])

            st.write("Tweets posted successfully!")

        post_tweet_logic()
    else:
        st.write("Please fill all the necessary fields.")


# Get the selected date and time from the user
selected_date = st.date_input("Select a date for the tweet:")
selected_time = st.time_input("Select a time for the tweet:")

# Combine date and time into a datetime object
scheduled_datetime = datetime.datetime.combine(selected_date, selected_time)

# Supabase related code for scheduling tweets
if st.button("Schedule"):
    image_data = None
    if image:
        image_data = base64.b64encode(image.getvalue()).decode('utf-8')  # Encode the bytes to base64 string

    combined_content = '\n'.join(chunks)

    # Check if content is empty or not
    if not combined_content.strip():
        st.write("Cannot schedule an empty tweet!")
        st.stop()

    # Insert the data into Supabase (including the UUID)
    response = supabase.table('tweets').insert({
        'uuid': input_uuid,  # <-- This is the new line to insert UUID
        'api_key': api_key,
        'api_secret': api_secret,
        'access_token': access_token,
        'access_token_secret': access_token_secret,
        'content': combined_content,
        'scheduled_time': scheduled_datetime.strftime('%Y-%m-%d %H:%M'),
        'image': image_data
    }).execute()

    tweet_id = response.data[0]['id']
    for chunk in chunks:
        chunk_response = supabase.table('tweet_chunks').insert({
            'tweet_id': tweet_id,
            'content': chunk
        }).execute()


    # print(str(response))

    if response != None:
        st.write("Tweet scheduled successfully!")
    else:
        st.write(f"Error scheduling tweet: {response}")


def fetch_generated_tweets_from_db(db_params):
    try:
        # Connect to the database
        conn = psycopg2.connect(db_params)
        cursor = conn.cursor()
        
        # Fetch table names with the prefix "rss_entries_"
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries';")
        tables = cursor.fetchall()

        # List to store tweets, links, titles, published dates, publishers, and reasons
        tweets_data = []

        for table in tables:
            table_name = table[0]
            
            # Fetch generated_tweet, link, title, published_date, publisher, and ANALYSIS_COLUMN_NAME
            cursor.execute(f"SELECT title, link, \"{TWEET_COLUMN_NAME}\", published, publisher, \"{ANALYSIS_COLUMN_NAME}\" FROM {table_name} WHERE \"{TWEET_COLUMN_NAME}\" IS NOT NULL;")
            entries = cursor.fetchall()

            for entry in entries:
                title, link, tweet, published_date, publisher, analysis_data = entry

                # Extract reason from the analysis_data
                try:
                    outer_json = json.loads(analysis_data)
                    inner_json_str = outer_json.get("content", "{}")
                    inner_json = json.loads(inner_json_str)
                    reason = inner_json.get("Reason", "Unknown Reason")
                except:
                    reason = "Error decoding reason"

                tweets_data.append({
                    "title": title,
                    "link": link,
                    "tweet": tweet,
                    "published": published_date,
                    "publisher": publisher,
                    "reason": reason
                })

        cursor.close()
        conn.close()
        
        return tweets_data

    except Exception as e:
        print(f"Failed to fetch tweets from database. Error: {e}")
        return []

    
    # Streamlit UI
st.title("Generated Tweets from Database")

db_params = st.secrets["cockroachdb"]["connection_string"]
tweets_data = fetch_generated_tweets_from_db(db_params)

def post_instant_tweet(api_key, api_secret, access_token, access_token_secret, content, link):
    try:
        client_v1 = get_twitter_conn_v1(api_key, api_secret, access_token, access_token_secret)
        client_v2 = get_twitter_conn_v2(api_key, api_secret, access_token, access_token_secret)

        # Post the main content
        tweet = client_v2.create_tweet(text=content)
        
        # Pause for a second to avoid hitting rate limits
        time.sleep(1.0)

        # Post the link as a reply
        client_v2.create_tweet(text=link, in_reply_to_tweet_id=tweet.data["id"])

        return True, "Tweet and reply posted successfully!"
    except Exception as e:
        return False, f"Error posting tweet: {str(e)}"


# Assuming the 'published_date' is a field in the 'tweets_data' returned by the database
# If it's named differently, please adjust accordingly

# Sort tweets_data by 'published' in descending order
tweets_data = sorted(tweets_data, key=lambda x: x.get('published', ''), reverse=True)

for data in tweets_data:
    tweet_content = data['tweet'].strip('"')
    tweet_link = data['link']
    tweet_title = data['title']
    tweet_published_date = data.get('published', 'Unknown Date')

    # Extract publisher and reason from the data
    tweet_publisher = data.get('publisher', 'Unknown Publisher')
    tweet_reason = data.get('reason', 'Unknown Reason')

    col1, col2, col3 = st.columns(3)

    with col1:
        # Calculate text area height based on number of lines in tweet content
        estimated_lines = len(tweet_content) // 50 + 1
        text_area_height = max(3, estimated_lines) * 50  # Estimate each line to be 25 pixels in height

        edited_tweet = st.text_area("Edit Tweet:", value=tweet_content, height=int(text_area_height), key=f"edit_tweet_{uuid.uuid4()}")

        st.write(f"Characters: {len(edited_tweet)}/280")
        st.write(f"Link: {tweet_link}")
        st.write(f"Title: {tweet_title}")
        st.write(f"Published Date: {tweet_published_date}")
        st.write(f"Publisher: {tweet_publisher}")
        st.write(f"Reason: {tweet_reason}")

    with col2:
        if st.button(f"Post {hash(tweet_content)}", key=f"post_button_{uuid.uuid4()}"):
            # Post the tweet
            client_v1 = get_twitter_conn_v1(api_key, api_secret, access_token, access_token_secret)
            client_v2 = get_twitter_conn_v2(api_key, api_secret, access_token, access_token_secret)

            # Post the edited tweet content
            tweet = client_v2.create_tweet(text=edited_tweet)
            
            # Post the link as a reply
            time.sleep(1.0)  # Pause to avoid rate limits
            client_v2.create_tweet(text=tweet_link, in_reply_to_tweet_id=tweet.data["id"])

            st.success("Tweet and reply posted successfully!")

    st.write("-----")


st.title("Scheduled Tweets for UUID")

# Displaying scheduled tweets
input_uuid = st.text_input("Enter UUID to view scheduled tweets:")

if input_uuid:
    response = supabase.table('tweets').select("*").eq('uuid', input_uuid).execute()
    if response.data:
        scheduled_tweets = sorted(response.data, key=lambda x: x['scheduled_time'])
    else:
        scheduled_tweets = []

    scheduled_tweets = response.data

    # Display each scheduled tweet in a container
    for tweet in scheduled_tweets:
        tweet_id = tweet['id']
        api_key = tweet['api_key']
        api_secret = tweet['api_secret']
        access_token = tweet['access_token']
        access_token_secret = tweet['access_token_secret']
        content = tweet['content']
        scheduled_time = tweet['scheduled_time']
        image_data = tweet['image']

        col1, col2, col3 = st.columns([3, 1, 1])  # Using 3 columns: one for content, one for scheduled time, one for the delete button

        with col1:
            st.text(content)  # Display the tweet content

        with col2:
            st.text(scheduled_time)  # Display the scheduled time

        with col3:
            # Button to delete the tweet
            if st.button(f"Delete {tweet_id}"):
                delete_response = supabase.table('tweets').delete().eq('id', tweet_id).execute()
                supabase.table('tweet_chunks').delete().eq('tweet_id', tweet_id).execute() # Delete associated chunks
                
                if delete_response.error:
                    st.write(f"Error deleting tweet {tweet_id}: {delete_response.error}")
                else:
                    st.success(f"Tweet {tweet_id} deleted!")

