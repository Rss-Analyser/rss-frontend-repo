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
from sklearn.metrics.pairwise import cosine_similarity
import random
import ast
import base64
import numpy as np

def load_config():
    with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
        return json.load(file)
    
config_data = load_config()    
params = config_data["params"]
suffixes = config_data["suffixes"]

# Supabase setup
SUPABASE_URL = st.secrets["database"]["url"]
SUPABASE_API_KEY = st.secrets["database"]["api_key"]

# Apply suffixes
ANALYSIS_COLUMN_NAME = config_data["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config_data["suffixes"]["ANALYSIS_COLUMN_NAME"]
TWEET_COLUMN_NAME = config_data["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config_data["suffixes"]["TWEET_COLUMN_NAME"]

print(ANALYSIS_COLUMN_NAME)
print(TWEET_COLUMN_NAME)

DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]

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

def send_tweet_chunks(api_key, api_secret, access_token, access_token_secret, chunks, image=None):
    """Sends a series of tweets, with the first potentially containing an image, and subsequent ones as replies."""
    
    if not (api_key and api_secret and access_token and access_token_secret and chunks):
        return "Please fill all the necessary fields."

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

    return "Tweets posted successfully!"

def fetch_api_data(input_uuid):
    """Fetches API data from the Supabase table based on the given UUID."""
    
    response = supabase.table('api_data').select("*").eq('uuid', input_uuid).execute()
    data = response.data[0] if response.data else None
    if data:
        return {
            "api_key": data["api_key"],
            "api_secret": data["api_secret"],
            "access_token": data["access_token"],
            "access_token_secret": data["access_token_secret"]
        }
    else:
        return None
    

def schedule_tweet(input_uuid, api_key, api_secret, access_token, access_token_secret, chunks, scheduled_datetime, image=None):
    """Schedule a tweet using Supabase."""
    
    image_data = None
    if image:
        image_data = base64.b64encode(image.getvalue()).decode('utf-8')  # Encode the bytes to base64 string

    combined_content = '\n'.join(chunks)

    # Check if content is empty or not
    if not combined_content.strip():
        return "Cannot schedule an empty tweet!", None

    # Insert the data into Supabase (including the UUID)
    response = supabase.table('tweets').insert({
        'uuid': input_uuid,
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
        supabase.table('tweet_chunks').insert({
            'tweet_id': tweet_id,
            'content': chunk
        }).execute()

    if response:
        return "Tweet scheduled successfully!", tweet_id
    else:
        return f"Error scheduling tweet: {response}", None
    

    # Modify fetch_generated_tweets_from_db function to include filtering
def fetch_generated_tweets_from_db(db_params, days_back, selected_class):
    try:
        # Connect to the database
        conn = psycopg2.connect(db_params)
        cursor = conn.cursor()

        # Calculate the date x days back from today
        past_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # Fetch table names with the prefix "rss_entries_"
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries';")
        tables = cursor.fetchall()

        # List to store tweets, links, titles, published dates, publishers, and reasons
        tweets_data = []

        for table in tables:
            table_name = table[0]
            
            # Construct WHERE clause for date filtering and optional class filtering
            where_clauses = [f"published >= '{past_date}'"]
            if selected_class != "All Classes":
                where_clauses.append(f"class = '{selected_class}'")

            where_clause = " AND ".join(where_clauses)

            # Fetch generated_tweet, link, title, published_date, publisher, class, and ANALYSIS_COLUMN_NAME
            cursor.execute(f"SELECT title, link, class, \"{TWEET_COLUMN_NAME}\", published, publisher, \"{ANALYSIS_COLUMN_NAME}\", embedding FROM {table_name} WHERE {where_clause};")
            entries = cursor.fetchall()

            for entry in entries:
                title, link, tweet_class, tweet, published_date, publisher, analysis_data, embedding = entry


                if tweet != None and tweet != "No content due to empty article content.":

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
                        "class": tweet_class,
                        "tweet": tweet,
                        "published": published_date,
                        "publisher": publisher,
                        "reason": reason,
                        "embedding": embedding
                    })

        cursor.close()
        conn.close()
        
        return tweets_data

    except Exception as e:
        print(f"Failed to fetch tweets from database. Error: {e}")
        return []
    

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
    
def fetch_scheduled_tweets(input_uuid):
    """Fetch scheduled tweets from the Supabase."""
    response = supabase.table('tweets').select("*").eq('uuid', input_uuid).execute()
    return sorted(response.data, key=lambda x: x['scheduled_time']) if response.data else []

def delete_scheduled_tweet(tweet_id):
    """Delete the scheduled tweet and its associated chunks."""
    delete_response = supabase.table('tweets').delete().eq('id', tweet_id).execute()
    supabase.table('tweet_chunks').delete().eq('tweet_id', tweet_id).execute()
    return delete_response

def str_to_list(embedding_str):
    """Convert a string representation of a list to an actual list of floats."""
    try:
        return [float(x) for x in ast.literal_eval(embedding_str)]
    except (ValueError, SyntaxError):
        raise ValueError("Invalid string format for embedding")
    
def calculate_cosine_similarity(embedding1, embedding2):
    """Calculate cosine similarity between two embeddings using sklearn."""
    # Convert embeddings to numpy arrays if they aren't already
    embedding1 = np.array(embedding1)
    embedding2 = np.array(embedding2)
    return cosine_similarity([embedding1], [embedding2])[0][0]

def is_similar(embedding1_str, embedding2_str, threshold=0.8):
    """Check if two embeddings are similar based on a threshold."""
    embedding1 = str_to_list(embedding1_str)
    embedding2 = str_to_list(embedding2_str)
    similarity = calculate_cosine_similarity(embedding1, embedding2)
    return similarity > threshold

def schedule_tweets_over_day(tweets_data, input_uuid, api_data, db_params, hour_start=0, hour_end=24):
    """Schedule tweets over the day."""
    
    # Store the embeddings of already scheduled tweets
    scheduled_embeddings = []

    # Calculate time intervals
    total_tweets = len(tweets_data)
    interval_in_seconds = ((hour_end - hour_start) * 3600) // total_tweets
    
    # Start scheduling from the current time, not the beginning of the day
    current_time = datetime.datetime.now()

    conn = psycopg2.connect(db_params)
    cursor = conn.cursor()

    # Check if the 'scheduled' column exists
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='rss_entries' AND column_name='scheduled';
    """)
    column_exists = cursor.fetchone()

    # If 'scheduled' column doesn't exist, create it
    if not column_exists:
        cursor.execute("ALTER TABLE rss_entries ADD COLUMN scheduled BOOLEAN DEFAULT FALSE;")
        conn.commit()

    for tweet_data in tweets_data:
        tweet_embedding = tweet_data["embedding"]
        link = tweet_data["link"]

        # Check if the tweet is already scheduled
        cursor.execute("SELECT scheduled FROM rss_entries WHERE link = %s;", (link,))
        is_scheduled = cursor.fetchone()
        if is_scheduled and is_scheduled[0]:
            print(f"Tweet '{tweet_data['title']}' is already scheduled.")
            continue
        
        # Check if current tweet's embedding is similar to any already scheduled tweet
        similar_scheduled = [se for se in scheduled_embeddings if is_similar(tweet_embedding, se)]
        
        if similar_scheduled:
            print(f"Tweet '{tweet_data['title']}' is similar to already scheduled tweets.")
            continue

        tweet = tweet_data["tweet"].strip('"')
        if len(tweet) > 280:
            chunks = tweet.split('\\n\\n')
            # chunks = [chunk.lstrip('\n\n') for chunk in chunks]  # Remove any leading '\n' characters from each chunk
        else:
            chunks = [tweet]

        chunks = [chunk for chunk in chunks if chunk.strip()]

        # Check each chunk for length > 280 and split if necessary
        final_chunks = []
        for chunk in chunks:
            while len(chunk) > 280:
                # Find the nearest whitespace before the 280-character limit
                split_index = chunk.rfind(' ', 0, 280)
                
                # If we can't find a whitespace, split at 280 characters anyway
                split_index = split_index if split_index != -1 else 280
                
                final_chunks.append(chunk[:split_index])
                chunk = chunk[split_index:].lstrip()  # Remove leading whitespace from the remaining chunk

            final_chunks.append(chunk)

        chunks = final_chunks

        # Always add the link as an extra chunk to be posted as a reply.
        chunks.append(link)

        print(chunks)

        # Randomly schedule a tweet within the interval to add randomness
        random_seconds = random.randint(0, interval_in_seconds - 1)
        scheduled_datetime = current_time + datetime.timedelta(seconds=random_seconds)
        
        # Schedule the tweet using the provided function
        schedule_tweet(input_uuid, api_data["api_key"], api_data["api_secret"], api_data["access_token"], 
                       api_data["access_token_secret"], chunks, scheduled_datetime)
        
        # Move to the next interval
        current_time += datetime.timedelta(seconds=interval_in_seconds)
        
        # Add the embedding of the scheduled tweet to the list
        scheduled_embeddings.append(tweet_embedding)

        # If scheduled time exceeds the hour_end, reset
        if current_time.hour >= hour_end:
            current_time = datetime.datetime.now()  # Reset to the current time

    cursor.close()
    conn.close()

def mark_scheduled_in_db(tweets_data, db_params):
    """Mark tweets as scheduled in CockroachDB."""
    try:
        conn = psycopg2.connect(db_params)
        cursor = conn.cursor()

        # Check if the 'scheduled' column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='rss_entries' AND column_name='scheduled';
        """)
        column_exists = cursor.fetchone()

        # If 'scheduled' column doesn't exist, create it
        if not column_exists:
            cursor.execute("ALTER TABLE rss_entries ADD COLUMN scheduled BOOLEAN DEFAULT FALSE;")
            conn.commit()

        # Update the 'scheduled' column for each tweet
        for tweet_data in tweets_data:
            link = tweet_data["link"]
            cursor.execute(f"UPDATE rss_entries SET scheduled = TRUE WHERE link = %s;", (link,))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to mark tweets as scheduled in database. Error: {e}")



# tweets_data = fetch_generated_tweets_from_db(db_params=DATABASE_PATH, days_back=30, selected_class="All Classes")
if __name__ == "__main__":
    
    input_uuid = "6722db1e-eb96-4651-96d7-8c064151541d"
    api_data = fetch_api_data(input_uuid)
    print(api_data)

    # Fetch the tweets
    tweets_data = fetch_generated_tweets_from_db(DATABASE_PATH, 1, "All Classes")

    for tweet in tweets_data:
        print("--------------")
        print(tweet["tweet"])
        print(tweet["published"])
        print("--------------")

    # Schedule the tweets over the day
    schedule_tweets_over_day(tweets_data, input_uuid, api_data, DATABASE_PATH)

    # Mark tweets as scheduled in CockroachDB
    mark_scheduled_in_db(tweets_data, DATABASE_PATH)

    print("Done scheduling tweets.")

