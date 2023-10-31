# import streamlit as st
# import requests

# API_URL = "https://f3u0thkf9idjsh1r.us-east-1.aws.endpoints.huggingface.cloud"

# token = st.secrets["huggingfaces"]["token"]
# headers = {"Authorization": token}

# def query(payload):
# 	response = requests.post(API_URL, headers=headers, json=payload)
# 	return response.json()
	
# output = query({
# 	"inputs": """Context: You are a solopreneur a solopreneur is a person who sets up a business, 
#     of which they are the sole employee. A solopreneur is both the owner and the workforce of their business. 
#     They are responsible for organizing, managing, and assuming the risks of their enterprise, without the help of a partner.\n
#     You are generating tweets which need to be as controversial as possible.\n
#     Return a tweet written in a simple language as if you are a human.\n
#     Try to extract the most important info from the article text for your context.\n
#     Important: Use maximum 280 characters. 
    
    
#     Text: With 🤗 Inference Endpoints, easily deploy Transformers, 
#     Diffusers or any model on dedicated, fully managed infrastructure. Keep your costs low with our secure, compliant and flexible production solution.

#     Return only the tweet in a Json with structure: {"tweet":"tweet_content"}
    
#     """,


#     "parameters": {"temperature": 0.1, "max_new_tokens": 100}
# })

# print(output)


# tweet = "Qualcomm's new Snapdragon X Elite chip is a game changer, promising the fastest CPU for a laptop! 🚀 Even more impressive, it outperforms Intel chips with double the power yet consumes the same amount of energy. 💪 \n\nThe chip's boosted AI processing hints at a future where laptops can run advanced AI models locally. This leap in tech could trigger the next big evolution in PCs as we know them. Better power, better performance, right in your hands! 🌐🔝🔜#AI #TechInnovation #Qualcomm"
# chunks = tweet.split('\n\n')

# print(chunks)

# tweet = """Discover the latest features of Android 14! 📱 

# 1) Create custom #EmojiWallpapers for a fun personal touch. 🌈

# 2) Gain more control over #PhotoPermissions. Allow access to selective photos and videos. 📸

# Stay tuned for more Android 14 highlights! #AI #TechUpdates

# \n\nMore reasons to love Android 14! 🚀

# 3) Unleashing your creative side with #LockScreenCustomization. Explore different clock styles, colors, and shortcut buttons. ⏲️

# 4) Never miss a notification with #NotificationFlashes lighting up your screen or camera flash. 📣

# #Android14

# \n\nExperience regional personalization with Android 14. 🌍

# 5) Set your preferred measurement system across the entire platform with the new #RegionalPreferences feature. Customization at its finest! 

# Stay ahead with the Android update. #AI #Innovation #TechNews"""

# chunks = tweet.split('\n\n')

# print(chunks)


# import feedparser
# rss_link = "https://www.naharnet.com/tags/climate-change-environment/en/feed.atom"
# content = feedparser.parse(rss_link)

# print(content.entries)

import json
import streamlit as st

import psycopg2

def fetch_tweets_from_db(connection_string, column_name):
    # Create a connection to the database
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    # Query to fetch tweets using the provided column name
    query = f"SELECT \"{column_name}\" FROM rss_entries WHERE \"{column_name}\" IS NOT NULL"

    cursor.execute(query)
    tweets = [row[0] for row in cursor.fetchall()]

    # Close the database connection
    cursor.close()
    conn.close()

    return tweets

def analyze_tweets(tweets):
    for tweet in tweets:
        char_count = len(tweet)
        
        print(f"Tweet: {tweet}")
        print(f"Character Length: {char_count}\n")

if __name__ == "__main__":

    def load_config():
        with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
            return json.load(file)

    config = load_config()

    # Apply suffixes
    ANALYSIS_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["ANALYSIS_COLUMN_NAME"]
    TWEET_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["TWEET_COLUMN_NAME"]

    # print(ANALYSIS_COLUMN_NAME)
    # print(TWEET_COLUMN_NAME)

# Example usage
    db_params = st.secrets["cockroachdb"]["connection_string"]


tweets_list = fetch_tweets_from_db(db_params, TWEET_COLUMN_NAME)
analyze_tweets(tweets_list)