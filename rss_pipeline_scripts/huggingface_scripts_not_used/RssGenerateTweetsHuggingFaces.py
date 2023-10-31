import psycopg2
import json
import os
import streamlit as st
import requests
from bs4 import BeautifulSoup
from newspaper import Article

# Endpoint and token placeholder for HuggingFace
API_URL = "https://x7541o6ju3rir98g.us-east-1.aws.endpoints.huggingface.cloud"
token = st.secrets["huggingfaces"]["token"]

HEADERS = {
    "Authorization": token
}

def fetch_article_content(link):
    try:
        article = Article(link)
        article.download()
        article.parse()
        return article.text
    
    except Exception as e:
        print(f"Failed to fetch content for link {link}. Error: {e}")
        return None

def analyze_with_huggingface(content, custom_instruction):
    if not content:
        print("Skipping empty content.")
        return {"content": "No content due to empty article content."}

    payload = {
        "inputs": custom_instruction + "\n" + content,
    }
    response = requests.post(API_URL, headers=HEADERS, json=payload)
    
    # Handling the response to extract the content
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error from HuggingFace API: {response.text}")
        return {"content": "Error while processing."}

def fetch_content_from_link(link):
    try:
        response = requests.get(link, timeout=10)  # adding a timeout for the request
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.get_text()
    except Exception as e:
        print(f"Failed to fetch content for link {link}. Error: {e}")
        return None

def fetch_high_importance_entries(db_params, custom_instruction, column_name, analysis_column_name):
    # Connect to the database
    conn = psycopg2.connect(db_params)
    cursor = conn.cursor()
    
    # Fetch table names with the given prefix "rss_entries_"
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries';")
    tables = cursor.fetchall()

    high_importance_entries = []

    for table in tables:
        table_name = table[0]
        
        # Check if "generated_tweet" column exists
        check_column_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='{table_name}' AND column_name='{column_name}';
        """
        cursor.execute(check_column_query)
        
        # If column doesn't exist, add it
        if not cursor.fetchone():
            add_column_query = f"""
            ALTER TABLE {table_name} ADD COLUMN "{column_name}" TEXT;
            """
            cursor.execute(add_column_query)
            conn.commit()  # Commit the transaction immediately after adding the column
            
        # Fetch title, link, stocktraderanalysis, and generated_tweet column values
        fetch_values_query = f"""
        SELECT title, link, "{analysis_column_name}", "{column_name}" 
        FROM {table_name} 
        WHERE "{analysis_column_name}" IS NOT NULL;
        """
        cursor.execute(fetch_values_query)
        entries = cursor.fetchall()
        
        for entry in entries:

            title, link, text_data, _ = entry

            try:
                # Check if the substring "Importance: high" is present in the text_data
                if "Importance: high" in text_data:
                    high_importance_entries.append({
                        "title": title,
                        "link": link,
                        "analysis": text_data
                    })
            except Exception as e:
                print(f"Unexpected error for title '{title}': {e}")

    for entry in high_importance_entries:
        try:
            if cursor.rowcount == 0:
                print(f"No rows were updated for entry {entry['title']} because a tweet already exists.")
            else:
                custom_tweet_instruction = custom_instruction + "\nYou should concentrate on: " + entry["title"]
                content = fetch_article_content(entry["link"])
                entry['content'] = content
                            # Analyze and update entry
                generated_tweet = analyze_with_huggingface(content, custom_tweet_instruction)

                print(generated_tweet)
                        
                # Check if the current value of the column is empty (i.e., NULL) and then save the tweet to the database
                cursor.execute(f'''
                UPDATE {table_name} 
                SET "{column_name}" = %s 
                WHERE link = %s AND "{column_name}" IS NULL;
                ''', (generated_tweet, entry["link"]))
                conn.commit()  # Commit the transaction after updating the tweet

                entry['tweet'] = generated_tweet
                # print(entry['tweet'])
                yield entry

        except Exception as e:
            print(f"Error processing entry {entry['title']}. Error: {e}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":

    custom_instruction = """
        You are a solopreneur a person who sets up and runs a business on their own.
        You are generating tweets which need to be as controversial as possible.
        Return a tweet written in a simple language as if you are a human.
        Important: Use maximum 280 characters.
        """

    def load_config():
        with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
            return json.load(file)
    # Example usage
    config = load_config()

    db_params = st.secrets["cockroachdb"]["connection_string"]
    # Apply suffixes
    ANALYSIS_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["ANALYSIS_COLUMN_NAME"]
    TWEET_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["TWEET_COLUMN_NAME"]

    for result in fetch_high_importance_entries(db_params, custom_instruction, TWEET_COLUMN_NAME, ANALYSIS_COLUMN_NAME):
        print(result)
