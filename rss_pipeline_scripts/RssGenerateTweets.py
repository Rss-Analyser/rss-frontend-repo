import psycopg2
import json
import os
import openai

import streamlit as st

import requests
from bs4 import BeautifulSoup

from newspaper import Article

def fetch_article_content(link):
    try:
        article = Article(link)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print(f"Failed to fetch content for link {link}. Error: {e}")
        return None

def fetch_content_from_link(link):
    try:
        response = requests.get(link, timeout=10)  # adding a timeout for the request
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.get_text()
    except Exception as e:
        print(f"Failed to fetch content for link {link}. Error: {e}")
        return None
    

# Placeholder for GPT-4
def analyze_with_gpt4(title, custom_instruction):
    # Normally, here you would send the title to GPT-4 with the custom instruction
    # and get the response. For now, I'm just returning a dummy JSON.

    if not title:
        print("Skipping empty content.")
        return {"content": "No content due to empty article content."}
    
    openai.api_key =  st.secrets["openai"]["openai_api_key"]

    completion = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": custom_instruction},
        {"role": "user", "content": title}
    ],
    # max_tokens=70,
    )
    return completion.choices[0].message

def generate_valid_tweet(content, custom_tweet_instruction_generation):
    custom_tweet_instruction = """
    
    Create a shorter text with the same content and character lenght below or equal 280 
    don't mention the character lenght in the text try to maximize the content lenght but stay below 280 characters, 
    don't start and end with double quotes: 
    
    """
    
    # Initially, generate a tweet
    generated_tweet = analyze_with_gpt4(content, custom_tweet_instruction_generation)["content"]
    
    # While the tweet's character count is over 280, keep regenerating
    while len(generated_tweet) > 280:
        print("------")
        print(generated_tweet)
        print(f"tweet is too long ({len(generated_tweet)}) paraphrasing:")
        generated_tweet = generated_tweet + " character lenght is: " + str(len(generated_tweet))
        generated_tweet = analyze_with_gpt4(generated_tweet, custom_tweet_instruction)["content"]
        print("new tweet:")
        print(generated_tweet)
        print("------")

    return generated_tweet

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
                # Convert the text data into JSON
                data = json.loads(json.loads(text_data)["content"])
                
                importance = data.get("Importance")
                if importance and importance.lower() == "high":
                    high_importance_entries.append({
                        "title": title,
                        "link": link,
                        "analysis": data
                    })
            except (json.JSONDecodeError, KeyError):
                print("Json decode error")
                # Handle cases where the data isn't valid JSON or doesn't have the expected structure
                pass

    for entry in high_importance_entries:
        try:
            # Check if the tweet already exists for this entry
            cursor.execute(f'''
            SELECT "{column_name}" FROM {table_name} 
            WHERE link = %s;
            ''', (entry["link"],))
            existing_tweet = cursor.fetchone()

            # If tweet doesn't exist, generate and update
            if not existing_tweet or existing_tweet[0] is None:
                custom_tweet_instruction = custom_instruction + "\nYou should concentrate on: " + entry["title"]
                content = fetch_article_content(entry["link"])
                entry['content'] = content

                generated_tweet = generate_valid_tweet(content, custom_tweet_instruction)
                
                # Save the tweet to the database
                print(f"Updating entry with link: {entry['link']}")
                cursor.execute(f'''
                UPDATE {table_name} 
                SET "{column_name}" = %s 
                WHERE link = %s;
                ''', (generated_tweet, entry["link"]))
                print(f"Rows updated: {cursor.rowcount}")
                conn.commit()  # Commit the transaction after updating the tweet

                entry['tweet'] = generated_tweet
                print(entry['tweet'])
                # yield entry
            else:
                print(f"Tweet already exists for entry {entry['title']}.")

        except Exception as e:
            print(f"Error processing entry {entry['title']}. Error: {e}")


    
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

    fetch_high_importance_entries(db_params, config["params"]["TWEET_INSTRUCTION"], column_name=TWEET_COLUMN_NAME, analysis_column_name=ANALYSIS_COLUMN_NAME)
    
