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
    openai.api_key = st.secrets["openai"]["openai_api_key"]

    completion = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": custom_instruction},
        {"role": "user", "content": title}
    ]
    )
    return completion.choices[0].message

def fetch_high_importance_entries(db_params, custom_instruction):
    # Connect to the database
    conn = psycopg2.connect(db_params)
    cursor = conn.cursor()
    
    # Fetch table names with the given prefix "rss_entries_"
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries_%';")
    tables = cursor.fetchall()

    high_importance_entries = []

    for table in tables:
        table_name = table[0]
        
        # Fetch title, link, and stocktraderanalysis column values
        cursor.execute(f"SELECT title, link, \"stocktraderanalysis\" FROM {table_name} WHERE \"stocktraderanalysis\" IS NOT NULL;")
        entries = cursor.fetchall()
        
        for entry in entries:
            title, link, text_data = entry
            try:
                # Convert the text data into JSON
                data = json.loads(json.loads(text_data)["content"])
                
                # Check if the importance is "High"
                if data.get("Importance") == "High":
                    high_importance_entries.append({
                        "title": title,
                        "link": link,
                        "analysis": data
                    })
            except (json.JSONDecodeError, KeyError):
                # Handle cases where the data isn't valid JSON or doesn't have the expected structure
                pass

    cursor.close()
    conn.close()

    # Iterate through high importance entries
    for entry in high_importance_entries:

        content = fetch_article_content(entry["link"])
        entry['content'] = content

        # print(content)

        tweet = analyze_with_gpt4(content, custom_instruction)
        print(tweet["content"])

        # Do whatever you need with each entry here
        # print(entry)

custom_instruction = """
                    You are generating tweets which need to be as contoversal as possible:

                    return a tweet written in a simple language as if you are a human.


                    """

# Example usage
db_params = os.environ["DATABASE_URL"]

fetch_high_importance_entries(db_params, custom_instruction=custom_instruction)
