

import psycopg2
import json
import streamlit as st
import requests

from concurrent.futures import ThreadPoolExecutor
import concurrent.futures


# Endpoint and token placeholder for HuggingFace
API_URL = "https://f3u0thkf9idjsh1r.us-east-1.aws.endpoints.huggingface.cloud"
token = st.secrets["huggingfaces"]["token"]
headers = {"Authorization": token}

def analyze_with_huggingface(title, custom_instruction):
    payload = {
        "inputs": custom_instruction + "\n Title: " + title,
        "parameters": {"temperature": 0.1, "max_new_tokens": 100}
    }
    response = requests.post(API_URL, headers=headers, json=payload)
    return response.json()

def process_title(title, custom_instruction, table_name, column_name, db_params):
    retry_count = 0
    max_retries = 3
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(db_params)
            cursor = conn.cursor()

            analysis = analyze_with_huggingface(title, custom_instruction)[0]["generated_text"]
            print(analysis)
            cursor.execute(f"UPDATE {table_name} SET \"{column_name}\" = %s WHERE title = %s;", (json.dumps(analysis), title))

            conn.commit()
            cursor.close()
            conn.close()
            return  # Successfully updated, so exit the loop
        except psycopg2.OperationalError as e:
            if "TransactionRetryError" in str(e):
                retry_count += 1
                if retry_count == max_retries:
                    print(f"Error processing title '{title}': Exceeded maximum retries.")
            else:
                print(f"Error processing title '{title}': {e}")
                return
        except Exception as e:
            print(f"Error processing title '{title}': {e}")
            return



def analyze_titles_in_cockroachdb(db_params, custom_instruction, column_name, language_filters=None, class_filters=None):
    try:
        conn = psycopg2.connect(db_params)
        cursor = conn.cursor()
        
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries%';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s AND column_name=%s;", (table_name, column_name))
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE {} ADD COLUMN {} TEXT;".format(table_name, column_name))
                conn.commit()
            
            where_clauses = ["\"{}\" IS NULL".format(column_name)]
            params = []
            
            if language_filters:
                where_clauses.append("(ai_language IN %s OR (ai_language IS NULL AND language IN %s))")
                params.extend([tuple(language_filters), tuple(language_filters)])
                
            if class_filters:
                where_clauses.append("class IN %s")
                params.append(tuple(class_filters))
            
            where_clause = " AND ".join(where_clauses)
            if where_clause:
                where_clause = "WHERE " + where_clause
            
            cursor.execute(f"SELECT title FROM {table_name} {where_clause}", params)
            
            titles = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        max_threads = 10
        with ThreadPoolExecutor(max_threads) as executor:
            futures = [executor.submit(process_title, title, custom_instruction, table_name, column_name, db_params) for title in titles]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread error: {e}")
                    
    except Exception as e:
        print(f"Error occurred: {e}")



# db_params = st.secrets["cockroachdb"]["connection_string"]

# custom_instruction = """
#                     context: You are a solopreneur a person who sets up and runs a business on their own:

#                     Classify the news headline importance: high, medium, low
#                     Do a sentiment classification: positive, negative, neutral

#                     Return a single json with following info and structur, only return the json without anything else: 

#                     {"Reason": "(Reason why for your sentiment and importance decsision based on the context)",
#                     "Sentiment": "positive, negative, neutral",
#                     "Importance": "high, medium, low"}
#                     """

# languages = ["en", "en-gb", "en-US"]
# classes = ["Economy", "Finance", "Business", "Stock Market", "Technology", "Politics"]

# analyze_titles_in_cockroachdb(db_params, custom_instruction=custom_instruction, column_name="general_feeds_ANALYSIS", language_filters=languages, class_filters=classes)