import os
import openai
import psycopg2
import json
import streamlit as st

# Placeholder for GPT-4
def analyze_with_gpt4(title, custom_instruction):
    # Normally, here you would send the title to GPT-4 with the custom instruction
    # and get the response. For now, I'm just returning a dummy JSON.
    openai.api_key = st.secrets["openai"]["openai_api_key"]

    completion = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": custom_instruction},
        {"role": "user", "content": title}
    ]
    )

    return completion.choices[0].message

def analyze_titles_in_cockroachdb(db_params, custom_instruction, column_name, language_filters=None, class_filters=None):
    # Connect to the database
    conn = psycopg2.connect(db_params)
    cursor = conn.cursor()
    
    # Fetch table names with the given prefix "rss_entries_"
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries%';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        
        # Check if column exists
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND column_name='{column_name}';")
        if not cursor.fetchone():
            # If column doesn't exist, add it
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{column_name}\" TEXT;")
            conn.commit()  # Commit the transaction immediately after adding the column
        
        # Construct the WHERE clause based on filters
        where_clauses = [f"\"{column_name}\" IS NULL"]  # Add condition to check NULL values
        
        # Check ai_language first. If that's NULL, then check the language column
        language_clause = "(ai_language IS NOT NULL OR (ai_language IS NULL AND language NOT IN ('Unknown Language', '')))"
        where_clauses.append(language_clause)
        
        if language_filters:
            formatted_languages = ', '.join(['%s'] * len(language_filters))
            where_clauses.append(f"(ai_language IN ({formatted_languages}) OR (ai_language IS NULL AND language IN ({formatted_languages})))")
        if class_filters:
            formatted_classes = ', '.join(['%s'] * len(class_filters))
            where_clauses.append(f"class IN ({formatted_classes})")
        
        where_clause = " AND ".join(where_clauses)
        if where_clause:
            where_clause = "WHERE " + where_clause
        
        # Fetch rows from the table based on filters
        cursor.execute(f"SELECT title FROM {table_name} {where_clause};", tuple(language_filters + (class_filters or [])))
        
        titles = [row[0] for row in cursor.fetchall()]
        
        for title in titles:
            analysis = analyze_with_gpt4(title, custom_instruction)  # Assuming this function is defined somewhere
            print(analysis)
            
            # Since we're only fetching rows where the column is NULL, we can directly update
            cursor.execute(f"UPDATE {table_name} SET \"{column_name}\" = %s WHERE title = %s;", (json.dumps(analysis), title))
        
        conn.commit()

    cursor.close()
    conn.close()


# db_params = st.secrets["cockroachdb"]["connection_string"]

# custom_instruction = """
#                     You are a stock trader:

#                     Classify the news headline importance: high, medium, low
#                     Do a sentiment classification: positive, negative, neutral

#                     Return a single  json with following info: 
#                     - Contextual Analysis
#                     - Audience Analysis
#                     - Potential Impact
#                     - Stock Classes
#                     - Sentiment
#                     - Importance

#                     only return the json without anything else
#                     """

# custom_instruction = """
#                     You are a solopreneur a person who sets up and runs a business on their own:

#                     Classify the news headline importance: high, medium, low
#                     Do a sentiment classification: positive, negative, neutral

#                     Return a single json with following info: 
#                     - Reason
#                     - Sentiment
#                     - Importance

#                     only return the json without anything else
#                     """

# languages = ["en", "en-gb", "en-US"]
# classes = ["Economy", "Finance", "Business", "Stock Market", "Technology", "Politics"] 

# # classes = None
# analyze_titles_in_cockroachdb(db_params, custom_instruction=custom_instruction, column_name="stocktraderanalysis", language_filters=languages, class_filters=classes)
