import os
import openai
import psycopg2
import json
import streamlit as st
import time

def analyze_with_gpt4(title, custom_instruction):
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
    retries = 3
    for _ in range(retries):
        try:
            with psycopg2.connect(db_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries%';")
                    tables = cursor.fetchall()

                    for table in tables:
                        table_name = table[0]

                        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s AND column_name=%s;", (table_name, column_name))
                        if not cursor.fetchone():
                            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{column_name}\" TEXT;")
                            conn.commit()

                        where_clauses = [f"\"{column_name}\" IS NULL"]
                        params = []

                        if language_filters:
                            where_clauses.append("(ai_language = ANY(%s) OR (ai_language IS NULL AND language = ANY(%s)))")
                            params.extend([language_filters, language_filters])

                        if class_filters:
                            where_clauses.append("class = ANY(%s)")
                            params.append(class_filters)

                        where_clause = " AND ".join(where_clauses)
                        if where_clause:
                            where_clause = "WHERE " + where_clause

                        cursor.execute(f"SELECT title FROM {table_name} {where_clause};", params)

                        titles = [row[0] for row in cursor.fetchall()]

                        print(len(titles))

                        for title in titles:
                            max_retries = 3
                            retries = 0
                            while retries < max_retries:
                                try:
                                    analysis = analyze_with_gpt4(title, custom_instruction)
                                    print(analysis)
                                    cursor.execute(f"UPDATE {table_name} SET \"{column_name}\" = %s WHERE title = %s;", (json.dumps(analysis), title))
                                    conn.commit()
                                    break
                                except openai.error.Timeout:
                                    retries += 1
                                    wait_time = 2 ** retries
                                    time.sleep(wait_time)

            # If the above logic completes without an error, break out of the loop
            break
        except psycopg2.OperationalError as e:
            # Log the error (consider using the logging module)
            print(f"Database error: {e}")
            # Close the connection if it's not already closed
            if conn and not conn.closed:
                conn.close()
            # If we've reached the max number of retries, re-raise the error
            if _ == retries - 1:
                raise
            # Otherwise, wait for a short duration before retrying
            time.sleep(5)


if __name__ == "__main__":

    def load_config():
        with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
            return json.load(file)

    config = load_config()

    # Apply suffixes
    ANALYSIS_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["ANALYSIS_COLUMN_NAME"]

    print(ANALYSIS_COLUMN_NAME)

    custom_instruction = config["params"]["ANALYSIS_INSTRUCTION"]

    languages = config["params"]["ANALYSIS_LANGUAGES"]
    classes = config["params"]["ANALYSIS_CLASSES"]
# Example usage
    db_params = st.secrets["cockroachdb"]["connection_string"]

    analyze_titles_in_cockroachdb(db_params, custom_instruction=custom_instruction, column_name=ANALYSIS_COLUMN_NAME, language_filters=languages, class_filters=classes)
