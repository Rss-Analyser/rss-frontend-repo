import psycopg2
import streamlit as st
import json
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline

tokenizer = AutoTokenizer.from_pretrained('qanastek/51-languages-classifier')
model = AutoModelForSequenceClassification.from_pretrained('qanastek/51-languages-classifier')
classifier = TextClassificationPipeline(model=model, tokenizer=tokenizer)

def classify_language(text):
    result = classifier(text)
    return result[0]['label'], result[0]['score']

def classify_titles_language_from_db(cockroachdb_conn_str, increment_func=None):
    conn = psycopg2.connect(cockroachdb_conn_str)
    cursor = conn.cursor()

    # Fetching table names
    cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name LIKE 'rss_entries';
    """)
    tables = [table[0] for table in cursor.fetchall()]

    for table in tables:
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table,))
        columns = [column[0] for column in cursor.fetchall()]
        
        # Adding 'ai_language' column if not present
        if 'ai_language' not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN ai_language TEXT;")
            conn.commit()

        cursor.execute(f"SELECT rowid, Title FROM {table} WHERE ai_language IS NULL OR ai_language = '';")
        titles = cursor.fetchall()

        for rowid, title in titles:
            detected_language, confidence = classify_language(title)
            cursor.execute(f"UPDATE {table} SET ai_language = %s WHERE rowid = %s", (detected_language, rowid))
            
            if increment_func:
                increment_func()

        conn.commit()
    conn.close()

# Database connection string example
DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]
classify_titles_language_from_db(DATABASE_PATH)
