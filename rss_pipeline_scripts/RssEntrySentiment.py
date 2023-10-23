from transformers import pipeline
import psycopg2
import streamlit as st

MODEL = st.secrets["sentimentModel"]['sentiment_model']

sentiment_analysis = pipeline("sentiment-analysis", model=MODEL)

def classify_sentiments_in_db(cockroachdb_conn_str, classes_list=None, increment_func=None):
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
        # Fetch columns for the current table
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table,))
        columns = [column[0] for column in cursor.fetchall()]
        
        # Check and add 'Sentiment' column if it doesn't exist
        if 'sentiment' not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN sentiment TEXT;")
            conn.commit()
        if classes_list:
            placeholders = ', '.join(['%s' for _ in classes_list])
            cursor.execute(f"SELECT rowid, Title FROM {table} WHERE (class IN ({placeholders})) AND (sentiment IS NULL OR sentiment = '');", classes_list)
        else:
            cursor.execute(f"SELECT rowid, Title FROM {table} WHERE sentiment IS NULL OR sentiment = '';")
        
        titles = cursor.fetchall()

        for row_id, title in titles:
            try:
                sentiment = analyze_sentiment(title)
            except Exception as e:
                print(f"Error during classification of title '{title}': {e}")
                sentiment = 'Unknown'
            
            cursor.execute(f"UPDATE {table} SET sentiment = %s WHERE rowid = %s", (sentiment, row_id))

            if increment_func:
                increment_func()
        
        conn.commit()
    conn.close()

def analyze_sentiment(title):
    if not title or not isinstance(title, str):
        return 'Unknown'
    return str(sentiment_analysis(title)[0]["label"])

# # DEFAULT_CLASSES = ["News", "Entertainment", "Sports", "Economy", "Technology", "Science", "Stock Market", "Reviews", "Business", "Finance", "Politics"]
# DEFAULT_CLASSES = None
# DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]

# classify_sentiments_in_db(DATABASE_PATH, classes_list=DEFAULT_CLASSES)
