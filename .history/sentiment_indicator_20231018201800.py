import psycopg2
import pandas as pd

def compute_semantic_indicator(db_params, date_suffix, target_classes=None):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Check if sentiment_indicators table exists
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name='sentiment_indicators';")
    if cursor.fetchone():
        # If table exists, delete it
        cursor.execute("DROP TABLE IF EXISTS sentiment_indicators;")
    
    # Create the sentiment_indicators table
    cursor.execute("""
        CREATE TABLE sentiment_indicators (
            id SERIAL PRIMARY KEY,
            class TEXT NOT NULL,
            sentiment_index REAL NOT NULL,
            entry_count INTEGER NOT NULL
        );
    """)
    
    # Fetch specific table with the given date_suffix
    table_name = f"rss_entries_{date_suffix}"

    # Map sentiments to scores
    sentiment_to_score = {
        'positive': 1,
        'neutral': 0,
        'negative': -1,
        'Unknown': 0  # Handle potential unknowns
    }

    # If target classes are not provided, compute for all classes
    if not target_classes:
        df = pd.read_sql_query(f"SELECT class, sentiment FROM {table_name} WHERE sentiment IS NOT NULL", conn)
    else:
        placeholders = ', '.join(['%s' for _ in target_classes])
        df = pd.read_sql_query(f"SELECT class, sentiment FROM {table_name} WHERE sentiment IS NOT NULL AND class IN ({placeholders})", conn, params=target_classes)
    
    aggregated_data = []
    
    for class_name in df['class'].unique():
        class_df = df[df['class'] == class_name]
        class_df['score'] = class_df['sentiment'].map(sentiment_to_score)
        sentiment_index = class_df['score'].mean()
        aggregated_data.append((class_name, sentiment_index, len(class_df)))
        cursor.execute("INSERT INTO sentiment_indicators (class, sentiment_index, entry_count) VALUES (%s, %s, %s)", (class_name, sentiment_index, len(class_df)))

    # Compute aggregated sentiment score across specified classes
    total_entries = sum([entry_count for _, _, entry_count in aggregated_data])
    weighted_sum = sum([sentiment_index * entry_count for _, sentiment_index, entry_count in aggregated_data])
    aggregated_sentiment = weighted_sum / total_entries if total_entries else 0
    cursor.execute("INSERT INTO sentiment_indicators (class, sentiment_index, entry_count) VALUES (%s, %s, %s)", ('aggregated', aggregated_sentiment, total_entries))
    
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    db_parameters = {
        'dbname': 'rss_db',
        'user': 'root',
        'password': None,
        'host': 'localhost',
        'port': '26257'
    }
    date_suffix = input("Enter the date suffix (e.g., '20231010' for table rss_entries_20231010): ")
    
    # If no specific target classes are provided, this will be an empty list
    target_classes_input = input("Enter the target classes separated by comma (or press Enter to calculate for all classes): ")
    target_classes = [cls.strip() for cls in target_classes_input.split(',')] if target_classes_input else None

    compute_semantic_indicator(db_parameters, date_suffix, target_classes)
