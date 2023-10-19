def compute_semantic_indicator_for_all_tables(db_params, target_classes=None):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Fetch all table names with prefix rss_entries_
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'rss_entries_%';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        date_suffix = table_name.split('_')[-1]

        # Check if corresponding sentiment_indicators table exists
        sentiment_table_name = f"sentiment_indicators_{date_suffix}"
        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name='{sentiment_table_name}';")
        if cursor.fetchone():
            # If table exists, delete it
            cursor.execute(f"DROP TABLE IF EXISTS {sentiment_table_name};")
        
        # Create the sentiment_indicators_suffix table
        cursor.execute(f"""
            CREATE TABLE {sentiment_table_name} (
                id SERIAL PRIMARY KEY,
                class TEXT NOT NULL,
                sentiment_index REAL NOT NULL,
                entry_count INTEGER NOT NULL
            );
        """)

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
            cursor.execute(f"INSERT INTO {sentiment_table_name} (class, sentiment_index, entry_count) VALUES (%s, %s, %s)", (class_name, sentiment_index, len(class_df)))

        # Compute aggregated sentiment score across specified classes
        total_entries = sum([entry_count for _, _, entry_count in aggregated_data])
        weighted_sum = sum([sentiment_index * entry_count for _, sentiment_index, entry_count in aggregated_data])
        aggregated_sentiment = weighted_sum / total_entries if total_entries else 0
        cursor.execute(f"INSERT INTO {sentiment_table_name} (class, sentiment_index, entry_count) VALUES (%s, %s, %s)", ('aggregated', aggregated_sentiment, total_entries))

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
    
    # If no specific target classes are provided, this will be an empty list
    target_classes_input = input("Enter the target classes separated by comma (or press Enter to calculate for all classes): ")
    target_classes = [cls.strip() for cls in target_classes_input.split(',')] if target_classes_input else None

    compute_semantic_indicator_for_all_tables(db_parameters, target_classes)
