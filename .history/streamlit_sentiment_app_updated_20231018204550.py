
import streamlit as st
import psycopg2
import plotly.graph_objects as go

# Database connection parameters
db_params = {
    'dbname': 'rss_db',
    'user': 'root',
    'password': None,
    'host': 'localhost',
    'port': '26257'
}

def fetch_sentiment_tables():
    with psycopg2.connect(**db_params) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'sentiment_indicators_%';")
        tables = [t[0] for t in cursor.fetchall()]
    return tables

def fetch_sentiment_indices(table_name):
    with psycopg2.connect(**db_params) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT Class, sentiment_index, entry_count FROM {table_name};")
        indices = cursor.fetchall()
    return indices

def plot_sentiment_index(class_name, sentiment_index, entry_count):
    sentiment_index = max(min(sentiment_index, 1), -1)  # Clamping to -1 to 1
    title_text = f"Class: {class_name} Data from RSS News Titles Total Entries: {entry_count}"
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = sentiment_index,
        domain = {'x': [0, 1], 'y': [0, 1]},
        delta = {'reference': 0},
        gauge = {
            'axis': {'range': [-1, 1], 'tickwidth': 1, 'tickcolor': "black"},
            'bar': {'color': "darkblue"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [-1, -0.33], 'color': 'red'},
                {'range': [-0.33, 0.33], 'color': 'yellow'},
                {'range': [0.33, 1], 'color': 'green'},
            ],
        },
        title = {'text': title_text}
    ))
    fig.update_layout(paper_bgcolor="lavender", font={'color': "darkblue", 'family': "Arial"})
    return fig

def main():
    st.title("Sentiment Indices Visualization")
    
    # Fetch all sentiment indicator tables
    sentiment_tables = fetch_sentiment_tables()
    
    # Create a dropdown for users to select the table (suffix)
    selected_table = st.selectbox("Choose a table:", sentiment_tables)
    
    # Fetch and display data for the selected table
    sentiment_indices = fetch_sentiment_indices(selected_table)
    for class_name, sentiment_index, entry_count in sentiment_indices:
        st.plotly_chart(plot_sentiment_index(class_name, sentiment_index, entry_count))

if __name__ == "__main__":
    main()
