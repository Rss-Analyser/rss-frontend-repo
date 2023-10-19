
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

def plot_aggregated_sentiment_indices(selected_indices):
    fig = go.Figure()
    
    for class_name, sentiment_index, entry_count in selected_indices:
        fig.add_trace(go.Indicator(
            mode = "gauge+number",
            value = sentiment_index,
            domain = {'x': [0, 1], 'y': [0, 1]},
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
            title = {'text': f"Class: {class_name} Total Entries: {entry_count}"}
        ))

    fig.update_layout(paper_bgcolor="lavender", font={'color': "darkblue", 'family': "Arial"})
    return fig

def main():
    st.title("Sentiment Indices Visualization")
    
    # Fetch all sentiment indicator tables
    sentiment_tables = fetch_sentiment_tables()
    
    # Create a dropdown for users to select the table (suffix)
    selected_table = st.selectbox("Choose a table:", sentiment_tables)
    
    # Fetch data for the selected table
    sentiment_indices = fetch_sentiment_indices(selected_table)
    available_classes = [idx[0] for idx in sentiment_indices]
    
    # Allow users to select multiple classes
    selected_classes = st.multiselect("Select classes to display:", available_classes, default=available_classes)
    
    # Filter the indices based on selected classes
    selected_indices = [idx for idx in sentiment_indices if idx[0] in selected_classes]

    # Display the aggregated sentiment indices
    st.plotly_chart(plot_aggregated_sentiment_indices(selected_indices))

if __name__ == "__main__":
    main()
