import psycopg2
import plotly.graph_objects as go
from PIL import Image
import io

def fetch_sentiment_tables(db_params):
    with psycopg2.connect(**db_params) as conn:
        cursor = conn.cursor()
        
        # Get all sentiment_indicators tables
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'sentiment_indicators_%';")
        tables = [t[0] for t in cursor.fetchall()]
        
    return tables

def fetch_sentiment_indices(db_params, table_name):
    with psycopg2.connect(**db_params) as conn:
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT DISTINCT Class, sentiment_index, entry_count FROM {table_name};")
        indices = cursor.fetchall()
        
    return indices

def plot_sentiment_index(class_name, sentiment_index, entry_count):
    sentiment_index = max(min(sentiment_index, 1), -1)  # Clamping to -1 to 1

    title_text = f"Class: {class_name}\nData from RSS News Titles\nTotal Entries: {entry_count}"

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
    db_params = {
        'dbname': 'rss_db',
        'user': 'root',
        'password': None,
        'host': 'localhost',
        'port': '26257'
    }
    
    # Fetch all sentiment indicator tables
    sentiment_tables = fetch_sentiment_tables(db_params)
    
    for table in sentiment_tables:
        sentiment_indices = fetch_sentiment_indices(db_params, table)
        images = []

        for idx, (class_name, sentiment_index, entry_count) in enumerate(sentiment_indices):
            fig = plot_sentiment_index(class_name, sentiment_index, entry_count)

            # Convert the Plotly figure to an Image object (PIL)
            image_bytes = fig.to_image(format="png")
            image = Image.open(io.BytesIO(image_bytes))
            images.append(image)

        # Concatenate all images vertically
        total_width = max(image.width for image in images)
        total_height = sum(image.height for image in images)
        concatenated_image = Image.new('RGB', (total_width, total_height))

        y_offset = 0
        for image in images:
            concatenated_image.paste(image, (0, y_offset))
            y_offset += image.height

        suffix = table.split('_')[-1]
        concatenated_image.save(f"sentiment_indices_{suffix}.png")

if __name__ == "__main__":
    main()
