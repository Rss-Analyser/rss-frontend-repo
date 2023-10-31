import sqlite3
from transformers import AutoTokenizer, AutoModel
from torch.nn import functional as F
import torch
import json
import psycopg2
import streamlit as st

tokenizer = AutoTokenizer.from_pretrained("thenlper/gte-small")
model = AutoModel.from_pretrained("thenlper/gte-small")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

def classify_titles_from_db(cockroachdb_conn_str, classes, threshold=0.8, increment_func=None):
    conn = psycopg2.connect(cockroachdb_conn_str)
    cursor = conn.cursor()
    # Fetching table names
    cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name LIKE 'rss_entries';
    """)
    tables = [table[0] for table in cursor.fetchall()]
    class_embeddings = [model(tokenizer.encode(text, return_tensors='pt').to(device))[0].mean(1).squeeze().detach().cpu() for text in classes]

    # classified_count = 0  # Counter to track the number of classified entries

    for table in tables:
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table,))
        columns = [column[0] for column in cursor.fetchall()]
        
        if 'class' not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN class TEXT;")
            conn.commit()
        if 'similarity' not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN similarity REAL;")
            conn.commit()
        if 'embedding' not in columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN embedding TEXT;")
            conn.commit()

        cursor.execute(f"SELECT rowid, Title FROM {table} WHERE class IS NULL OR class = '';")
        titles = cursor.fetchall()

        
        
        for rowid, title in titles:
            classification, similarity, title_embedding_tensor = get_most_similar_class(title, class_embeddings, classes, threshold)
            serialized_embedding = json.dumps(title_embedding_tensor.detach().cpu().numpy().tolist())
            cursor.execute(f"UPDATE {table} SET Class = %s, Similarity = %s, Embedding = %s WHERE rowid = %s", (classification, similarity, serialized_embedding, rowid))
            
            if increment_func:
                increment_func()
        
        conn.commit()
    conn.close()

    # return classified_count  # Return the total number of classified entries

def get_most_similar_class(title, class_embeddings, classes, threshold):
    if not title or not isinstance(title, str):
        return "None", 0, torch.tensor([])
    title_embedding_tensor = model(tokenizer.encode(title, return_tensors='pt').to(device))[0].mean(1).squeeze()
    similarities = [F.cosine_similarity(title_embedding_tensor, class_emb, dim=0).item() for class_emb in class_embeddings]
    max_similarity = max(similarities)
    classification = "None" if max_similarity < threshold else classes[similarities.index(max_similarity)]
    return classification, round(max_similarity, 2), title_embedding_tensor


# DEFAULT_CLASSES = ["News", "Entertainment", "Sports", "Economy", "Technology", "Science", "Stock Market", "Reviews", "Business", "Finance", "Politics"]
# DEFAULT_THRESHOLD = 0.75


if __name__ == "__main__":

    def load_config():
        with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
            return json.load(file)
        
    config = load_config()
            
    DEFAULT_CLASSES = config["params"]["DEFAULT_CLASSES"]
    DEFAULT_THRESHOLD = config["params"]["DEFAULT_THRESHOLD"]
    DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]


    classify_titles_from_db(DATABASE_PATH, classes=DEFAULT_CLASSES, threshold=DEFAULT_THRESHOLD)