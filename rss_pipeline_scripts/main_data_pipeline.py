import json
import streamlit as st
from RssWebFeedExtractor import WebsiteRssFeedExtractor
from RssEntryReader import run_rss_reader
from RssEntryClassifier import classify_titles_from_db
from RssEntrySentiment import classify_sentiments_in_db
from RssOpenAiAnalyser import analyze_titles_in_cockroachdb
from RssGenerateTweets import fetch_high_importance_entries

DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]

def load_config():
    with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
        return json.load(file)

def main_pipeline():
    config = load_config()
    
    # Apply suffixes
    ANALYSIS_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["ANALYSIS_COLUMN_NAME"]
    TWEET_COLUMN_NAME = config["params"]["ANALYSIS_COLUMN_NAME_BASE"] + config["suffixes"]["TWEET_COLUMN_NAME"]
    
    # RSS Feed Extraction
    if config["steps"]["extract"]:
        extractor = WebsiteRssFeedExtractor(max_depth=2)
        extractor.start_crawler(config["params"]["URLS"], config["params"]["RSS_LINKS"])
        print("extraction completed")
    
    # RSS Reader Execution
    if config["steps"]["read"]:
        run_rss_reader()
    
    # Classify Titles
    if config["steps"]["classify_titles"]:
        classify_titles_from_db(DATABASE_PATH, classes=config["params"]["DEFAULT_CLASSES"], threshold=config["params"]["DEFAULT_THRESHOLD"])
    
    # Classify Sentiments
    if config["steps"]["classify_sentiments"]:
        classify_sentiments_in_db(DATABASE_PATH)
    
    # Analyze Titles
    if config["steps"]["analyze_titles"]:
        analyze_titles_in_cockroachdb(DATABASE_PATH, 
                                      custom_instruction=config["params"]["ANALYSIS_INSTRUCTION"], 
                                      column_name=ANALYSIS_COLUMN_NAME, 
                                      language_filters=config["params"]["ANALYSIS_LANGUAGES"], 
                                      class_filters=config["params"]["ANALYSIS_CLASSES"])
    
    # Fetch and Print High Importance Entries create tweets
    if config["steps"]["fetch_and_print"]:
        for result in fetch_high_importance_entries(DATABASE_PATH, config["params"]["TWEET_INSTRUCTION"], TWEET_COLUMN_NAME, ANALYSIS_COLUMN_NAME):
            print(result)


    return 0

# if __name__ == "__main__":
#     main()
