import os
import psycopg2
import datetime
import threading
import time
from tenacity import RetryError
import streamlit as st
import feedparser
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed
from concurrent.futures import ThreadPoolExecutor


class RSSReader:

    def __init__(self, database, days_to_crawl=1):
        self.database = database
        self.days_to_crawl = days_to_crawl

    def _get_rss_links(self):
        with psycopg2.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT link FROM rss_links")
            return [row[0] for row in cursor.fetchall()]

    def _load_existing_entries(self, table_name):
        with psycopg2.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT publisher, title, link, published, language FROM {table_name}")
            return set(cursor.fetchall())
        
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def _fetch_rss_entries(self, rss_link):

            feed = feedparser.parse(rss_link)
            current_entries = set()
            cutoff_date = datetime.now() - timedelta(days=self.days_to_crawl)

            publisher_name = feed.feed.title if hasattr(feed.feed, 'title') else "Unknown Publisher"
            language = feed.feed.language if hasattr(feed.feed, 'language') else "Unknown Language" 

            for entry in feed.entries:

                try:
                    title = entry.title if hasattr(entry, 'title') else ""
                    link = entry.link if hasattr(entry, 'link') else ""
                    published = entry.published if hasattr(entry, 'published') else ""

                    try:
                        pub_date = datetime.strptime(published, '%a, %d %b %Y %H:%M:%S %Z')
                        if pub_date >= cutoff_date:
                            current_entries.add((publisher_name, title, link, published, language))
                    except ValueError:
                        pass
                except Exception as e:
                    print(e)
                    pass

            return current_entries

    def _save_to_db(self, entries, table_name):
        with psycopg2.connect(self.database) as conn:
            cursor = conn.cursor()
            for entry in entries:
                try:
                    cursor.execute(f'''
                    INSERT INTO {table_name} (publisher, title, link, published, language)
                    VALUES (%s, %s, %s, %s, %s)
                    ''', entry)
                except psycopg2.IntegrityError:  # Link already exists
                    conn.rollback()  # Rollback transaction on error
                else:
                    conn.commit()  # Commit transaction if no errors

    def create_table_for_run(self):
        table_name = "rss_entries"  # Using a fixed table name
        with psycopg2.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                publisher TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT UNIQUE NOT NULL,
                published TEXT NOT NULL,
                language TEXT NOT NULL
            )
            ''')
        return table_name

    def start(self, rss_links):
        table_name = self.create_table_for_run()

        for rss_link in rss_links:
            current_entries = self._fetch_rss_entries(rss_link)
            existing_entries = self._load_existing_entries(table_name)
            new_entries = current_entries - existing_entries

            if new_entries:
                self._save_to_db(new_entries, table_name)



DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]
CHUNK_SIZE = 50  # Adjust as needed
DAYS_TO_CRAWL = 1  # Adjust as needed

RSS_READER_STATUS = {
    "status": "idle",
    "entries_crawled": 0,
    "rss_feeds_crawled": 0,
    "start_time": None,
    "runtime": None,
    "current_runtime": None
}

entries_lock = threading.Lock()  # Lock for thread-safe updates

def fetch_rss_links_from_db(chunk_size=CHUNK_SIZE):
    """Fetch RSS links from the database and split them into chunks."""
    try:
        with psycopg2.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT link FROM rss_links")
            all_links = [row[0] for row in cursor.fetchall()]
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return []

    # Splitting the all_links list into chunks of size chunk_size
    for i in range(0, len(all_links), chunk_size):
        yield all_links[i:i + chunk_size]

def fetch_single_rss_link(rss_link, done_event):
    """
    Fetch a single RSS link using the RSSReader.
    Signals the done_event when finished.
    """
    try:
        reader = RSSReader(DATABASE_PATH, days_to_crawl=DAYS_TO_CRAWL)
        reader.start([rss_link])
    except RetryError:
        print(f"Failed to fetch RSS from {rss_link} after multiple retries. Skipping this link.")
    finally:
        done_event.set()

def fetch_rss_data_chunk(rss_links_chunk):
    global RSS_READER_STATUS

    TIMEOUT_DURATION = 10  # e.g., 10 seconds

    for rss_link in rss_links_chunk:
        print(rss_link)

        max_attempts = 3
        attempts = 0

        while attempts < max_attempts:
            done_event = threading.Event()
            # rss_thread = threading.Thread(target=fetch_single_rss_link, args=(rss_link, done_event))
            rss_thread = threading.Thread(target=fetch_single_rss_link, args=(rss_link, done_event), daemon=True)

            rss_thread.start()

            done_event.wait(timeout=TIMEOUT_DURATION)
            
            if not done_event.is_set():
                print(f"Fetching RSS from {rss_link} took too long. Skipping this link.")
                with entries_lock:  # Ensuring thread-safe updates
                    RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

            try:
                with psycopg2.connect(DATABASE_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM rss_entries")
                    count = cursor.fetchone()[0]

                    with entries_lock:  # Ensuring thread-safe updates
                        RSS_READER_STATUS["entries_crawled"] += count
                        RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

            except RetryError:
                print(f"Failed to fetch RSS from {rss_link} after multiple retries. Skipping this link.")
                with entries_lock:  # Ensuring thread-safe updates
                    RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

            except psycopg2.OperationalError as e:
                if "database is locked" in str(e):
                    # Database is locked, wait and retry
                    time.sleep(1)  # Wait for a second before retrying
                    attempts += 1
                    print(e)
                    print(f"retry: {attempts}")

            except Exception as e:
                print(f"Unexpected error for {rss_link}: {str(e)}")
                with entries_lock:  # Ensuring thread-safe updates
                    RSS_READER_STATUS["rss_feeds_crawled"] += 1
                break

    return "success"

def run_rss_reader():
    global RSS_READER_STATUS

    RSS_READER_STATUS["start_time"] = time.time()

    chunks = list(fetch_rss_links_from_db())
    
    # Use ThreadPoolExecutor to run fetch_rss_data_chunk in multiple threads
    with ThreadPoolExecutor() as executor:
        executor.map(fetch_rss_data_chunk, chunks)

    end_time = time.time()
    RSS_READER_STATUS["runtime"] = end_time - RSS_READER_STATUS["start_time"]
    RSS_READER_STATUS["status"] = "idle"

    print(RSS_READER_STATUS)

    return 0

# if __name__ == "__main__":
#     run_rss_reader()
