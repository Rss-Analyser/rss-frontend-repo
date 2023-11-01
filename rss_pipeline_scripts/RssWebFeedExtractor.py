
from PyPDF2 import PdfFileReader
from bs4 import BeautifulSoup
from io import BytesIO
import csv
import os
import re
import requests
import threading
import yaml
import psycopg2
import streamlit as st
import psycopg2.extras
import json

class WebsiteRssFeedExtractor:
    DATABASE_PATH = st.secrets["cockroachdb"]["connection_string"]
    RSS_FEED_WEBSITES_TABLE_NAME = st.secrets["cockroachdb"]["website_table_name"]
    RSS_LINKS_TABLE_NAME = st.secrets["cockroachdb"]["rss_links_table_name"]
    
    def __init__(self, max_depth=3):
  
        self.visited_urls = set()  # To keep track of visited URLs
        self.max_depth = max_depth

    @staticmethod
    def _is_valid_url(url):
        """Validate the URL structure to avoid invalid ones."""
        parsed_url = requests.utils.urlparse(url)
        return all([parsed_url.scheme, parsed_url.netloc, parsed_url.path])

    def _extract_links_from_text(self, content):
        rss_links = set()
        # Improved regex pattern that avoids capturing unwanted HTML tags
        url_pattern = re.compile(
            r'https?://[\w\d\-._~:/?#[\]@!$&\'()*+,;=%]+')
        links = re.findall(url_pattern, content)
        for link in links:
            if (link.endswith(('.rss', '.rss.xml', '.xml')) or 'rss' in link or 'feed' in link) and self._is_valid_url(link):
                rss_links.add(link)
        return rss_links

    def _parse_opml_content(self, content):
        soup = BeautifulSoup(content, 'xml')  # Parse the content as XML
        rss_links = set()

        # Look for <outline> tags with an "xmlUrl" attribute
        for outline in soup.find_all("outline", xmlUrl=True):
            rss_link = outline['xmlUrl']
            if self._is_valid_url(rss_link):  # Validate the URL
                rss_links.add(rss_link)
        
        return rss_links

    def _parse_for_rss_links(self, html, depth=0):
        soup = BeautifulSoup(html, 'html.parser')
        rss_links = set()

        for link in soup.find_all("a", href=True):
            href = link['href']
            if (href.endswith(('.rss', '.rss.xml', '.xml')) or 'rss' in href or 'feed' in href) and self._is_valid_url(href):
                rss_links.add(href)

            # Check for resource type links and recurse if necessary
            elif any(href.endswith(ext) for ext in ['.csv', '.yml', '.yaml', '.opml', '.txt', '.pdf']):
                if depth < self.max_depth and href not in self.visited_urls:  # Check if depth is within limit and URL is not visited
                    self.visited_urls.add(href)
                    rss_links.update(self.crawl(href, depth + 1))  # Recurse

        # Extracting from plain text inside HTML
        rss_links_from_text = self._extract_links_from_text(html)
        rss_links.update(rss_links_from_text)

        return rss_links
    
    def _parse_csv_content(self, content):
        rss_links = set()
        csv_data = csv.reader(content.splitlines())
        for row in csv_data:
            for cell in row:
                links = self._extract_links_from_text(cell.strip())
                rss_links.update(links)
        return rss_links
    
    def _parse_pdf_content(self, content):
        rss_links = set()
        pdf_file = PdfFileReader(BytesIO(content))
        for page_num in range(pdf_file.getNumPages()):
            page = pdf_file.getPage(page_num)
            content = page.extractText()
            links = self._extract_links_from_text(content)
            rss_links.update(links)
        return rss_links
    
    def _parse_yaml_content(self, content):
        try:
            data = yaml.safe_load(content)
            if isinstance(data, list):  # Check if the YAML content is a list of URLs
                return set(filter(self._is_valid_url, data))
            else:
                return set()  # Return an empty set if the YAML format is not as expected
        except yaml.YAMLError:
            print("Error parsing the YAML content")
            return set()

    def crawl(self, url=None, depth=0):

        # response = requests.get(url)
        try:
            response = requests.get(url, timeout=10)
        except requests.Timeout:
            print(f"Timeout for URL: {url}")
            return set()
        
        if response.status_code != 200:
            print(f"Failed to fetch content from {url}")
            return set()

        if url.endswith('.csv'):
            # Note: Assuming you have a method named "_parse_csv_content"
            return self._parse_csv_content(response.text)

        if url.endswith(('.yml', '.yaml')):
            # Note: Assuming you have a method named "_parse_yaml_content"
            return self._parse_yaml_content(response.text)

        if url.endswith('.opml'):
            # Note: Assuming you have a method named "_parse_opml_content"
            return self._parse_opml_content(response.text)

        if url.endswith('.txt'):
            return self._extract_links_from_text(response.text)

        if url.endswith('.pdf'):
            # Note: Assuming you have a method named "_parse_pdf_content"
            return self._parse_pdf_content(response.content)

        # If it's none of the above, then parse as HTML
        return self._parse_for_rss_links(response.text, depth)

    def run_crawler(self):
        # Reset counters at the start of a new crawl
        self.TOTAL_WEBSITES = 0
        self.TOTAL_LINKS_FOUND = 0
        self.NEW_LINKS_ADDED = 0
        
        try:
            # Load URLs to be crawled from the database
            print("Connecting to database to fetch websites...")
            with psycopg2.connect(self.DATABASE_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT website FROM {self.RSS_FEED_WEBSITES_TABLE_NAME}")
                urls_to_crawl = [row[0] for row in cursor.fetchall()]
            print(f"Fetched {len(urls_to_crawl)} websites from database.")

            # For storing the links found during crawling
            all_found_links = set()

            # Crawl each website
            for website_url in urls_to_crawl:
                print(f"Crawling website: {website_url}")
                self.TOTAL_WEBSITES += 1
                new_links = self.crawl(website_url)
                all_found_links.update(new_links)
                print(f"Found {len(new_links)} new links from {website_url}. Total links so far: {len(all_found_links)}")

            self.TOTAL_LINKS_FOUND = len(all_found_links)

            # Save the crawled links to the database and count new links
            print("Checking which links are new...")
            with psycopg2.connect(self.DATABASE_PATH) as conn:
                cursor = conn.cursor()

                # Convert the set to a list for indexing
                all_found_links_list = list(all_found_links)

                # Fetch all links that already exist in the database
                query = f"SELECT link FROM {self.RSS_LINKS_TABLE_NAME} WHERE link = ANY(%s)"
                cursor.execute(query, (all_found_links_list,))
                existing_links = {result[0] for result in cursor.fetchall()}

                # Filter out the links that already exist
                new_links = [link for link in all_found_links_list if link not in existing_links]
                self.NEW_LINKS_ADDED = len(new_links)

                # Bulk insert the new links
                if new_links:
                    print("Inserting new links to database...")
                    insert_query = f"INSERT INTO {self.RSS_LINKS_TABLE_NAME} (link) VALUES %s"
                    psycopg2.extras.execute_values(cursor, insert_query, [(link,) for link in new_links])

                conn.commit()
            print(f"Finished saving links to database. Total new links added: {self.NEW_LINKS_ADDED}")

        except Exception as e:
            print(f"An error occurred: {e}")



    def add_to_database(self, url):
        with psycopg2.connect(self.DATABASE_PATH) as conn:
            cursor = conn.cursor()
            print(f"Starting crawl for URL: {url}")
            cursor.execute(f"INSERT INTO {self.RSS_FEED_WEBSITES_TABLE_NAME} (website) VALUES (%s) ON CONFLICT (website) DO NOTHING", (url,))
            conn.commit()

    def add_urls_to_database(self, urls):
        for url in urls:
            self.add_to_database(url)

    def start_crawler(self, urls=None, rss_links=None):

        if urls:
            for url in urls:
                self.add_to_database(url)

        if rss_links:
            for link in rss_links:
                print("inserting RSS LINKS directly")
                # If the URL is an RSS link, directly add it to the RSS links database
                with psycopg2.connect(self.DATABASE_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"INSERT INTO {self.RSS_LINKS_TABLE_NAME} (link) VALUES (%s) ON CONFLICT (link) DO NOTHING", (link,))
                    conn.commit()

        self.run_crawler()
        # conn.close()


if __name__ == "__main__":

    def load_config():
            with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
                return json.load(file)

    config = load_config()

    website_urls = config["params"]["URLS"]
    rss_links = config["params"]["RSS_LINKS"]

    extractor = WebsiteRssFeedExtractor(max_depth=4)
    extractor.start_crawler(website_urls, rss_links)