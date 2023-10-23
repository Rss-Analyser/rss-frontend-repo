CREATE DATABASE rss_db;

USE rss_db;

-- Table for rss links
CREATE TABLE rss_links (
    id SERIAL PRIMARY KEY,
    link TEXT NOT NULL
);

-- Table for rss feed websites
CREATE TABLE rss_feed_websites (
    id SERIAL PRIMARY KEY,
    website TEXT NOT NULL
);

-- -- Table for rss entries for specific dates (example for 20231010)
-- CREATE TABLE rss_entries_20231010 (
--     id SERIAL PRIMARY KEY,
--     publisher TEXT,
--     title TEXT,
--     link TEXT,
--     published TEXT,
--     language TEXT,
--     class TEXT,
--     Similarity FLOAT,
--     Embedding TEXT,
--     Sentiment TEXT
-- );

-- Table for sentiment indicators
CREATE TABLE sentiment_indicators (
    id SERIAL PRIMARY KEY,
    class TEXT NOT NULL,
    sentiment_index FLOAT NOT NULL,
    entry_count INT NOT NULL
);

ALTER TABLE rss_feed_websites ADD CONSTRAINT unique_website UNIQUE (website);

ALTER TABLE rss_links ADD CONSTRAINT unique_link UNIQUE (link);



-- Additional tables can be created similarly
