 #!/bin/bash
sudo yum update -y
sudo yum install python3 -y
sudo yum install python-pip -y

sudo pip install streamlit
sudo pip install psycopg2-binary
sudo pip install plotly

sudo yum install ec2-instance-connect

# Install CockroachDB

wget -qO- https://binaries.cockroachdb.com/cockroach-v21.1.9.linux-amd64.tgz | tar xvz
sudo cp -i cockroach-v21.1.9.linux-amd64/cockroach /usr/local/bin/

# Initialize the Cluster with the first node
cockroach start \
--insecure \
--store=node1 \
--listen-addr=localhost \
--http-addr=localhost:8080 \
--join=localhost:26257,localhost:26258,localhost:26259 \
--background

# Start and Join the second node to the cluster
cockroach start \
--insecure \
--store=node2 \
--listen-addr=localhost:26258 \
--http-addr=localhost:8081 \
--join=localhost:26257,localhost:26258,localhost:26259 \
--background

# Start and Join the third node to the cluster
cockroach start \
--insecure \
--store=node3 \
--listen-addr=localhost:26259 \
--http-addr=localhost:8082 \
--join=localhost:26257,localhost:26258,localhost:26259 \
--background

git clone https://github.com/Rss-Analyser/rss-infrastructure-repo.git

# Execute the setup_db.sql
cockroach sql --insecure < /home/ec2-user/rss-infrastructure-repo/setup_db.sql

git clone https://github.com/Rss-Analyser/rss-frontend-repo.git
(crontab -l 2>/dev/null; echo "0 * * * * python3 rss-frontend-repo/rss_data_fetch_pipe.py") | crontab -

# Start the streamlit app (you might need to adjust this based on how Streamlit is installed and started)

sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8501

sudo ~/.local/bin/streamlit run rss-frontend-repo/streamlit_sentiment_app.py --server.port 8501 &

git clone https://github.com/Rss-Analyser/rss-sentiment-classifier-microservice
git clone https://github.com/Rss-Analyser/rss-classifier-microservice
git clone https://github.com/Rss-Analyser/rss-feed-crawler-microservice
git clone https://github.com/Rss-Analyser/rss-reader-microservice
cd rss-reader && python3 app_rssReader.py &
cd rss-reader && python3 app_rssFeedCrawler.py &
cd rss-reader && python3 app_rssClassifier.py &
cd rss-reader && python3 app_rssSentimentClassifier.py &