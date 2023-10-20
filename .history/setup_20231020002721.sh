 #!/bin/bash
sudo yum update -y
sudo yum install python3 -y
sudo yum install python-pip -y

sudo pip install streamlit
sudo pip install psycopg2-binary

sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8501

sudo yum install -y docker git
sudo yum install ec2-instance-connect
sudo systemctl start docker
sudo systemctl enable docker
sudo docker run -p 5001:5001 roboworksolutions/rss-feed-crawler-microservice:latest &
sudo docker run -p 5014:5014 roboworksolutions/rss-sentiment-classifier-microservice:latest &
sudo docker run -p 5003:5003 roboworksolutions/rss-classifier-microservice:latest &
sudo docker run -p 5002:5002 roboworksolutions/rss-reader-microservice:latest &

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

sudo ~/.local/bin/streamlit run rss-frontend-repo/streamlit_sentiment_app.py --server.port 80 &