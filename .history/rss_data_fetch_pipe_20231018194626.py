import requests
import time
import json


class MicroServiceTest:
    def __init__(self, base_url, start_endpoint, start_payload=None, start_method='POST'):
        self.base_url = base_url
        self.start_endpoint = start_endpoint
        self.start_payload = start_payload
        self.start_method = start_method

    def start(self):
        url = f"{self.base_url}/{self.start_endpoint}"
        if self.start_method == 'POST':
            response = requests.post(url, json=self.start_payload)
        else:
            response = requests.get(url, params=self.start_payload)
        
        if response.status_code == 200:
            print(response.json()["message"])
        else:
            print(f"Error triggering service: {response.text}")

    def poll_status(self):
        while True:
            response = requests.get(f"{self.base_url}/status")
            data = response.json()
            print(f"Status: {data['status']}")
            
            # Exit loop when service is idle
            if data['status'] == 'idle':
                break
                
            time.sleep(1)

    def run(self):
        self.start()
        self.poll_status()


# Define the microservices in order
microservices = [
    MicroServiceTest(
        "http://localhost:5001", 
        "start-crawl", 
        {"urls": ["https://github.com/plenaryapp/awesome-rss-feeds", "https://raw.githubusercontent.com/androidsx/micro-rss/master/list-of-feeds.txt"]},
    ),
    MicroServiceTest(
        "http://localhost:5002", 
        "start-rss-read", 
        {"days_to_crawl": 1},
    ),
    MicroServiceTest(
        "http://127.0.0.1:5003", 
        "classify", 
        {"classes": ["News", "Entertainment", "Sports", "Economy", "Technology", "Science", "Stock Market", "Reviews", "Business", "Finance"], "threshold": 0.76},
        start_method='GET'
    ),
    MicroServiceTest(
        "http://localhost:5014", 
        "classify_sentiment", 
        {"classes": None},
        start_method='GET'
    )
]

def run_all_microservices():
    for service in microservices:
        service.run()


# This is just a function to run the microservices. 
# Uncomment the next line to actually execute it.
# run_all_microservices()
