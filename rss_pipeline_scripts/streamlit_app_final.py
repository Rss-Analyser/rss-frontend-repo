
import streamlit as st
import json
import subprocess
from main_data_pipeline import main_pipeline
import sys
import threading
import queue
from io import StringIO
import time

# Context manager for capturing stdout and pushing to queue
class CaptureOutputToQueue:
    def __init__(self, q):
        self.queue = q

    def write(self, data):
        self.queue.put(data)

    def flush(self):
        pass

    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._stdout


def threaded_function(q):
    with CaptureOutputToQueue(q):
        main_pipeline()


# Load config.json
def load_config():
    with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "r") as file:
        return json.load(file)

# Save to config.json
def save_config(data):
    with open("/Users/danieltremer/Documents/RssFeed_Analyser/rss-frontend-repo/rss_pipeline_scripts/config.json", "w") as file:
        json.dump(data, file, indent=4)

# Display and edit config.json via Streamlit frontend
def main():
    st.title("Edit Configuration")

    # Load config data
    config_data = load_config()
    
    # Display and allow editing
    steps = config_data["steps"]
    for key, value in steps.items():
        steps[key] = st.checkbox(f"Step: {key}", value)
    new_step_key = st.text_input("Add New Step Key")
    if new_step_key:
        steps[new_step_key] = st.checkbox(f"Step: {new_step_key}", False)

    params = config_data["params"]
    for key, value in params.items():
        if isinstance(value, list):
            params[key] = st.multiselect(f"Params: {key}", value, default=value)
            new_item = st.text_input(f"Add New Item to {key}")
            if new_item:
                params[key].append(new_item)
        else:
            params[key] = st.text_area(f"Params: {key}", value, height=100)
    new_param_key = st.text_input("Add New Param Key")
    if new_param_key:
        new_param_value = st.text_area(f"Value for Param: {new_param_key}", "", height=100)
        if new_param_value:
            params[new_param_key] = new_param_value

    suffixes = config_data["suffixes"]
    for key, value in suffixes.items():
        suffixes[key] = st.text_area(f"Suffixes: {key}", value, height=100)
    new_suffix_key = st.text_input("Add New Suffix Key")
    if new_suffix_key:
        new_suffix_value = st.text_area(f"Value for Suffix: {new_suffix_key}", "", height=100)
        if new_suffix_value:
            suffixes[new_suffix_key] = new_suffix_value

    # Save button
    if st.button("Save"):
        save_config(config_data)
        st.success("Configuration saved!")

    # Execute main_data_pipeline.py with new config
    if st.button("Run Main Data Pipeline"):
        q = queue.Queue()
        thread = threading.Thread(target=threaded_function, args=(q,))
        thread.start()
        
        output = ""
        console_output_placeholder = st.empty()  # Create a placeholder
        while thread.is_alive() or not q.empty():
            while not q.empty():
                output += q.get()
                console_output_placeholder.text_area("Console Output:", output, height=300)
            time.sleep(0.1)

        if not thread.is_alive():
            st.success("Main Data Pipeline executed!")

if __name__ == "__main__":
    main()
