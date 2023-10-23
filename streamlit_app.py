
import streamlit as st
import json
import subprocess

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

    params = config_data["params"]
    for key, value in params.items():
        if isinstance(value, list):
            params[key] = st.multiselect(f"Params: {key}", value, default=value)
        else:
            params[key] = st.text_input(f"Params: {key}", value)

    suffixes = config_data["suffixes"]
    for key, value in suffixes.items():
        suffixes[key] = st.text_input(f"Suffixes: {key}", value)

    # Save button
    if st.button("Save"):
        save_config(config_data)
        st.success("Configuration saved!")

    # Execute main_data_pipeline.py with new config
    if st.button("Run Main Data Pipeline"):
        try:
            result = subprocess.run(["python", "main_data_pipeline.py"], capture_output=True, text=True)
            st.write(result.stdout)
            st.success("Main Data Pipeline executed!")
        except Exception as e:
            st.error(f"Error executing Main Data Pipeline: {e}")

if __name__ == "__main__":
    main()
