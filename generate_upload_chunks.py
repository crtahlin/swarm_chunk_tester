import os
import requests
import yaml
import csv
from datetime import datetime

# Load settings from YAML file
with open("settings.yaml", 'r') as stream:
    settings = yaml.safe_load(stream)

# Initialize CSV file based on write_mode
csv_file = settings['swarm']['record_to_file']
write_mode = settings['swarm']['write_mode']
file_mode = 'a' if write_mode == 'append' else 'w'

with open(csv_file, file_mode, newline='') as f:
    writer = csv.writer(f)
    if file_mode == 'w':
        writer.writerow(["reference", "timestamp"])
        
        
# Swarm API details
url = f"{settings['swarm']['url']}/chunks"
headers = {
    "swarm-postage-batch-id": settings['swarm']['postage_batch_id'],
}

# Optionally add swarm-tag if it exists in settings and is an integer
if 'tag' in settings['swarm'] and isinstance(settings['swarm']['tag'], int):
    headers["swarm-tag"] = str(settings['swarm']['tag'])

# Number of chunks to upload
number_of_chunks = settings['swarm']['number_of_chunks']

try:
    for i in range(number_of_chunks):
        # Generate 4096 bytes of random data
        random_data = os.urandom(4096)

        # Upload to Swarm
        response = requests.post(url, headers=headers, data=random_data)
        if response.status_code == 201:
            # Parse JSON to get the actual reference
            reference = response.json()["reference"]
            print(f"Successfully uploaded chunk {i+1}. Address: {reference}")

            # Record the reference and timestamp
            with open(csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                timestamp = datetime.utcnow().isoformat()
                writer.writerow([reference, timestamp])
        else:
            print(f"Failed to upload chunk {i+1}. Status code: {response.status_code}, Response: {response.text}")

except KeyboardInterrupt:
    print("Script interrupted. Closing CSV file.")


