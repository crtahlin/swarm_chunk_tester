import requests
import csv
import yaml
from datetime import datetime
import os

# Load settings from the YAML file
with open('settings.yaml', 'r') as f:
    settings = yaml.safe_load(f)

# Read the existing CSV file for references
references = []
with open(settings['swarm']['record_to_file'], 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip the header
    for row in reader:
        references.append(row[0])

# Initialize the download log CSV file
csv_file = settings['swarm']['record_downloads_to_file']
write_mode_downloads = settings['swarm'].get('write_mode_downloads', 'append')
file_mode = 'a' if write_mode_downloads == 'append' else 'w'

with open(csv_file, file_mode, newline='') as f:
    writer = csv.writer(f, escapechar='\\', quoting=csv.QUOTE_ALL)
    if file_mode == 'w':
        writer.writerow(["reference", "start_timestamp", "end_timestamp", "response_time", "response_status", "response_body"])

# Download chunks from Swarm
try:
    for ref in references:
        print(f"Downloading chunk with reference {ref} ....", end=" ", flush=True)
        url = f"http://localhost:1633/chunks/{ref}"

        start_time = datetime.utcnow()
        start_timestamp = start_time.isoformat()

        response = requests.get(url)
        
        end_time = datetime.utcnow()
        end_timestamp = end_time.isoformat()

        response_time = (end_time - start_time).total_seconds()
        
        print(f"{response.status_code}, Time: {response_time:.4f} seconds")

        # Log download data
        with open(csv_file, 'a', newline='') as f:
            writer = csv.writer(f, escapechar='\\', quoting=csv.QUOTE_ALL)
            writer.writerow([ref, start_timestamp, end_timestamp, response_time, response.status_code])

except KeyboardInterrupt:
    print("Script interrupted. Closing CSV file.")

