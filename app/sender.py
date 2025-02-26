import os
import json
import random
import string
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient
from config.config import Config

# Azure Storage Connection String
STORAGE_CONNECTION_STRING = Config.I_CONNECTION_STRING

# Constants
CONTAINER_NAME = "imgstree"
QUEUE_NAME = "input"
FOLDER_PATH = "app/to_send"

# Generate random group ID
group_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

# Create clients using connection string
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
queue_service_client = QueueServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
queue_client = queue_service_client.get_queue_client(QUEUE_NAME)

# Get all image file paths in the specified folder
image_paths = [os.path.join(FOLDER_PATH, filename) 
               for filename in os.listdir(FOLDER_PATH) 
               if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))]

image_urls = []

# Upload images to Blob Storage
for image_path in image_paths:
    # Extract the blob name from the image path
    blob_name = os.path.basename(image_path)
    
    # Get a reference to the container and the blob
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_client = container_client.get_blob_client(blob_name)
    
    # Upload the image
    with open(image_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    # Construct the URL of the uploaded image
    image_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}"
    image_urls.append(image_url)
    
    print(f"Uploaded {blob_name} to {image_url}")

# Create a message with the group ID and the list of image URLs
message = {
    "TargetId": group_id,
    "Photos": image_urls
}

# Send the message to the queue
queue_client.send_message(json.dumps(message))

print(f"Message sent to the queue with group ID {group_id} and {len(image_urls)} image URLs.")