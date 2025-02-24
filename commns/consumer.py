import os
import json
import requests
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import DefaultAzureCredential

# Constants
ACCOUNT_URL = "https://treeivision.blob.core.windows.net"
QUEUE_ACCOUNT_URL = "https://treeivision.queue.core.windows.net"
CONTAINER_NAME = "imgstree"
QUEUE_NAME = "imgtest"
BASE_PATH = "commns/received"

# Authenticate using DefaultAzureCredential
CREDENTIAL = DefaultAzureCredential()

# Initialize QueueClient to read messages from the queue
queue_client = QueueClient(account_url=QUEUE_ACCOUNT_URL, queue_name=QUEUE_NAME, credential=CREDENTIAL)

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=CREDENTIAL)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def download_images(group_id, image_urls):
    # Create a folder with the group_id name
    folder_path = os.path.join(BASE_PATH, group_id)
    os.makedirs(folder_path, exist_ok=True)
    
    # Download each image
    for image_url in image_urls:
        # Get the image name from the URL
        image_name = image_url.split('/')[-1]
        
        try:
            # Create BlobClient for the specific blob
            blob_path = image_name  # Assuming the image_url's last segment is the blob name
            blob_client = container_client.get_blob_client(blob_path)
            
            # Download the blob
            image_path = os.path.join(folder_path, image_name)
            with open(image_path, 'wb') as download_file:
                download_data = blob_client.download_blob()
                download_file.write(download_data.readall())
            
            print(f"Downloaded {image_name} to {image_path}")
            
        except Exception as e:
            print(f"Error downloading {image_url}: {e}")

def process_queue_message():
    # Receive the message from the queue
    messages = queue_client.receive_messages()
    
    for message in messages:
        # Parse the message
        message_content = json.loads(message.content)
        group_id = message_content['group_id']
        image_urls = message_content['image_urls']
        
        # Download the images associated with this group_id
        download_images(group_id, image_urls)
        
        queue_client.delete_message(message)
        
        print(f"Processed message with group_id {group_id} and {len(image_urls)} image URLs.")

# Process messages from the queue
process_queue_message()