import os
import json
import random
import string
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient
from azure.identity import DefaultAzureCredential

# Constants
ACCOUNT_URL:str = "https://treeivision.blob.core.windows.net"
QUEUE_ACCOUNT_URL:str = "https://treeivision.queue.core.windows.net"
CONTAINER_NAME:str = "imgstree"
QUEUE_NAME:str = "input"
FOLDER_PATH:str = "app/to_send"  # Path to the folder containing the images
group_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

# Authenticate using DefaultAzureCredential
CREDENTIAL = DefaultAzureCredential()


blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=CREDENTIAL)
queue_client = QueueClient(account_url=QUEUE_ACCOUNT_URL, queue_name=QUEUE_NAME, credential=CREDENTIAL)

# Get all image file paths in the specified folder
image_paths = [os.path.join(FOLDER_PATH, filename) for filename in os.listdir(FOLDER_PATH) if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))]


image_urls = []
# Upload images to Blob Storage
for image_path in image_paths:
    # Extract the blob name from the image path (you can customize this if needed)
    blob_name = os.path.basename(image_path)

    # Get a reference to the container and the blob
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the image
    with open(image_path, "rb") as data:
        blob_client.upload_blob(data)

    # Construct the URL of the uploaded image
    image_url = f"{ACCOUNT_URL}/{CONTAINER_NAME}/{blob_name}"
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