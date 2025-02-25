import os
import json
import asyncio
from datetime import datetime
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from typing import List

from app.config.config import Config

class AzureManager:
 
    def __init__(self, input_blob_account_url: str, input_queue_account_url: str, input_container_name: str, 
             input_queue_name: str, output_blob_account_url: str, output_queue_account_url: str, 
             output_container_name: str, output_queue_name: str, job_dir: str, output_dir:str, max_retries: int = 3):

        """
        Initialize the AzureManager.
        
        Args:
            input_blob_account_url (str): URL of the input blob storage account
            input_queue_account_url (str): URL of the input queue storage account
            input_container_name (str): Name of the input blob container
            input_queue_name (str): Name of the input queue
            output_blob_account_url (str): URL of the output blob storage account
            output_queue_account_url (str): URL of the output queue storage account
            output_container_name (str): Name of the output blob container
            output_queue_name (str): Name of the output queue
            output_dir (str): Local directory to save processed images
            max_retries (int): Maximum number of retries for failed messages
        """
        self.input_blob_account_url = input_blob_account_url
        self.input_queue_account_url = input_queue_account_url
        self.input_container_name = input_container_name
        self.input_queue_name = input_queue_name

        self.output_blob_account_url = output_blob_account_url
        self.output_queue_account_url = output_queue_account_url
        self.output_container_name = output_container_name
        self.output_queue_name = output_queue_name

        self.job_dir = job_dir
        self.output_dir = output_dir
        self.max_retries = max_retries

        # Default authentication credentials
        self.credential = DefaultAzureCredential()
        
        # Initialize queue clients
        self.input_queue_client = QueueClient(account_url=self.input_queue_account_url, queue_name=self.input_queue_name,
                                                credential=self.credential) 
        
        self.output_queue_client = QueueClient(account_url=self.output_queue_account_url, queue_name=self.output_queue_name,
                                                credential=self.credential)
        
        # Initialize blob clients
        self.input_blob_client = BlobServiceClient(account_url=self.input_blob_account_url, credential=self.credential)
        self.input_container_client = self.input_blob_client.get_container_client(self.input_container_name)
        
        self.output_blob_client = BlobServiceClient(account_url=self.output_blob_account_url, credential=self.credential)
        self.output_container_client = self.output_blob_client.get_container_client(self.output_container_name)
        
        
        # Dictionary to track message retry counts (message_id: retry_count)
        self.retry_tracker = {}
    
    async def process_queue_message(self, message_content: str):
        """
        Process a single queue message.
        
        Args:
            message_content (str): Content of the queue message (JSON string)
            
        Returns:
            tuple: (success_flag, error_message)
        """
        try:
            # Parse the JSON message
            message_data = json.loads(message_content)
            
            target_id:str = message_data.get("TargetId")
            img_paths: List[str] = message_data.get("Photos", [])
            
            if not target_id or not img_paths:
                return False, "Invalid message format: missing id_group or img_paths"
            
            target_folder = os.path.join(self.job_dir, target_id)
            os.makedirs(target_folder, exist_ok=True)
            
            download_tasks = []
            for i, img_url in enumerate(img_paths):
                # Extract filename from URL or create a sequential filename
                filename = os.path.basename(img_url.split('?')[0]) or f"image_{i}.jpg"
                destination_path = os.path.join(target_folder, filename)
                download_tasks.append(self.download_image(img_url, destination_path))
            
            # Wait for all downloads to complete
            results = await asyncio.gather(*download_tasks)
            
            # Collect errors if any
            errors = []
            for success, error in results:
                if not success and error:
                    errors.append(error)
                    
            # Check if all downloads were successful
            if not errors:
                print(f"Successfully processed message for group {target_id}")
                return True, target_folder
            else:
                error_msg = f"Some downloads failed for group {target_id}: {'; '.join(errors)}"
                print(error_msg)
                return False, error_msg
                
        except json.JSONDecodeError:
            return False, "Invalid JSON in message"
        except Exception as e:
            error_msg = f"Error processing message: {str(e)}"
            print(error_msg)
            return False, error_msg
        
    async def process_job(self, message_content):
        """
        Process a single job from the queue.
        
        Args:
            message_content (str): Content of the queue message (JSON string)
            
        Returns:
            tuple: (success_flag, error_message)
        """
        try:
            content = "Null"
            success, content = await self.process_queue_message(message_content)
            
            if success:
                # return the output folder path to be uploaded to blob storage
                target_folder = content
                output_folder = os.path.join(self.output_dir, os.path.basename(target_folder))

                try:
                    # call the ML algorithm
                    pass
                except Exception as e:
                    error_msg = f"Error on the ML algorithm: {str(e)}"
                    print(error_msg)
                    return False, error_msg

                return True, output_folder    
            
            return False, content
        
        except Exception as e:
            error_msg = f"Error processing job: {str(e)}"
            print(error_msg)
            return False, content
        
    async def download_image(self, blob_url, destination_path):
        """
        Download an image from blob storage to the local filesystem.
        
        Args:
            blob_url (str): URL of the blob to download
            destination_path (str): Local path to save the downloaded file
            
        Returns:
            tuple: (success_flag, error_message)
        """
        try:
            # Create a blob client using the blob URL
            blob_name = blob_url.split('/')[-1]
            blob_client = self.input_container_client.get_blob_client(blob_name)
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            # Download the blob to the specified path
            with open(destination_path, "wb") as download_file:
                blob_data = blob_client.download_blob()
                download_file.write(blob_data.readall())
            
            print(f"Downloaded: {blob_url} to {destination_path}")
            return True, None
        except Exception as e:
            error_msg = f"Error downloading {blob_url}: {str(e)}"
            print(error_msg)
            return False, error_msg
        
    async def send_error(self, message_content, error_message, retry_count):
        """
        Send a failed message to the output queue with error information.
        
        Args:
            message_content (str): Original message content
            error_message (str): Error that caused the failure
        Returns:
            bool: Success flag
        """
        try:
            # Parse the original message
            message_data = json.loads(message_content)
            
            # Add error information
            dead_letter_message = {
                "error": error_message,
                "original_message": message_data,
                "failed_at": datetime.now().isoformat()
            }
            
            # Send to error message to queue
            self.output_queue_client.send_message(json.dumps(dead_letter_message))
            
            target_id = message_data.get("TargetId", "unknown")
            print(f"Sent error message for target {target_id} to queue after {retry_count} retries")
            return True
        except Exception as e:
            print(f"Error sending to dead letter queue: {str(e)}")
            return False

    async def verify_azure_resources(self):
        """
        Verify that the required Azure resources exist and are accessible.
        
        Returns:
            bool: Success flag
        """
        try:
            # Verify input blob container
            self.input_container_client.get_container_properties()
            print(f"Verified input container: {self.input_container_name}")
            
            # Verify output blob container
            self.output_container_client.get_container_properties()
            print(f"Verified output container: {self.output_container_name}")
            
            # Verify input queue
            self.input_queue_client.get_queue_properties()
            print(f"Verified input queue: {self.input_queue_name}")
            
            # Verify output queue
            self.output_queue_client.get_queue_properties()
            print(f"Verified output queue: {self.output_queue_name}")
            
            return True
        except ResourceNotFoundError as e:
            print(f"Error verifying resources: {str(e)}")
            return False
    
    async def send_end_flag(self):
        """
        Send a special message to the output queue to indicate the end of processing.
        
        Returns:
            bool: Success flag
        """
        try:
            end_message = {
                "end_of_processing": True,
                "timestamp": datetime.now().isoformat()
            }
            
            self.output_queue_client.send_message(json.dumps(end_message))
            print("Sent end of processing flag")
            return True
        except Exception as e:
            print(f"Error sending end flag: {str(e)}")
            return False

    async def upload_to_blob_storage(self, target_folder):
        """
        Upload the processed images to the output blob storage.
        
        Args:
            target_folder (str): Local directory containing processed images
        Returns:
            bool: Success flag
        """
        try:
            for root, _, files in os.walk(target_folder):
                for file in files:
                    local_path = os.path.join(root, file)
                    blob_name = os.path.relpath(local_path, target_folder)
                    blob_client = self.output_blob_client.get_blob_client(blob_name)
                    
                    with open(local_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
                    
                    print(f"Uploaded {local_path} to {blob_name}")
            return True
        except Exception as e:
            print(f"Error uploading to blob storage: {str(e)}")
            return False
        
    async def send_message_to_output_queue(self, target_folder):
        """
        Send a message to the output queue with information about the processed images.
        """
        pass

    async def process_queue(self):
        """
        Main processing loop to poll the queue and process messages.
        """
        try:
            await self.verify_azure_resources()

            print(f"Starting to process messages from queue: {self.input_queue_name}")

            while True:
                # Get messages from the queue
                messages = self.input_queue_client.receive_messages()

                message_processed = False
                for message in messages:  # Iterate over received messages
                    message_processed = True
                    message_id = message.id
                    pop_receipt = message.pop_receipt

                    current_retries = self.retry_tracker.get(message_id, 0)

                    print(f"Processing message ID: {message_id} (Attempt {current_retries + 1}/{self.max_retries})")
                    success, content = await self.process_job(message.content)

                    if success:
                        # Upload the processed images to the output blob storage
                        await self.upload_to_blob_storage(content)
                        # Send the message to the output queue
                        await self.send_message_to_output_queue(content)

                        # Delete the message from the queue
                        self.input_queue_client.delete_message(message)
                        print(f"Successfully processed and deleted message ID: {message_id}")

                        # Remove from retry tracker if it was there
                        if message_id in self.retry_tracker:
                            del self.retry_tracker[message_id]
                    else:
                        # Update retry count
                        self.retry_tracker[message_id] = current_retries + 1

                        # After error, make message immediately visible again
                        response = self.input_queue_client.update_message(
                            message.id, 
                            message.pop_receipt,
                            visibility_timeout=0
                        )
                        pop_receipt = response.pop_receipt

                        # Check if we've reached max retries
                        if self.retry_tracker[message_id] >= self.max_retries:
                            # Send to dead letter queue
                            await self.send_error(
                                message.content, #FIXME
                                content,
                                self.retry_tracker[message_id]
                            )

                            # Delete from original queue
                            self.input_queue_client.delete_message(message_id, pop_receipt)

                            # Remove from retry tracker
                            del self.retry_tracker[message_id]
                        else:
                            print(f"Processing failed for message ID: {message_id}, will retry later (Attempt {self.retry_tracker[message_id]}/{self.max_retries})")

                # if no remaining messages, send the end flag
                if not message_processed:
                    print(f"No messages found... Sending the end flag")
                    await self.send_end_flag()
                    break

        except Exception as e:
            print(f"Error in main process: {str(e)}")


# Example usage
async def main():
    # Configuration
    processor = AzureManager(
        input_blob_account_url= Config.I_BLOB_ACCOUNT_URL,
        input_queue_account_url= Config.I_QUEUE_ACCOUNT_URL,
        input_container_name= Config.I_CONTAINER_NAME,
        input_queue_name= Config.I_QUEUE_NAME,
        output_blob_account_url= Config.O_BLOB_ACCOUNT_URL,
        output_queue_account_url= Config.O_QUEUE_ACCOUNT_URL,
        output_container_name= Config.O_CONTAINER_NAME,
        output_queue_name= Config.O_QUEUE_NAME,
        job_dir="app/jobs",
        output_dir="app/finished_jobs",
        max_retries=3
    )
    
    # Start processing
    await processor.process_queue()

if __name__ == "__main__":
    asyncio.run(main())