import os
import json
import asyncio
import logging
from datetime import datetime
from azure.storage.queue import QueueServiceClient
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from typing import List, Dict, Tuple, Optional, Any
import aiofiles

from config.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AzureManager")

# Disable verbose Azure loggers
logging.getLogger("azure").setLevel(logging.WARNING)

class AzureManager:
    """Manages Azure blob and queue operations for processing jobs."""
 
    def __init__(self, input_blob_conn_str: str, input_container_name: str, 
             input_queue_name: str, output_blob_conn_str: str, 
             output_container_name: str, output_queue_name: str, 
             job_dir: str, output_dir: str, max_retries: int = 3):
        """
        Initialize the AzureManager with connection strings.
        
        Args:
            input_blob_conn_str (str): Connection string for input blob/queue storage account
            input_container_name (str): Name of the input blob container
            input_queue_name (str): Name of the input queue
            output_blob_conn_str (str): Connection string for output blob/queue storage account
            output_container_name (str): Name of the output blob container
            output_queue_name (str): Name of the output queue
            job_dir (str): Local directory to save downloaded images
            output_dir (str): Local directory to save the ML results
            max_retries (int): Maximum number of retries for failed messages
        """
        # Input storage configuration
        self.input_blob_conn_str = input_blob_conn_str
        self.input_container_name = input_container_name
        self.input_queue_name = input_queue_name

        # Output storage configuration  
        self.output_blob_conn_str = output_blob_conn_str
        self.output_container_name = output_container_name
        self.output_queue_name = output_queue_name

        self.job_dir = job_dir
        self.output_dir = output_dir
        self.max_retries = max_retries

        os.makedirs(self.job_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize clients
        self._initialize_clients()
        
        # Dictionary to track message retry counts (message_id: retry_count)
        self.retry_tracker: Dict[str, int] = {}

    def _initialize_clients(self) -> None:
        """Initialize Azure queue and blob clients using connection strings."""
        try:
            # Initialize queue clients
            input_queue_service = QueueServiceClient.from_connection_string(self.input_blob_conn_str)
            self.input_queue_client = input_queue_service.get_queue_client(self.input_queue_name)
            
            output_queue_service = QueueServiceClient.from_connection_string(self.output_blob_conn_str)
            self.output_queue_client = output_queue_service.get_queue_client(self.output_queue_name)
            
            # Initialize blob clients
            self.input_blob_client = BlobServiceClient.from_connection_string(self.input_blob_conn_str)
            self.input_container_client = self.input_blob_client.get_container_client(
                self.input_container_name
            )
            
            self.output_blob_client = BlobServiceClient.from_connection_string(self.output_blob_conn_str)
            self.output_container_client = self.output_blob_client.get_container_client(
                self.output_container_name
            )
            
            logger.info("Successfully initialized Azure clients")
        except Exception as e:
            logger.error(f"Failed to initialize Azure clients: {str(e)}")
            raise
    
    def _validate_message(self, message_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate message data format.
        
        Args:
            message_data (Dict): Parsed message JSON
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        if not isinstance(message_data, dict):
            return False, "Message data is not a dictionary"
            
        target_id = message_data.get("TargetId")
        if not target_id or not isinstance(target_id, str):
            return False, "Invalid or missing TargetId"
            
        img_paths = message_data.get("Photos")
        if not img_paths or not isinstance(img_paths, list) or len(img_paths) == 0:
            return False, "Invalid or missing Photos list"
            
        for path in img_paths:
            if not isinstance(path, str):
                return False, "Image path must be a string"
                
        return True, ""
    
    async def process_queue_message(self, message_content: str) -> Tuple[bool, str]:
        """
        Process a single queue message.
        
        Args:
            message_content (str): Content of the queue message (JSON string)
            
        Returns:
            Tuple[bool, str]: (success_flag, result_or_error_message)
        """
        try:
            try:
                message_data = json.loads(message_content)
            except json.JSONDecodeError:
                return False, "Invalid JSON in message"
            
            # Validate message format
            is_valid, error_message = self._validate_message(message_data)
            if not is_valid:
                return False, error_message
            
            target_id: str = message_data.get("TargetId")
            img_paths: List[str] = message_data.get("Photos", [])
            
            target_folder = os.path.join(self.job_dir, target_id)
            os.makedirs(target_folder, exist_ok=True)
            

            download_tasks = []
            for i, img_url in enumerate(img_paths):
                # Extract filename from URL or create a sequential filename
                filename = os.path.basename(img_url.split('?')[0]) or f"image_{i}.jpg"
                destination_path = os.path.join(target_folder, filename)
                download_tasks.append(self.download_image(img_url, destination_path))

            results = await asyncio.gather(*download_tasks)

            errors = []
            for success, error in results:
                if not success and error:
                    errors.append(error)
                    
            if not errors:
                logger.info(f"Successfully processed message for group {target_id}")
                return True, target_folder
            else:
                error_msg = f"Some downloads failed for group {target_id}: {'; '.join(errors)}"
                logger.error(error_msg)
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Error processing message: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
        
    async def process_job(self, message_content: str) -> Tuple[bool, str]:
        """
        Process a single job from the queue.
        
        Args:
            message_content (str): Content of the queue message (JSON string)
            
        Returns:
            Tuple[bool, str]: (success_flag, output_folder_or_error)
        """
        try:
            success, content = await self.process_queue_message(message_content)
            
            if success:
                target_folder = content
                target_id = os.path.basename(target_folder)
                output_folder = os.path.join(self.output_dir, target_id)
                os.makedirs(output_folder, exist_ok=True)

                try:
                    #TODO: INTREGRATION 
                    # Call the ML algorithm here
                    # This is a placeholder for the actual ML processing
                    logger.info(f"Starting ML processing for {target_id}")
                    
                    # Example: Simple file copying as placeholder for ML processing
                    for filename in os.listdir(target_folder):
                        source_path = os.path.join(target_folder, filename)
                        if os.path.isfile(source_path):
                            dest_path = os.path.join(output_folder, f"processed_{filename}")
                            async with aiofiles.open(source_path, 'rb') as src_file:
                                content = await src_file.read()
                                async with aiofiles.open(dest_path, 'wb') as dest_file:
                                    await dest_file.write(content)
                    
                    logger.info(f"ML processing completed for {target_id}")
                    
                except Exception as e:
                    error_msg = f"Error in ML algorithm for {target_id}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    return False, error_msg

                return True, output_folder    
            
            return False, content
        
        except Exception as e:
            error_msg = f"Error processing job: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
        
    async def download_image(self, blob_url: str, destination_path: str) -> Tuple[bool, Optional[str]]:
        """
        Download an image from blob storage to the local filesystem.
        
        Args:
            blob_url (str): URL of the blob to download
            destination_path (str): Local path to save the downloaded file
            
        Returns:
            Tuple[bool, Optional[str]]: (success_flag, error_message)
        """
        try:
            if not blob_url or not isinstance(blob_url, str):
                return False, "Invalid blob URL"
                
            # Extract blob name from URL
            blob_name = blob_url.split('/')[-1].split('?')[0]
            if not blob_name:
                return False, "Could not extract blob name from URL"
                
            blob_client = self.input_container_client.get_blob_client(blob_name)
            
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            
            async with aiofiles.open(destination_path, "wb") as download_file:
                blob_data = blob_client.download_blob()
                content = blob_data.readall()
                await download_file.write(content)
            
            logger.info(f"Downloaded: {blob_url} to {destination_path}")
            return True, None
        except ResourceNotFoundError:
            error_msg = f"Blob not found: {blob_url}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Error downloading {blob_url}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
        
    async def send_error(self, message_content: str, error_message: str, retry_count: int) -> bool:
        """
        Send a failed message to the output queue with error information.
        
        Args:
            message_content (str): Original message content
            error_message (str): Error that caused the failure
            retry_count (int): Number of retries attempted
            
        Returns:
            bool: Success flag
        """
        try:
            # Parse the original message if it's valid JSON
            try:
                message_data = json.loads(message_content)
                target_id = message_data.get("TargetId", "unknown")
            except (json.JSONDecodeError, TypeError):
                # If the message is not valid JSON, use it as-is
                message_data = message_content
                target_id = "unknown"
            
            # Create the dead letter message
            dead_letter_message = {
                "error": error_message,
                "original_message": message_data,
                "retry_count": retry_count,
                "failed_at": datetime.now().isoformat()
            }
            
            # Send to error message to queue
            self.output_queue_client.send_message(json.dumps(dead_letter_message))
            
            logger.info(f"Sent error message for target {target_id} to queue after {retry_count} retries")
            return True
        except Exception as e:
            logger.error(f"Error sending to dead letter queue: {str(e)}", exc_info=True)
            return False

    async def verify_azure_resources(self) -> bool:
        """
        Verify that the required Azure resources exist and are accessible.
        
        Returns:
            bool: Success flag
        """
        try:
            # Verify input blob container
            self.input_container_client.get_container_properties()
            logger.info(f"Verified input container: {self.input_container_name}")
            
            # Verify output blob container
            self.output_container_client.get_container_properties()
            logger.info(f"Verified output container: {self.output_container_name}")
            
            # Verify input queue
            self.input_queue_client.get_queue_properties()
            logger.info(f"Verified input queue: {self.input_queue_name}")
            
            # Verify output queue
            self.output_queue_client.get_queue_properties()
            logger.info(f"Verified output queue: {self.output_queue_name}")
            
            return True
        except ResourceNotFoundError as e:
            logger.error(f"Error verifying resources: {str(e)}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error verifying resources: {str(e)}", exc_info=True)
            return False
    
    async def send_end_flag(self) -> bool:
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
            logger.info("Sent end of processing flag")
            return True
        except Exception as e:
            logger.error(f"Error sending end flag: {str(e)}", exc_info=True)
            return False

    async def upload_to_blob_storage(self, target_folder: str) -> bool:
        """
        Upload the processed images to the output blob storage.
        
        Args:
            target_folder (str): Local directory containing processed images
            
        Returns:
            bool: Success flag
        """
        if not os.path.exists(target_folder):
            logger.error(f"Target folder does not exist: {target_folder}")
            return False
            
        try:
            #TODO: INTREGRATION 
            upload_tasks = []
            upload_results = []
            
            for root, _, files in os.walk(target_folder):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, target_folder)
                    blob_name = os.path.join(os.path.basename(target_folder), relative_path)
                    
                    # Get blob client
                    blob_client = self.output_container_client.get_blob_client(blob_name)
                    
                    # Read file content
                    async with aiofiles.open(local_path, "rb") as data:
                        file_content = await data.read()
                        # Upload blob
                        blob_client.upload_blob(file_content, overwrite=True)
                        logger.info(f"Uploaded {local_path} to {blob_name}")
                        upload_results.append(True)
            
            return all(upload_results) if upload_results else False
            
        except Exception as e:
            logger.error(f"Error uploading to blob storage: {str(e)}", exc_info=True)
            return False
        
    async def send_message_to_output_queue(self, target_folder: str) -> bool:
        """
        Send a message to the output queue with information about the processed images.
        
        Args:
            target_folder (str): Local directory containing processed images
            
        Returns:
            bool: Success flag
        """
        try:
            #TODO: INTREGRATION 
            target_id = os.path.basename(target_folder)
            
            processed_files = []
            for root, _, files in os.walk(target_folder):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, target_folder)
                    blob_name = os.path.join(target_id, relative_path)
                    processed_files.append(blob_name)
            
            message = {
                "TargetId": target_id,
                "ProcessedFiles": processed_files,
                "ProcessedAt": datetime.now().isoformat(),
                "Status": "Completed"
            }
            
            self.output_queue_client.send_message(json.dumps(message))
            logger.info(f"Sent completion message for {target_id} to output queue")
            return True
            
        except Exception as e:
            logger.error(f"Error sending message to output queue: {str(e)}", exc_info=True)
            return False

    async def process_queue(self) -> None:
        """
        Main processing loop to poll the queue and process messages.
        """
        try:
            resources_verified = await self.verify_azure_resources()
            if not resources_verified:
                logger.error("Failed to verify Azure resources. Exiting.")
                return

            logger.info(f"Starting to process messages from queue: {self.input_queue_name}")

            while True:
                messages = self.input_queue_client.receive_messages(messages_per_page=10)
                message_processed = False
                
                for message in messages:
                    message_processed = True
                    message_id = message.id
                    
                    current_retries = self.retry_tracker.get(message_id, 0)
                    logger.info(f"Processing message ID: {message_id} (Attempt {current_retries + 1}/{self.max_retries})")
                    
                    success, content = await self.process_job(message.content)

                    if success:
                        upload_success = await self.upload_to_blob_storage(content)
                        if not upload_success:
                            logger.error(f"Failed to upload processed files for message {message_id}")
                            self.retry_tracker[message_id] = current_retries + 1
                            continue
                        
                        queue_success = await self.send_message_to_output_queue(content)
                        if not queue_success:
                            logger.error(f"Failed to send output message for {message_id}")
                            self.retry_tracker[message_id] = current_retries + 1
                            continue

                        self.input_queue_client.delete_message(message)
                        logger.info(f"Successfully processed and deleted message ID: {message_id}")

                        if message_id in self.retry_tracker:
                            del self.retry_tracker[message_id]
                    else:

                        self.retry_tracker[message_id] = current_retries + 1
                        
                        if self.retry_tracker[message_id] >= self.max_retries:
                            await self.send_error(
                                message.content,
                                content,
                                self.retry_tracker[message_id]
                            )

                            # Delete from original queue
                            self.input_queue_client.delete_message(message)
                            logger.info(f"Message {message_id} exceeded max retries. Moved to error queue.")

                            del self.retry_tracker[message_id]
                        else:
                            # Update visibility timeout to retry later
                            self.input_queue_client.update_message(
                                message.id, 
                                message.pop_receipt,
                                visibility_timeout=0
                            )
                            logger.info(f"Processing failed for message ID: {message_id}, will retry later (Attempt {self.retry_tracker[message_id]}/{self.max_retries})")

                # Check if we should send end flag and exit
                if not message_processed:
                    logger.info("No messages found in queue. Sending end flag.")
                    await self.send_end_flag()
                    break
                
                # Small pause between polling to avoid hammering the queue
                await asyncio.sleep(1)

        except Exception as e:
            logger.critical(f"Critical error in main process: {str(e)}", exc_info=True)
            await self.send_error("Main process", str(e), 0)

async def main():
    try:
        # Configuration
        processor = AzureManager(
            input_blob_conn_str=Config.STORAGE_CONNECTION_STRING,
            input_container_name=Config.I_CONTAINER_NAME,
            input_queue_name=Config.I_QUEUE_NAME,
            output_blob_conn_str=Config.STORAGE_CONNECTION_STRING,
            output_container_name=Config.O_CONTAINER_NAME,
            output_queue_name=Config.O_QUEUE_NAME,
            job_dir=Config.JOB_DIR,
            output_dir=Config.OUTPUT_DIR,
            max_retries=Config.MAX_ENTRIES
        )
        
        await processor.process_queue()
    except Exception as e:
        logger.critical(f"Application failed to start: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())