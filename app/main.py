import asyncio
from config.config import Config
from AzureManager import AzureManager

async def main():
    try:
        # Configuration
        processor = AzureManager(
            input_blob_conn_str=Config.I_CONNECTION_STRING,
            input_container_name=Config.I_CONTAINER_NAME,
            input_queue_name=Config.I_QUEUE_NAME,
            output_blob_conn_str=Config.O_CONNECTION_STRING,
            output_container_name=Config.O_CONTAINER_NAME,
            output_queue_name=Config.O_QUEUE_NAME,
            job_dir=Config.JOB_DIR,
            output_dir=Config.OUTPUT_DIR,
            max_retries=Config.MAX_ENTRIES
        )
        
        await processor.process_queue()
    except Exception as e:
        print(f"Application failed to start: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
