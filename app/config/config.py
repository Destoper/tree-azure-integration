import os
from dotenv import load_dotenv
from dataclasses import dataclass

# Load environment variables from .env file (if you are using it)
load_dotenv(dotenv_path="app/.env")

@dataclass
class Config: 

    # input storage account
    I_CONNECTION_STRING = os.getenv("I_CONNECTION_STRING")
    I_BLOB_ACCOUNT_URL = os.getenv("I_BLOB_ACCOUNT_URL")
    I_QUEUE_ACCOUNT_URL = os.getenv("I_QUEUE_ACCOUNT_URL")
    I_CONTAINER_NAME = os.getenv("I_CONTAINER_NAME")
    I_QUEUE_NAME = os.getenv("I_QUEUE_NAME")

    # output storage account
    O_CONNECTION_STRING = os.getenv("O_CONNECTION_STRING")
    O_BLOB_ACCOUNT_URL = os.getenv("O_BLOB_ACCOUNT_URL")
    O_QUEUE_ACCOUNT_URL = os.getenv("O_QUEUE_ACCOUNT_URL")
    O_CONTAINER_NAME = os.getenv("O_CONTAINER_NAME")
    O_QUEUE_NAME = os.getenv("O_QUEUE_NAME")

    # path to the local folders
    JOB_DIR = os.getenv("JOB_DIR")
    OUTPUT_DIR = os.getenv("OUTPUT_DIR")

    # max entries
    MAX_ENTRIES = int(os.getenv("MAX_ENTRIES"))