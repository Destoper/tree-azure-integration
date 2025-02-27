from dataclasses import dataclass

@dataclass
class Constants:
    # General
    MAX_DEFAULT_RETRIES = 3
    POLLING_INTERVAL_SECONDS = 1
    QUEUE_MESSAGES_PER_PAGE = 10
    
    # File and directory naming
    IMAGE_FILENAME_TEMPLATE = "image_{}.jpg"
    PROCESSED_FILENAME_PREFIX = "processed_"
    SCENE_ZIP_FILENAME = "scene.zip"
    SCENE_OUTPUT_FILENAME_TEMPLATE = "scene_{}.zip"
    
    # JSON keys
    KEY_TARGET_ID = "TargetId"
    KEY_PHOTOS = "Photos"
    KEY_ERROR = "error"
    KEY_ORIGINAL_MESSAGE = "original_message"
    KEY_RETRY_COUNT = "retry_count"
    KEY_FAILED_AT = "failed_at"
    KEY_END_OF_PROCESSING = "end_of_processing"
    KEY_TIMESTAMP = "timestamp"
    KEY_SCENE_URL = "SceneUrl"
    KEY_PROCESSED_AT = "ProcessedAt"
    KEY_STATUS = "Status"
    
    # Status values
    STATUS_COMPLETED = "Completed"
    
    # Default values
    DEFAULT_TARGET_ID = "unknown"