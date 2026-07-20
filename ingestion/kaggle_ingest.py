"""
Kaggle Dataset Ingestion Script
Downloads e-commerce behavior data from Kaggle to MinIO raw bucket.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Try to import python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
    pass


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": getattr(record, "event", record.msg),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add extra fields if they exist
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        
        return json.dumps(log_entry)


def setup_logging():
    """Configure structured JSON logging"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    # Prevent duplicate logs
    logger.propagate = False
    
    return logger


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Ingest Kaggle e-commerce dataset to MinIO raw bucket"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if already ingested"
    )
    
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Directory to store downloaded data (default: ./data)"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)"
    )
    
    return parser.parse_args()

def check_already_ingested(data_dir, logger):
    """
    Check if data has already been ingested.
    Returns: (bool, str) - (is_ingested, marker_path)
    """
    # Create data directory if it doesn't exist
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    
    # Check for success marker
    marker_path = data_path / "_SUCCESS"
    
    if marker_path.exists():
        try:
            with open(marker_path, 'r') as f:
                marker_data = json.load(f)
            logger.info(
                "Found existing ingestion marker",
                extra={
                    "event": "marker_found",
                    "marker_path": str(marker_path),
                    "marker_data": marker_data
                }
            )
            return True, str(marker_path)
        except Exception as e:
            logger.warning(
                "Failed to read marker file",
                extra={
                    "event": "marker_read_error",
                    "error": str(e),
                    "marker_path": str(marker_path)
                }
            )
            return False, str(marker_path)
    
    return False, str(marker_path)


def write_marker(data_dir, metadata, logger):
    """Write success marker after successful ingestion"""
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    
    marker_path = data_path / "_SUCCESS"
    
    marker_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "success",
        **metadata
    }
    
    with open(marker_path, 'w') as f:
        json.dump(marker_data, f, indent=2)
    
    logger.info(
        "Success marker written",
        extra={
            "event": "marker_written",
            "marker_path": str(marker_path),
            "marker_data": marker_data
        }
    )


def download_dataset(api, dataset_name, data_dir, logger):
    """
    Download Kaggle dataset to local directory
    Returns: dict with download metadata
    """
    import time
    from pathlib import Path
    
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(
        "Starting dataset download",
        extra={
            "event": "download_start",
            "dataset": dataset_name,
            "target_dir": str(data_path)
        }
    )
    
    try:
        # Download the dataset
        start_time = time.time()
        
        # Use Kaggle API to download dataset
        api.dataset_download_files(
            dataset_name,
            path=str(data_path),
            unzip=True,
            quiet=False
        )
        
        duration_ms = int((time.time() - start_time) * 1000)
        
        # Calculate total size of downloaded files
        total_bytes = 0
        file_count = 0
        
        for file_path in data_path.glob("*"):
            if file_path.is_file():
                total_bytes += file_path.stat().st_size
                file_count += 1
        
        logger.info(
            "Download completed successfully",
            extra={
                "event": "download_success",
                "duration_ms": duration_ms,
                "bytes": total_bytes,
                "file_count": file_count,
                "dataset": dataset_name
            }
        )
        
        return {
            "dataset": dataset_name,
            "duration_ms": duration_ms,
            "bytes": total_bytes,
            "file_count": file_count,
            "path": str(data_path)
        }
        
    except Exception as e:
        logger.error(
            "Download failed",
            extra={
                "event": "download_failure",
                "error": str(e),
                "error_type": type(e).__name__,
                "dataset": dataset_name
            }
        )
        raise
def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    # Set log level
    logger.setLevel(getattr(logging, args.log_level))
    
    # Log script start
    logger.info(
        "Script started",
        extra={
            "event": "script_start",
            "data_dir": args.data_dir,
            "force": args.force
        }
    )
    
    # ===== TASK 2: Kaggle Authentication =====
    try:
        from kaggle import KaggleApi
        from kaggle import KaggleApi as KaggleApiClient
    except ImportError:
        logger.error(
            "Kaggle package not installed",
            extra={"event": "import_error", "package": "kaggle"}
        )
        sys.exit(1)
    
    # Get credentials from environment
    username = os.environ.get("KAGGLE_USERNAME")
    api_key = os.environ.get("KAGGLE_KEY")
    
    # Validate credentials
    if not username or not api_key:
        logger.error(
            "Missing Kaggle credentials in environment",
            extra={
                "event": "auth_failure",
                "missing": "username" if not username else "api_key"
            }
        )
        sys.exit(1)
    
    # Log that we have credentials (never log the actual key!)
    logger.info(
        "Kaggle credentials found",
        extra={
            "event": "auth_credentials_found",
            "username": username,
            "has_key": bool(api_key)
        }
    )
    
    # Authenticate
    try:
        api = KaggleApi()
        api.authenticate()
        
        logger.info(
            "Kaggle authentication successful",
            extra={"event": "auth_success", "username": username}
        )
    except Exception as e:
        logger.error(
            "Kaggle authentication failed",
            extra={
                "event": "auth_failure",
                "error": str(e),
                "error_type": type(e).__name__
            }
        )
        sys.exit(1)
    
    # TODO: Add more functionality here
    
    logger.info(
        "Script completed",
        extra={
            "event": "script_complete",
            "status": "success"
        }
    )

        # ===== TASK 3: Idempotency Check =====
    already_ingested, marker_path = check_already_ingested(args.data_dir, logger)
    
    if already_ingested and not args.force:
        logger.info(
            "Already ingested, skipping download",
            extra={
                "event": "skip_already_ingested",
                "marker_path": marker_path
            }
        )
        return
    elif already_ingested and args.force:
        logger.info(
            "Force mode enabled, re-downloading",
            extra={
                "event": "force_mode",
                "marker_path": marker_path
            }
        )

        # ===== TASK 4: Download Dataset =====
    #dataset_name = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
    # since the dataset is huge , we will test our code first with a samller dataset 
    dataset_name = "mkechinov/ecommerce-events"  # Small test dataset

    download_metadata = download_dataset(api, dataset_name, args.data_dir, logger)

if __name__ == "__main__":
    main()