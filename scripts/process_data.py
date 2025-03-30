import os
import sys
from datetime import datetime
from alembic.config import Config
from alembic import command
from sqlalchemy.orm import Session
from src.database import SessionLocal, engine
from src.models import Base
from src.processors import process_files
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_migrations():
    """Run database migrations."""
    try:
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")
        logger.info("Successfully ran database migrations")
    except Exception as e:
        logger.error(f"Error running migrations: {str(e)}")
        sys.exit(1)

def process_data_files():
    """Process data files and populate the database."""
    try:
        # Get data directory path
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        
        # Get latest files for current month
        current_month = datetime.now().strftime("%m-%Y")
        orders_file = os.path.join(data_dir, f"orders-{current_month}.csv")
        returns_file = os.path.join(data_dir, f"returns-{current_month}.csv")
        settlements_file = os.path.join(data_dir, f"settlement-{current_month}.csv")

        # Check if files exist
        if not all(os.path.exists(f) for f in [orders_file, returns_file, settlements_file]):
            logger.error("Required data files not found")
            sys.exit(1)

        # Create database session
        db = SessionLocal()
        try:
            # Process files
            process_files(orders_file, returns_file, settlements_file, db)
            logger.info("Successfully processed data files")
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Error processing data files: {str(e)}")
        sys.exit(1)

def main():
    """Main function to run migrations and process data."""
    try:
        # Run migrations
        run_migrations()
        
        # Process data files
        process_data_files()
        
        logger.info("Successfully completed all operations")
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 