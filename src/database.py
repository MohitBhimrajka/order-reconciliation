"""
Database configuration and operations.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import shutil
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')

# Connection pool settings
DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '5'))
DB_MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '10'))
DB_POOL_TIMEOUT = int(os.getenv('DB_POOL_TIMEOUT', '30'))
DB_POOL_RECYCLE = int(os.getenv('DB_POOL_RECYCLE', '1800'))  # 30 minutes

# Backup settings
BACKUP_DIR = Path('backups')
BACKUP_RETENTION_DAYS = int(os.getenv('BACKUP_RETENTION_DAYS', '7'))

def get_db_url() -> str:
    """Get database URL from environment variables."""
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        raise ValueError("Missing required database environment variables")
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_db_engine():
    """Create SQLAlchemy engine with connection pooling."""
    db_url = get_db_url()
    
    try:
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
            pool_timeout=DB_POOL_TIMEOUT,
            pool_recycle=DB_POOL_RECYCLE,
            pool_pre_ping=True,
            echo=False  # Set to True for SQL query logging
        )
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        raise

# Create engine and session factory
engine = create_db_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Initialize the database by creating all tables."""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def close_db():
    """Close database connection."""
    try:
        engine.dispose()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Error closing database connection: {e}")
        raise

def create_backup():
    """Create a database backup using pg_dump."""
    try:
        # Ensure backup directory exists
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = BACKUP_DIR / f"backup_{timestamp}.sql"
        
        # Construct pg_dump command
        cmd = f"pg_dump -h {DB_HOST} -p {DB_PORT} -U {DB_USER} -d {DB_NAME} -F c -f {backup_file}"
        
        # Set PGPASSWORD environment variable for passwordless operation
        os.environ['PGPASSWORD'] = DB_PASSWORD
        
        # Execute backup command
        result = os.system(cmd)
        if result != 0:
            raise Exception(f"pg_dump failed with exit code {result}")
        
        logger.info(f"Database backup created successfully: {backup_file}")
        return backup_file
    except Exception as e:
        logger.error(f"Error creating database backup: {e}")
        raise

def restore_backup(backup_file):
    """Restore database from a backup file."""
    try:
        if not backup_file.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")
        
        # Construct pg_restore command
        cmd = f"pg_restore -h {DB_HOST} -p {DB_PORT} -U {DB_USER} -d {DB_NAME} -c {backup_file}"
        
        # Set PGPASSWORD environment variable
        os.environ['PGPASSWORD'] = DB_PASSWORD
        
        # Execute restore command
        result = os.system(cmd)
        if result != 0:
            raise Exception(f"pg_restore failed with exit code {result}")
        
        logger.info(f"Database restored successfully from: {backup_file}")
    except Exception as e:
        logger.error(f"Error restoring database backup: {e}")
        raise

def cleanup_old_backups(max_backups=5):
    """Clean up old backup files, keeping only the most recent ones."""
    try:
        if not BACKUP_DIR.exists():
            return
        
        # Get list of backup files sorted by modification time
        backup_files = sorted(
            BACKUP_DIR.glob('backup_*.sql'),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        # Remove old backups
        for backup_file in backup_files[max_backups:]:
            backup_file.unlink()
            logger.info(f"Removed old backup: {backup_file}")
        
        # Remove backups older than retention period
        cutoff_date = datetime.now().timestamp() - (BACKUP_RETENTION_DAYS * 24 * 60 * 60)
        for backup_file in backup_files[:max_backups]:
            if backup_file.stat().st_mtime < cutoff_date:
                backup_file.unlink()
                logger.info(f"Removed expired backup: {backup_file}")
    except Exception as e:
        logger.error(f"Error cleaning up old backups: {e}")
        raise

if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("Database initialization complete!") 