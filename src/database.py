from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_db_url() -> str:
    """Get database URL from environment variables."""
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')

    if not all([db_user, db_password, db_name]):
        raise ValueError("Missing required database environment variables")

    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def create_db_engine():
    """Create SQLAlchemy engine with connection pooling."""
    db_url = get_db_url()
    
    # Connection pool settings
    pool_size = int(os.getenv('DB_POOL_SIZE', '5'))
    max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '10'))
    pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', '30'))
    pool_recycle = int(os.getenv('DB_POOL_RECYCLE', '1800'))  # 30 minutes

    try:
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,  # Enable connection health checks
            echo=os.getenv('DB_ECHO', 'false').lower() == 'true'
        )
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
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
    """Initialize database tables."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create database tables: {str(e)}")
        raise

def close_db():
    """Close database connections."""
    try:
        engine.dispose()
        logger.info("Database connections closed successfully")
    except Exception as e:
        logger.error(f"Failed to close database connections: {str(e)}")
        raise

def create_backup():
    """Create a backup of the database."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(BACKUP_DIR, f"reconciliation_backup_{timestamp}.db")
        
        if DATABASE_URL.startswith("sqlite"):
            # For SQLite, simply copy the file
            shutil.copy2("reconciliation.db", backup_file)
        else:
            # For PostgreSQL, use pg_dump
            import subprocess
            subprocess.run([
                "pg_dump",
                "-Fc",
                "-f", backup_file,
                DATABASE_URL
            ])
        
        logger.info(f"Created backup: {backup_file}")
        return backup_file
    except Exception as e:
        logger.error(f"Error creating backup: {str(e)}")
        raise

def restore_backup(backup_file):
    """Restore database from a backup file."""
    try:
        if DATABASE_URL.startswith("sqlite"):
            # For SQLite, simply copy the file back
            shutil.copy2(backup_file, "reconciliation.db")
        else:
            # For PostgreSQL, use pg_restore
            import subprocess
            subprocess.run([
                "pg_restore",
                "-d", DATABASE_URL,
                "-c",  # Clean (drop) database objects before recreating
                backup_file
            ])
        
        logger.info(f"Restored backup: {backup_file}")
    except Exception as e:
        logger.error(f"Error restoring backup: {str(e)}")
        raise

def cleanup_old_backups(max_backups=5):
    """Clean up old backups, keeping only the most recent ones."""
    try:
        backup_files = sorted([
            f for f in os.listdir(BACKUP_DIR)
            if f.startswith("reconciliation_backup_")
        ])
        
        while len(backup_files) > max_backups:
            oldest_backup = backup_files.pop(0)
            os.remove(os.path.join(BACKUP_DIR, oldest_backup))
            logger.info(f"Removed old backup: {oldest_backup}")
    except Exception as e:
        logger.error(f"Error cleaning up old backups: {str(e)}")
        raise 