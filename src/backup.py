import os
import shutil
import logging
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
import subprocess
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class BackupManager:
    def __init__(self, db_url: str, backup_dir: str, retention_days: int = 7):
        self.db_url = db_url
        self.backup_dir = Path(backup_dir)
        self.retention_days = retention_days
        self.engine = create_engine(db_url)
        
        # Create backup directory if it doesn't exist
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def create_backup(self, backup_name: Optional[str] = None) -> str:
        """Create a database backup."""
        try:
            # Generate backup filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = backup_name or f"backup_{timestamp}.sql"
            backup_path = self.backup_dir / backup_name

            # Extract database credentials from URL
            db_name = self.db_url.split('/')[-1]
            db_user = self.db_url.split('://')[1].split(':')[0]
            db_host = self.db_url.split('@')[1].split('/')[0]

            # Create backup using pg_dump
            cmd = [
                'pg_dump',
                '-h', db_host,
                '-U', db_user,
                '-d', db_name,
                '-F', 'c',  # Custom format
                '-f', str(backup_path)
            ]

            # Set PGPASSWORD environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_url.split(':')[2].split('@')[0]

            # Execute backup command
            subprocess.run(cmd, env=env, check=True)
            logger.info(f"Successfully created backup: {backup_path}")
            return str(backup_path)

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create backup: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during backup: {str(e)}")
            raise

    def restore_backup(self, backup_path: str) -> bool:
        """Restore database from a backup file."""
        try:
            backup_file = Path(backup_path)
            if not backup_file.exists():
                raise FileNotFoundError(f"Backup file not found: {backup_path}")

            # Extract database credentials from URL
            db_name = self.db_url.split('/')[-1]
            db_user = self.db_url.split('://')[1].split(':')[0]
            db_host = self.db_url.split('@')[1].split('/')[0]

            # Drop all connections to the database
            with self.engine.connect() as conn:
                conn.execute(text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :db_name"),
                           {"db_name": db_name})

            # Restore backup using pg_restore
            cmd = [
                'pg_restore',
                '-h', db_host,
                '-U', db_user,
                '-d', db_name,
                '--clean',  # Clean (drop) database objects before recreating
                '--if-exists',  # Use IF EXISTS when dropping objects
                str(backup_file)
            ]

            # Set PGPASSWORD environment variable
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_url.split(':')[2].split('@')[0]

            # Execute restore command
            subprocess.run(cmd, env=env, check=True)
            logger.info(f"Successfully restored backup: {backup_path}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to restore backup: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during restore: {str(e)}")
            raise

    def cleanup_old_backups(self) -> int:
        """Remove backups older than retention period."""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.retention_days)
            removed_count = 0

            for backup_file in self.backup_dir.glob("backup_*.sql"):
                file_timestamp = datetime.strptime(backup_file.stem.split('_')[1], "%Y%m%d_%H%M%S")
                if file_timestamp < cutoff_date:
                    backup_file.unlink()
                    removed_count += 1
                    logger.info(f"Removed old backup: {backup_file}")

            return removed_count

        except Exception as e:
            logger.error(f"Error during backup cleanup: {str(e)}")
            raise

    def list_backups(self) -> list:
        """List all available backups with their details."""
        try:
            backups = []
            for backup_file in self.backup_dir.glob("backup_*.sql"):
                file_timestamp = datetime.strptime(backup_file.stem.split('_')[1], "%Y%m%d_%H%M%S")
                backups.append({
                    'name': backup_file.name,
                    'path': str(backup_file),
                    'timestamp': file_timestamp,
                    'size': backup_file.stat().st_size
                })
            return sorted(backups, key=lambda x: x['timestamp'], reverse=True)

        except Exception as e:
            logger.error(f"Error listing backups: {str(e)}")
            raise

    def verify_backup(self, backup_path: str) -> bool:
        """Verify the integrity of a backup file."""
        try:
            backup_file = Path(backup_path)
            if not backup_file.exists():
                raise FileNotFoundError(f"Backup file not found: {backup_path}")

            # Extract database credentials from URL
            db_name = self.db_url.split('/')[-1]
            db_user = self.db_url.split('://')[1].split(':')[0]
            db_host = self.db_url.split('@')[1].split('/')[0]

            # Create a temporary database for verification
            temp_db = f"verify_{db_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create temporary database
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE {temp_db}"))

            try:
                # Attempt to restore to temporary database
                cmd = [
                    'pg_restore',
                    '-h', db_host,
                    '-U', db_user,
                    '-d', temp_db,
                    '--clean',
                    '--if-exists',
                    str(backup_file)
                ]

                env = os.environ.copy()
                env['PGPASSWORD'] = self.db_url.split(':')[2].split('@')[0]

                subprocess.run(cmd, env=env, check=True)
                logger.info(f"Successfully verified backup: {backup_path}")
                return True

            finally:
                # Clean up temporary database
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP DATABASE IF EXISTS {temp_db}"))

        except Exception as e:
            logger.error(f"Error verifying backup: {str(e)}")
            return False 