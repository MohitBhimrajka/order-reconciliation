#!/usr/bin/env python3
"""
Database management script for handling migrations and database operations.
"""
import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from alembic.config import Config
from alembic import command
from src.database.init_db import init_db
from src.backup import BackupManager
from src.database import get_db_url

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / 'db_management.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_migration(message):
    """Create a new database migration."""
    alembic_cfg = Config("alembic.ini")
    command.revision(alembic_cfg, autogenerate=True, message=message)

def run_migrations():
    """Run all pending migrations."""
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")

def rollback_migration(revision):
    """Rollback to a specific migration revision."""
    alembic_cfg = Config("alembic.ini")
    command.downgrade(alembic_cfg, revision)

def setup_argparse():
    parser = argparse.ArgumentParser(description='Database Management Tool')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create a database backup')
    backup_parser.add_argument('--name', help='Custom name for the backup file')
    backup_parser.add_argument('--verify', action='store_true', help='Verify backup after creation')

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from a backup')
    restore_parser.add_argument('backup_path', help='Path to the backup file to restore from')

    # List command
    subparsers.add_parser('list', help='List available backups')

    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old backups')
    cleanup_parser.add_argument('--days', type=int, default=7, help='Number of days to retain backups')

    # Verify command
    verify_parser = subparsers.add_parser('verify', help='Verify a backup file')
    verify_parser.add_argument('backup_path', help='Path to the backup file to verify')

    return parser

def main():
    # Load environment variables
    load_dotenv(project_root / '.env')

    # Parse command line arguments
    parser = setup_argparse()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        # Initialize backup manager
        backup_dir = project_root / 'backups'
        backup_manager = BackupManager(
            db_url=get_db_url(),
            backup_dir=str(backup_dir),
            retention_days=7
        )

        if args.command == 'backup':
            # Create backup
            backup_path = backup_manager.create_backup(args.name)
            logger.info(f"Created backup: {backup_path}")

            if args.verify:
                if backup_manager.verify_backup(backup_path):
                    logger.info("Backup verification successful")
                else:
                    logger.error("Backup verification failed")
                    sys.exit(1)

        elif args.command == 'restore':
            # Restore backup
            if backup_manager.restore_backup(args.backup_path):
                logger.info("Backup restored successfully")
            else:
                logger.error("Failed to restore backup")
                sys.exit(1)

        elif args.command == 'list':
            # List backups
            backups = backup_manager.list_backups()
            if not backups:
                logger.info("No backups found")
            else:
                logger.info("Available backups:")
                for backup in backups:
                    logger.info(f"- {backup['name']} ({backup['timestamp']}, {backup['size']} bytes)")

        elif args.command == 'cleanup':
            # Clean up old backups
            removed_count = backup_manager.cleanup_old_backups()
            logger.info(f"Removed {removed_count} old backups")

        elif args.command == 'verify':
            # Verify backup
            if backup_manager.verify_backup(args.backup_path):
                logger.info("Backup verification successful")
            else:
                logger.error("Backup verification failed")
                sys.exit(1)

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 