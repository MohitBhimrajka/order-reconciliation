"""
Script to process data files and populate the database.
"""
import argparse
from datetime import datetime
from pathlib import Path
import logging
from sqlalchemy.orm import Session

from src.database.config import get_db
from src.processors import process_files, analyze_settlements
from src.utils import validate_file_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_data(
    orders_file: str,
    returns_file: str,
    settlements_file: str,
    month: str = None
) -> None:
    """
    Process data files and populate the database.
    
    Args:
        orders_file: Path to orders file
        returns_file: Path to returns file
        settlements_file: Path to settlements file
        month: Optional month to process data for (format: YYYY-MM)
    """
    try:
        # Validate files exist
        for file_path in [orders_file, returns_file, settlements_file]:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"File not found: {file_path}")
        
        # Validate file columns
        if not all([
            validate_file_columns(orders_file, 'orders'),
            validate_file_columns(returns_file, 'returns'),
            validate_file_columns(settlements_file, 'settlements')
        ]):
            raise ValueError("Invalid columns in one or more files")
        
        # Parse month if provided
        target_month = None
        if month:
            try:
                target_month = datetime.strptime(month, '%Y-%m')
            except ValueError:
                raise ValueError("Invalid month format. Use YYYY-MM")
        
        # Process files with database session
        with get_db() as db:
            process_files(
                orders_file=orders_file,
                returns_file=returns_file,
                settlements_file=settlements_file,
                db=db,
                month=target_month
            )
            
            # Analyze settlements if month is provided
            if month:
                analysis = analyze_settlements(db, month)
                logger.info(f"Settlement analysis for {month}:")
                logger.info(f"Total settlements: {analysis['total_settlements']}")
                logger.info(f"Completed settlements: {analysis['completed_settlements']}")
                logger.info(f"Partial settlements: {analysis['partial_settlements']}")
                logger.info(f"Pending settlements: {analysis['pending_settlements']}")
                logger.info(f"Completion rate: {analysis['completion_rate']:.2f}%")
                logger.info(f"Amount completion rate: {analysis['amount_completion_rate']:.2f}%")
                logger.info(f"Average settlement time: {analysis['avg_settlement_time']:.1f} days")
                logger.info(f"Pending from previous month: {analysis['pending_from_previous']}")
        
        logger.info("Successfully processed all files")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Process data files and populate the database.')
    parser.add_argument('orders_file', help='Path to orders file')
    parser.add_argument('returns_file', help='Path to returns file')
    parser.add_argument('settlements_file', help='Path to settlements file')
    parser.add_argument('--month', help='Month to process data for (format: YYYY-MM)')
    
    args = parser.parse_args()
    
    try:
        process_data(
            orders_file=args.orders_file,
            returns_file=args.returns_file,
            settlements_file=args.settlements_file,
            month=args.month
        )
    except Exception as e:
        logger.error(f"Failed to process data: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main() 