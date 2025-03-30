"""
File processor module for importing data into the database.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

from src.database.config import get_db
from src.data_import.db_operations import (
    upsert_orders,
    upsert_returns,
    upsert_settlements,
    update_order_status
)
from src.utils import validate_file_columns

logger = logging.getLogger(__name__)

def process_orders_file(file_path: Path) -> bool:
    """
    Process an orders file and import it into the database.
    
    Args:
        file_path: Path to the orders file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Validate file columns first
        if not validate_file_columns(file_path, 'orders'):
            raise ValueError(f"Invalid columns in orders file: {file_path}")
        
        # Read the file
        orders_df = pd.read_csv(file_path)
        
        # Add source file information
        orders_df['source_file'] = file_path.name
        
        # Process the data with a database session
        with get_db() as session:
            processed_ids = upsert_orders(orders_df, session)
            logger.info(f"Successfully processed {len(processed_ids)} orders from {file_path.name}")
            
            # Update order statuses
            for _, order in orders_df.iterrows():
                status = "Cancelled" if order.get('is_ship_rel', 1) == 0 else "Pending"
                update_order_status(
                    order['order_release_id'],
                    status,
                    session,
                    {'source': 'initial_import'}
                )
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing orders file {file_path}: {e}")
        return False

def process_returns_file(file_path: Path) -> bool:
    """
    Process a returns file and import it into the database.
    
    Args:
        file_path: Path to the returns file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Validate file columns first
        if not validate_file_columns(file_path, 'returns'):
            raise ValueError(f"Invalid columns in returns file: {file_path}")
        
        # Read the file
        returns_df = pd.read_csv(file_path)
        
        # Add source file information
        returns_df['source_file'] = file_path.name
        
        # Process the data with a database session
        with get_db() as session:
            processed_ids = upsert_returns(returns_df, session)
            logger.info(f"Successfully processed {len(processed_ids)} returns from {file_path.name}")
            
            # Update order statuses for returned orders
            for _, return_record in returns_df.iterrows():
                update_order_status(
                    return_record['order_release_id'],
                    "Returned",
                    session,
                    {
                        'source': 'return_import',
                        'return_type': return_record.get('return_type', 'unknown')
                    }
                )
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing returns file {file_path}: {e}")
        return False

def process_settlement_file(file_path: Path) -> bool:
    """
    Process a settlement file and import it into the database.
    
    Args:
        file_path: Path to the settlement file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Validate file columns first
        if not validate_file_columns(file_path, 'settlement'):
            raise ValueError(f"Invalid columns in settlement file: {file_path}")
        
        # Read the file
        settlement_df = pd.read_csv(file_path)
        
        # Add source file information
        settlement_df['source_file'] = file_path.name
        
        # Process the data with a database session
        with get_db() as session:
            processed_ids = upsert_settlements(settlement_df, session)
            logger.info(f"Successfully processed {len(processed_ids)} settlements from {file_path.name}")
            
            # Update order statuses for settled orders
            for _, settlement in settlement_df.iterrows():
                status = "Returned - Settled" if settlement.get('return_type') else "Completed - Settled"
                update_order_status(
                    settlement['order_release_id'],
                    status,
                    session,
                    {'source': 'settlement_import'}
                )
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing settlement file {file_path}: {e}")
        return False

def process_file(file_path: Path, file_type: str) -> bool:
    """
    Process a file based on its type.
    
    Args:
        file_path: Path to the file
        file_type: Type of file (orders, returns, settlement)
    
    Returns:
        True if successful, False otherwise
    """
    processors = {
        'orders': process_orders_file,
        'returns': process_returns_file,
        'settlement': process_settlement_file
    }
    
    processor = processors.get(file_type)
    if not processor:
        logger.error(f"Invalid file type: {file_type}")
        return False
    
    return processor(file_path) 