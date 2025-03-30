"""
Utility functions for the reconciliation application.
"""
import os
import re
import logging
from typing import List, Dict, Set, Optional
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define base directory (project root)
BASE_DIR = Path(__file__).parent.parent

# Define required columns for each file type
REQUIRED_COLUMNS = {
    'orders': {
        'seller id', 'warehouse id', 'store order id', 'order release id', 'order line id',
        'seller order id', 'order id fk', 'core_item_id', 'created on', 'style id',
        'seller sku code', 'sku id', 'myntra sku code', 'size', 'vendor article number',
        'brand', 'style name', 'article type', 'article type id', 'order status',
        'packet id', 'seller packe id', 'courier code', 'order tracking number',
        'seller warehouse id', 'cancellation reason id fk', 'cancellation reason',
        'packed on', 'fmpu date', 'inscanned on', 'shipped on', 'delivered on',
        'cancelled on', 'rto creation date', 'lost date', 'return creation date',
        'final amount', 'total mrp', 'discount', 'coupon discount', 'shipping charge',
        'gift charge', 'tax recovery', 'city', 'state', 'zipcode', 'is_ship_rel'
    },
    'returns': {
        'order_release_id', 'order_line_id', 'return_type', 'return_date', 'packing_date',
        'delivery_date', 'ecommerce_portal_name', 'sku_code', 'invoice_number', 'packet_id',
        'hsn_code', 'product_tax_category', 'currency', 'customer_paid_amount',
        'postpaid_amount', 'prepaid_amount', 'mrp', 'total_discount_amount',
        'shipping_case', 'total_tax_rate', 'igst_amount', 'cgst_amount', 'sgst_amount',
        'tcs_amount', 'tds_amount', 'commission_percentage', 'minimum_commission',
        'platform_fees', 'total_commission', 'total_commission_plus_tcs_tds_deduction',
        'total_logistics_deduction', 'shipping_fee', 'fixed_fee', 'pick_and_pack_fee',
        'payment_gateway_fee', 'total_tax_on_logistics', 'article_level',
        'shipment_zone_classification', 'customer_paid_amt', 'total_settlement',
        'total_actual_settlement', 'amount_pending_settlement',
        'prepaid_commission_deduction', 'prepaid_logistics_deduction', 'prepaid_payment',
        'postpaid_commission_deduction', 'postpaid_logistics_deduction', 'postpaid_payment',
        'settlement_date_prepaid_comm_deduction', 'settlement_date_prepaid_logistics_deduction',
        'settlement_date_prepaid_payment', 'settlement_date_postpaid_comm_deduction',
        'settlement_date_postpaid_logistics_deduction', 'settlement_date_postpaid_payment',
        'bank_utr_no_prepaid_comm_deduction', 'bank_utr_no_prepaid_logistics_deduction',
        'bank_utr_no_prepaid_payment', 'bank_utr_no_postpaid_comm_deduction',
        'bank_utr_no_postpaid_logistics_deduction', 'bank_utr_no_postpaid_payment',
        'postpaid_amount_other', 'prepaid_amount_other', 'shipping_amount', 'gift_amount',
        'additional_amount', 'cess_amount', 'taxable_amount', 'igst_rate', 'cgst_rate',
        'sgst_rate', 'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate', 'tcs_cgst_rate',
        'tds_rate', 'brand', 'gender', 'brand_type', 'article_type', 'supply_type',
        'try_and_buy_purchase', 'customer_name', 'customer_delivery_pin_code',
        'seller_gstn', 'seller_name', 'myntra_gstn', 'shipping_city', 'shipping_pin_code',
        'shipping_state', 'shipping_state_code', 'prepaid_commission_percentage',
        'prepaid_minimum_commission', 'prepaid_platform_fees', 'prepaid_total_commission',
        'prepaid_ship_commission_charge', 'prepaid_gift_commission_charge',
        'prepaid_cod_commission_charge', 'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission',
        'postpaid_platform_fees', 'postpaid_total_commission',
        'postpaid_ship_commission_charge', 'postpaid_gift_commission_charge',
        'postpaid_cod_commission_charge', 'postpaid_cart_discount',
        'postpaid_coupon_discount', 'seller_order_id', 'return_id',
        'tcs_amount_prepaid', 'tcs_amount_postpaid', 'tds_amount_prepaid',
        'tds_amount_postpaid', 'seller_tier', 'royaltyCharges_prepaid',
        'royaltyCharges_postpaid', 'royaltyPercent_prepaid', 'royaltyPercent_postpaid',
        'marketingCharges_prepaid', 'marketingCharges_postpaid',
        'marketingPercent_prepaid', 'marketingPercent_postpaid',
        'marketingContribution_prepaid', 'marketingContribution_postpaid',
        'reverseAdditionalCharges_prepaid', 'reverseAdditionalCharges_postpaid'
    },
    'settlement': {
        'order_release_id', 'order_line_id', 'return_type', 'return_date', 'packing_date',
        'delivery_date', 'ecommerce_portal_name', 'sku_code', 'invoice_number', 'packet_id',
        'hsn_code', 'product_tax_category', 'currency', 'customer_paid_amount',
        'postpaid_amount', 'prepaid_amount', 'mrp', 'total_discount_amount',
        'shipping_case', 'total_tax_rate', 'igst_amount', 'cgst_amount', 'sgst_amount',
        'tcs_amount', 'tds_amount', 'commission_percentage', 'minimum_commission',
        'platform_fees', 'total_commission', 'total_commission_plus_tcs_tds_deduction',
        'total_logistics_deduction', 'shipping_fee', 'fixed_fee', 'pick_and_pack_fee',
        'payment_gateway_fee', 'total_tax_on_logistics', 'article_level',
        'shipment_zone_classification', 'customer_paid_amt', 'total_expected_settlement',
        'total_actual_settlement', 'amount_pending_settlement',
        'prepaid_commission_deduction', 'prepaid_logistics_deduction', 'prepaid_payment',
        'postpaid_commission_deduction', 'postpaid_logistics_deduction', 'postpaid_payment',
        'settlement_date_prepaid_comm_deduction', 'settlement_date_prepaid_logistics_deduction',
        'settlement_date_prepaid_payment', 'settlement_date_postpaid_comm_deduction',
        'settlement_date_postpaid_logistics_deduction', 'settlement_date_postpaid_payment',
        'bank_utr_no_prepaid_comm_deduction', 'bank_utr_no_prepaid_logistics_deduction',
        'bank_utr_no_prepaid_payment', 'bank_utr_no_postpaid_comm_deduction',
        'bank_utr_no_postpaid_logistics_deduction', 'bank_utr_no_postpaid_payment',
        'postpaid_amount_other', 'prepaid_amount_other', 'shipping_amount', 'gift_amount',
        'additional_amount', 'cess_amount', 'taxable_amount', 'igst_rate', 'cgst_rate',
        'sgst_rate', 'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate', 'tcs_cgst_rate',
        'tds_rate', 'brand', 'gender', 'brand_type', 'article_type', 'supply_type',
        'try_and_buy_purchase', 'customer_name', 'customer_delivery_pin_code',
        'seller_gstn', 'seller_name', 'myntra_gstn', 'shipping_city', 'shipping_pin_code',
        'shipping_state', 'shipping_state_code', 'prepaid_commission_percentage',
        'prepaid_minimum_commission', 'prepaid_platform_fees', 'prepaid_total_commission',
        'prepaid_ship_commission_charge', 'prepaid_gift_commission_charge',
        'prepaid_cod_commission_charge', 'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission',
        'postpaid_platform_fees', 'postpaid_total_commission',
        'postpaid_ship_commission_charge', 'postpaid_gift_commission_charge',
        'postpaid_cod_commission_charge', 'postpaid_cart_discount',
        'postpaid_coupon_discount', 'seller_order_id', 'tcs_amount_prepaid',
        'tcs_amount_postpaid', 'tds_amount_prepaid', 'tds_amount_postpaid', 'seller_tier',
        'techEnablement_prepaid', 'techEnablement_postpaid', 'airLogistics_prepaid',
        'airLogistics_postpaid', 'royaltyCharges_prepaid', 'royaltyCharges_postpaid',
        'royaltyPercent_prepaid', 'royaltyPercent_postpaid', 'marketingCharges_prepaid',
        'marketingCharges_postpaid', 'marketingPercent_prepaid', 'marketingPercent_postpaid',
        'marketingContribution_prepaid', 'marketingContribution_postpaid',
        'forwardAdditionalCharges_prepaid', 'forwardAdditionalCharges_postpaid'
    }
}

# Define file patterns
ORDERS_PATTERN = "orders-*.csv"
RETURNS_PATTERN = "returns-*.csv"
SETTLEMENT_PATTERN = "settlement-*.csv"

# Define directories
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = OUTPUT_DIR / "reports"
VISUALIZATION_DIR = OUTPUT_DIR / "visualizations"

# Define master file paths
ORDERS_MASTER = OUTPUT_DIR / "master_orders.csv"
RETURNS_MASTER = OUTPUT_DIR / "master_returns.csv"
SETTLEMENT_MASTER = OUTPUT_DIR / "master_settlement.csv"

# Define output file paths
ANALYSIS_OUTPUT = OUTPUT_DIR / "analysis_results.csv"
REPORT_OUTPUT = REPORT_DIR / "reconciliation_report.txt"
ANOMALIES_OUTPUT = OUTPUT_DIR / "anomalies.csv"

# Column renaming mapping for standardization
COLUMN_RENAMES = {
    'orders': {
        'order release id': 'order_release_id',
        'order line id': 'order_line_id',
        'order status': 'order_status',
        'final amount': 'final_amount',
        'total mrp': 'total_mrp',
        'coupon discount': 'coupon_discount',
        'shipping charge': 'shipping_charge',
        'gift charge': 'gift_charge',
        'tax recovery': 'tax_recovery',
        'return creation date': 'return_creation_date',
        'seller id': 'seller_id',
        'warehouse id': 'warehouse_id',
        'store order id': 'store_order_id',
        'seller order id': 'seller_order_id',
        'order id fk': 'order_id_fk',
        'core_item_id': 'core_item_id',
        'created on': 'created_on',
        'style id': 'style_id',
        'seller sku code': 'seller_sku_code',
        'sku id': 'sku_id',
        'myntra sku code': 'myntra_sku_code',
        'vendor article number': 'vendor_article_number',
        'style name': 'style_name',
        'article type': 'article_type',
        'article type id': 'article_type_id',
        'packet id': 'packet_id',
        'seller packe id': 'seller_pack_id',
        'courier code': 'courier_code',
        'order tracking number': 'order_tracking_number',
        'seller warehouse id': 'seller_warehouse_id',
        'cancellation reason id fk': 'cancellation_reason_id_fk',
        'cancellation reason': 'cancellation_reason',
        'packed on': 'packed_on',
        'fmpu date': 'fmpu_date',
        'inscanned on': 'inscanned_on',
        'shipped on': 'shipped_on',
        'delivered on': 'delivered_on',
        'cancelled on': 'cancelled_on',
        'rto creation date': 'rto_creation_date',
        'lost date': 'lost_date',
        'discount': 'discount',
        'city': 'city',
        'state': 'state',
        'zipcode': 'zipcode',
        'is_ship_rel': 'is_ship_rel'
    },
    'returns': {},  # Already using underscores
    'settlement': {}  # Already using underscores
}

def ensure_directories_exist() -> None:
    """Ensure all required directories exist."""
    for directory in [DATA_DIR, OUTPUT_DIR, REPORT_DIR, VISUALIZATION_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {directory}")

def validate_file_columns(file_path: Path, file_type: str) -> bool:
    """
    Validate that a file has all required columns.
    
    Args:
        file_path: Path to the file to validate
        file_type: Type of file ('orders', 'returns', or 'settlement')
    
    Returns:
        True if file has all required columns, False otherwise
    """
    try:
        # Read just the header row
        df = pd.read_csv(file_path, nrows=0)
        
        # Get required columns for this file type
        required = set(REQUIRED_COLUMNS.get(file_type, []))
        
        # Check for missing required columns
        missing = required - set(df.columns)
        if missing:
            logger.error(f"Missing required columns in {file_path}: {missing}")
            return False
        
        # Log any extra columns found
        extra = set(df.columns) - required
        if extra:
            logger.info(f"Extra columns found in {file_path}: {extra}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating columns in {file_path}: {e}")
        return False

def read_file(file_path: Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.
    
    Args:
        file_path: Path to the file to read
    
    Returns:
        DataFrame containing the file contents
    """
    try:
        df = pd.read_csv(file_path)
        logger.debug(f"Successfully read {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise

def get_processed_files() -> List[Path]:
    """
    Get list of processed files in the data directory.
    
    Returns:
        List of Path objects for processed files
    """
    if not DATA_DIR.exists():
        return []
    
    processed_files = []
    for pattern in [ORDERS_PATTERN, RETURNS_PATTERN, SETTLEMENT_PATTERN]:
        for file in DATA_DIR.glob('*'):
            if re.match(pattern, file.name):
                processed_files.append(file)
    
    return processed_files

def extract_date_from_filename(filename: str) -> Optional[tuple]:
    """
    Extract month and year from filename.
    
    Args:
        filename: Name of the file
    
    Returns:
        Tuple of (month, year) if found, None otherwise
    """
    for pattern in [ORDERS_PATTERN, RETURNS_PATTERN, SETTLEMENT_PATTERN]:
        match = re.match(pattern, filename)
        if match:
            return match.groups()[:2]
    return None

def get_file_identifier(file_type: str, month: str, year: str) -> str:
    """
    Generate standard filename for a given file type, month, and year.
    
    Args:
        file_type: Type of file (orders, returns, settlement)
        month: Month (01-12)
        year: Year (YYYY)
    
    Returns:
        Standardized filename
    """
    return f"{file_type}-{month}-{year}.csv"

def format_currency(value: float) -> str:
    """
    Format a number as currency.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted currency string
    """
    return f"₹{value:,.2f}"

def format_percentage(value: float) -> str:
    """
    Format a number as percentage.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted percentage string
    """
    return f"{value:.2f}%" 