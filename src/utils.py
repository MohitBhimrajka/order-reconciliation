"""
Utility functions for the reconciliation application.
"""
import os
import re
import logging
from typing import List, Dict, Set, Optional, Union
import pandas as pd
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define base directory as project root
BASE_DIR = Path(__file__).parent.parent

# Define data directories
DATA_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
REPORT_DIR = OUTPUT_DIR / 'reports'
VISUALIZATION_DIR = OUTPUT_DIR / 'visualizations'

# Create directories if they don't exist
for directory in [DATA_DIR, OUTPUT_DIR, REPORT_DIR, VISUALIZATION_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Define required columns based on actual data structure
REQUIRED_COLUMNS = {
    'orders': [
        'seller_id', 'warehouse_id', 'store_order_id', 'order_release_id', 'order_line_id',
        'seller_order_id', 'order_id_fk', 'core_item_id', 'created_on', 'style_id',
        'seller_sku_code', 'sku_id', 'myntra_sku_code', 'size', 'vendor_article_number',
        'brand', 'style_name', 'article_type', 'article_type_id', 'order_status',
        'packet_id', 'seller_packe_id', 'courier_code', 'order_tracking_number',
        'seller_warehouse_id', 'cancellation_reason_id_fk', 'cancellation_reason',
        'packed_on', 'fmpu_date', 'inscanned_on', 'shipped_on', 'delivered_on',
        'cancelled_on', 'rto_creation_date', 'lost_date', 'return_creation_date',
        'final_amount', 'total_mrp', 'discount', 'coupon_discount', 'shipping_charge',
        'gift_charge', 'tax_recovery', 'city', 'state', 'zipcode', 'is_ship_rel'
    ],
    'returns': [
        'order_release_id', 'order_line_id', 'return_type', 'return_date', 'packing_date',
        'delivery_date', 'ecommerce_portal_name', 'sku_code', 'invoice_number', 'packet_id',
        'hsn_code', 'product_tax_category', 'currency', 'customer_paid_amount',
        'postpaid_amount', 'prepaid_amount', 'mrp', 'total_discount_amount',
        'shipping_case_s', 'total_tax_rate', 'igst_amount', 'cgst_amount', 'sgst_amount',
        'tcs_amount', 'tds_amount', 'commission_percentage', 'minimum_commission',
        'platform_fees', 'total_commission', 'total_commission_plus_tcs_tds_deduction',
        'total_logistics_deduction', 'shipping_fee', 'fixed_fee', 'pick_and_pack_fee',
        'payment_gateway_fee', 'total_tax_on_logistics', 'article_level',
        'shipment_zone_classification', 'customer_paid_amt', 'total_settlement',
        'total_actual_settlement', 'amount_pending_settlement', 'prepaid_commission_deduction',
        'prepaid_logistics_deduction', 'prepaid_payment', 'postpaid_commission_deduction',
        'postpaid_logistics_deduction', 'postpaid_payment', 'settlement_date_prepaid_comm_deduction',
        'settlement_date_prepaid_logistics_deduction', 'settlement_date_prepaid_payment',
        'settlement_date_postpaid_comm_deduction', 'settlement_date_postpaid_logistics_deduction',
        'settlement_date_postpaid_payment', 'bank_utr_no_prepaid_comm_deduction',
        'bank_utr_no_prepaid_logistics_deduction', 'bank_utr_no_prepaid_payment',
        'bank_utr_no_postpaid_comm_deduction', 'bank_utr_no_postpaid_logistics_deduction',
        'bank_utr_no_postpaid_payment', 'postpaid_amount_other', 'prepaid_amount_other',
        'shipping_amount', 'gift_amount', 'additional_amount', 'cess_amount', 'taxable_amount',
        'igst_rate', 'cgst_rate', 'sgst_rate', 'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate',
        'tcs_cgst_rate', 'tds_rate', 'brand', 'gender', 'brand_type', 'article_type',
        'supply_type', 'try_and_buy_purchase', 'customer_name', 'customer_delivery_pin_code',
        'seller_gstn', 'seller_name', 'myntra_gstn', 'shipping_city', 'shipping_pin_code',
        'shipping_state', 'shipping_state_code', 'prepaid_commission_percentage',
        'prepaid_minimum_commission', 'prepaid_platform_fees', 'prepaid_total_commission',
        'prepaid_ship_commission_charge', 'prepaid_gift_commission_charge',
        'prepaid_cod_commission_charge', 'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission', 'postpaid_platform_fees',
        'postpaid_total_commission', 'postpaid_ship_commission_charge',
        'postpaid_gift_commission_charge', 'postpaid_cod_commission_charge',
        'postpaid_cart_discount', 'postpaid_coupon_discount', 'seller_order_id',
        'tcs_amount_prepaid', 'tcs_amount_postpaid', 'tds_amount_prepaid', 'tds_amount_postpaid',
        'seller_tier', 'royaltyCharges_prepaid', 'royaltyCharges_postpaid',
        'royaltyPercent_prepaid', 'royaltyPercent_postpaid', 'marketingCharges_prepaid',
        'marketingCharges_postpaid', 'marketingPercent_prepaid', 'marketingPercent_postpaid',
        'marketingContribution_prepaid', 'marketingContribution_postpaid',
        'reverseAdditionalCharges_prepaid', 'reverseAdditionalCharges_postpaid'
    ],
    'settlements': [
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
        'total_actual_settlement', 'amount_pending_settlement', 'prepaid_commission_deduction',
        'prepaid_logistics_deduction', 'prepaid_payment', 'postpaid_commission_deduction',
        'postpaid_logistics_deduction', 'postpaid_payment', 'settlement_date_prepaid_comm_deduction',
        'settlement_date_prepaid_logistics_deduction', 'settlement_date_prepaid_payment',
        'settlement_date_postpaid_comm_deduction', 'settlement_date_postpaid_logistics_deduction',
        'settlement_date_postpaid_payment', 'bank_utr_no_prepaid_comm_deduction',
        'bank_utr_no_prepaid_logistics_deduction', 'bank_utr_no_prepaid_payment',
        'bank_utr_no_postpaid_comm_deduction', 'bank_utr_no_postpaid_logistics_deduction',
        'bank_utr_no_postpaid_payment', 'postpaid_amount_other', 'prepaid_amount_other',
        'shipping_amount', 'gift_amount', 'additional_amount', 'cess_amount', 'taxable_amount',
        'igst_rate', 'cgst_rate', 'sgst_rate', 'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate',
        'tcs_cgst_rate', 'tds_rate', 'brand', 'gender', 'brand_type', 'article_type',
        'supply_type', 'try_and_buy_purchase', 'customer_name', 'customer_delivery_pin_code',
        'seller_gstn', 'seller_name', 'myntra_gstn', 'shipping_city', 'shipping_pin_code',
        'shipping_state', 'shipping_state_code', 'prepaid_commission_percentage',
        'prepaid_minimum_commission', 'prepaid_platform_fees', 'prepaid_total_commission',
        'prepaid_ship_commission_charge', 'prepaid_gift_commission_charge',
        'prepaid_cod_commission_charge', 'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission', 'postpaid_platform_fees',
        'postpaid_total_commission', 'postpaid_ship_commission_charge',
        'postpaid_gift_commission_charge', 'postpaid_cod_commission_charge',
        'postpaid_cart_discount', 'postpaid_coupon_discount', 'seller_order_id',
        'tcs_amount_prepaid', 'tcs_amount_postpaid', 'tds_amount_prepaid', 'tds_amount_postpaid',
        'seller_tier', 'techEnablement_prepaid', 'techEnablement_postpaid',
        'airLogistics_prepaid', 'airLogistics_postpaid', 'royaltyCharges_prepaid',
        'royaltyCharges_postpaid', 'royaltyPercent_prepaid', 'royaltyPercent_postpaid',
        'marketingCharges_prepaid', 'marketingCharges_postpaid', 'marketingPercent_prepaid',
        'marketingPercent_postpaid', 'marketingContribution_prepaid', 'marketingContribution_postpaid',
        'forwardAdditionalCharges_prepaid', 'forwardAdditionalCharges_postpaid'
    ]
}

# Define file patterns
ORDERS_PATTERN = "orders-*.csv"
RETURNS_PATTERN = "returns-*.csv"
SETTLEMENT_PATTERN = "settlement-*.csv"

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
    'returns': {
        'order release id': 'order_release_id',
        'order line id': 'order_line_id',
        'return type': 'return_type',
        'return date': 'return_date',
        'packing date': 'packing_date',
        'delivery date': 'delivery_date',
        'ecommerce portal name': 'ecommerce_portal_name',
        'sku code': 'sku_code',
        'invoice number': 'invoice_number',
        'packet id': 'packet_id',
        'hsn code': 'hsn_code',
        'product tax category': 'product_tax_category',
        'customer paid amount': 'customer_paid_amount',
        'postpaid amount': 'postpaid_amount',
        'prepaid amount': 'prepaid_amount',
        'total discount amount': 'total_discount_amount',
        'shipping case s': 'shipping_case_s',
        'total tax rate': 'total_tax_rate',
        'igst amount': 'igst_amount',
        'cgst amount': 'cgst_amount',
        'sgst amount': 'sgst_amount',
        'tcs amount': 'tcs_amount',
        'tds amount': 'tds_amount',
        'commission percentage': 'commission_percentage',
        'minimum commission': 'minimum_commission',
        'platform fees': 'platform_fees',
        'total commission': 'total_commission',
        'total commission plus tcs tds deduction': 'total_commission_plus_tcs_tds_deduction',
        'total logistics deduction': 'total_logistics_deduction',
        'shipping fee': 'shipping_fee',
        'fixed fee': 'fixed_fee',
        'pick and pack fee': 'pick_and_pack_fee',
        'payment gateway fee': 'payment_gateway_fee',
        'total tax on logistics': 'total_tax_on_logistics',
        'article level': 'article_level',
        'shipment zone classification': 'shipment_zone_classification',
        'customer paid amt': 'customer_paid_amt',
        'total settlement': 'total_settlement',
        'total actual settlement': 'total_actual_settlement',
        'amount pending settlement': 'amount_pending_settlement',
        'prepaid commission deduction': 'prepaid_commission_deduction',
        'prepaid logistics deduction': 'prepaid_logistics_deduction',
        'prepaid payment': 'prepaid_payment',
        'postpaid commission deduction': 'postpaid_commission_deduction',
        'postpaid logistics deduction': 'postpaid_logistics_deduction',
        'postpaid payment': 'postpaid_payment',
        'settlement date prepaid comm deduction': 'settlement_date_prepaid_comm_deduction',
        'settlement date prepaid logistics deduction': 'settlement_date_prepaid_logistics_deduction',
        'settlement date prepaid payment': 'settlement_date_prepaid_payment',
        'settlement date postpaid comm deduction': 'settlement_date_postpaid_comm_deduction',
        'settlement date postpaid logistics deduction': 'settlement_date_postpaid_logistics_deduction',
        'settlement date postpaid payment': 'settlement_date_postpaid_payment',
        'bank utr no prepaid comm deduction': 'bank_utr_no_prepaid_comm_deduction',
        'bank utr no prepaid logistics deduction': 'bank_utr_no_prepaid_logistics_deduction',
        'bank utr no prepaid payment': 'bank_utr_no_prepaid_payment',
        'bank utr no postpaid comm deduction': 'bank_utr_no_postpaid_comm_deduction',
        'bank utr no postpaid logistics deduction': 'bank_utr_no_postpaid_logistics_deduction',
        'bank utr no postpaid payment': 'bank_utr_no_postpaid_payment',
        'postpaid amount other': 'postpaid_amount_other',
        'prepaid amount other': 'prepaid_amount_other',
        'shipping amount': 'shipping_amount',
        'gift amount': 'gift_amount',
        'additional amount': 'additional_amount',
        'cess amount': 'cess_amount',
        'taxable amount': 'taxable_amount',
        'igst rate': 'igst_rate',
        'cgst rate': 'cgst_rate',
        'sgst rate': 'sgst_rate',
        'cess rate': 'cess_rate',
        'tcs igst rate': 'tcs_igst_rate',
        'tcs sgst rate': 'tcs_sgst_rate',
        'tcs cgst rate': 'tcs_cgst_rate',
        'tds rate': 'tds_rate',
        'try and buy purchase': 'try_and_buy_purchase',
        'customer name': 'customer_name',
        'customer delivery pin code': 'customer_delivery_pin_code',
        'seller gstn': 'seller_gstn',
        'seller name': 'seller_name',
        'myntra gstn': 'myntra_gstn',
        'shipping city': 'shipping_city',
        'shipping pin code': 'shipping_pin_code',
        'shipping state': 'shipping_state',
        'shipping state code': 'shipping_state_code',
        'prepaid commission percentage': 'prepaid_commission_percentage',
        'prepaid minimum commission': 'prepaid_minimum_commission',
        'prepaid platform fees': 'prepaid_platform_fees',
        'prepaid total commission': 'prepaid_total_commission',
        'prepaid ship commission charge': 'prepaid_ship_commission_charge',
        'prepaid gift commission charge': 'prepaid_gift_commission_charge',
        'prepaid cod commission charge': 'prepaid_cod_commission_charge',
        'prepaid cart discount': 'prepaid_cart_discount',
        'prepaid coupon discount': 'prepaid_coupon_discount',
        'postpaid commission percentage': 'postpaid_commission_percentage',
        'postpaid minimum commission': 'postpaid_minimum_commission',
        'postpaid platform fees': 'postpaid_platform_fees',
        'postpaid total commission': 'postpaid_total_commission',
        'postpaid ship commission charge': 'postpaid_ship_commission_charge',
        'postpaid gift commission charge': 'postpaid_gift_commission_charge',
        'postpaid cod commission charge': 'postpaid_cod_commission_charge',
        'postpaid cart discount': 'postpaid_cart_discount',
        'postpaid coupon discount': 'postpaid_coupon_discount',
        'seller order id': 'seller_order_id',
        'tcs amount prepaid': 'tcs_amount_prepaid',
        'tcs amount postpaid': 'tcs_amount_postpaid',
        'tds amount prepaid': 'tds_amount_prepaid',
        'tds amount postpaid': 'tds_amount_postpaid',
        'seller tier': 'seller_tier',
        'royalty charges prepaid': 'royaltyCharges_prepaid',
        'royalty charges postpaid': 'royaltyCharges_postpaid',
        'royalty percent prepaid': 'royaltyPercent_prepaid',
        'royalty percent postpaid': 'royaltyPercent_postpaid',
        'marketing charges prepaid': 'marketingCharges_prepaid',
        'marketing charges postpaid': 'marketingCharges_postpaid',
        'marketing percent prepaid': 'marketingPercent_prepaid',
        'marketing percent postpaid': 'marketingPercent_postpaid',
        'marketing contribution prepaid': 'marketingContribution_prepaid',
        'marketing contribution postpaid': 'marketingContribution_postpaid',
        'reverse additional charges prepaid': 'reverseAdditionalCharges_prepaid',
        'reverse additional charges postpaid': 'reverseAdditionalCharges_postpaid'
    },
    'settlements': {
        'order release id': 'order_release_id',
        'order line id': 'order_line_id',
        'return type': 'return_type',
        'return date': 'return_date',
        'packing date': 'packing_date',
        'delivery date': 'delivery_date',
        'ecommerce portal name': 'ecommerce_portal_name',
        'sku code': 'sku_code',
        'invoice number': 'invoice_number',
        'packet id': 'packet_id',
        'hsn code': 'hsn_code',
        'product tax category': 'product_tax_category',
        'customer paid amount': 'customer_paid_amount',
        'postpaid amount': 'postpaid_amount',
        'prepaid amount': 'prepaid_amount',
        'total discount amount': 'total_discount_amount',
        'shipping case': 'shipping_case',
        'total tax rate': 'total_tax_rate',
        'igst amount': 'igst_amount',
        'cgst amount': 'cgst_amount',
        'sgst amount': 'sgst_amount',
        'tcs amount': 'tcs_amount',
        'tds amount': 'tds_amount',
        'commission percentage': 'commission_percentage',
        'minimum commission': 'minimum_commission',
        'platform fees': 'platform_fees',
        'total commission': 'total_commission',
        'total commission plus tcs tds deduction': 'total_commission_plus_tcs_tds_deduction',
        'total logistics deduction': 'total_logistics_deduction',
        'shipping fee': 'shipping_fee',
        'fixed fee': 'fixed_fee',
        'pick and pack fee': 'pick_and_pack_fee',
        'payment gateway fee': 'payment_gateway_fee',
        'total tax on logistics': 'total_tax_on_logistics',
        'article level': 'article_level',
        'shipment zone classification': 'shipment_zone_classification',
        'customer paid amt': 'customer_paid_amt',
        'total expected settlement': 'total_expected_settlement',
        'total actual settlement': 'total_actual_settlement',
        'amount pending settlement': 'amount_pending_settlement',
        'prepaid commission deduction': 'prepaid_commission_deduction',
        'prepaid logistics deduction': 'prepaid_logistics_deduction',
        'prepaid payment': 'prepaid_payment',
        'postpaid commission deduction': 'postpaid_commission_deduction',
        'postpaid logistics deduction': 'postpaid_logistics_deduction',
        'postpaid payment': 'postpaid_payment',
        'settlement date prepaid comm deduction': 'settlement_date_prepaid_comm_deduction',
        'settlement date prepaid logistics deduction': 'settlement_date_prepaid_logistics_deduction',
        'settlement date prepaid payment': 'settlement_date_prepaid_payment',
        'settlement date postpaid comm deduction': 'settlement_date_postpaid_comm_deduction',
        'settlement date postpaid logistics deduction': 'settlement_date_postpaid_logistics_deduction',
        'settlement date postpaid payment': 'settlement_date_postpaid_payment',
        'bank utr no prepaid comm deduction': 'bank_utr_no_prepaid_comm_deduction',
        'bank utr no prepaid logistics deduction': 'bank_utr_no_prepaid_logistics_deduction',
        'bank utr no prepaid payment': 'bank_utr_no_prepaid_payment',
        'bank utr no postpaid comm deduction': 'bank_utr_no_postpaid_comm_deduction',
        'bank utr no postpaid logistics deduction': 'bank_utr_no_postpaid_logistics_deduction',
        'bank utr no postpaid payment': 'bank_utr_no_postpaid_payment',
        'postpaid amount other': 'postpaid_amount_other',
        'prepaid amount other': 'prepaid_amount_other',
        'shipping amount': 'shipping_amount',
        'gift amount': 'gift_amount',
        'additional amount': 'additional_amount',
        'cess amount': 'cess_amount',
        'taxable amount': 'taxable_amount',
        'igst rate': 'igst_rate',
        'cgst rate': 'cgst_rate',
        'sgst rate': 'sgst_rate',
        'cess rate': 'cess_rate',
        'tcs igst rate': 'tcs_igst_rate',
        'tcs sgst rate': 'tcs_sgst_rate',
        'tcs cgst rate': 'tcs_cgst_rate',
        'tds rate': 'tds_rate',
        'try and buy purchase': 'try_and_buy_purchase',
        'customer name': 'customer_name',
        'customer delivery pin code': 'customer_delivery_pin_code',
        'seller gstn': 'seller_gstn',
        'seller name': 'seller_name',
        'myntra gstn': 'myntra_gstn',
        'shipping city': 'shipping_city',
        'shipping pin code': 'shipping_pin_code',
        'shipping state': 'shipping_state',
        'shipping state code': 'shipping_state_code',
        'prepaid commission percentage': 'prepaid_commission_percentage',
        'prepaid minimum commission': 'prepaid_minimum_commission',
        'prepaid platform fees': 'prepaid_platform_fees',
        'prepaid total commission': 'prepaid_total_commission',
        'prepaid ship commission charge': 'prepaid_ship_commission_charge',
        'prepaid gift commission charge': 'prepaid_gift_commission_charge',
        'prepaid cod commission charge': 'prepaid_cod_commission_charge',
        'prepaid cart discount': 'prepaid_cart_discount',
        'prepaid coupon discount': 'prepaid_coupon_discount',
        'postpaid commission percentage': 'postpaid_commission_percentage',
        'postpaid minimum commission': 'postpaid_minimum_commission',
        'postpaid platform fees': 'postpaid_platform_fees',
        'postpaid total commission': 'postpaid_total_commission',
        'postpaid ship commission charge': 'postpaid_ship_commission_charge',
        'postpaid gift commission charge': 'postpaid_gift_commission_charge',
        'postpaid cod commission charge': 'postpaid_cod_commission_charge',
        'postpaid cart discount': 'postpaid_cart_discount',
        'postpaid coupon discount': 'postpaid_coupon_discount',
        'seller order id': 'seller_order_id',
        'tcs amount prepaid': 'tcs_amount_prepaid',
        'tcs amount postpaid': 'tcs_amount_postpaid',
        'tds amount prepaid': 'tds_amount_prepaid',
        'tds amount postpaid': 'tds_amount_postpaid',
        'seller tier': 'seller_tier',
        'tech enablement prepaid': 'techEnablement_prepaid',
        'tech enablement postpaid': 'techEnablement_postpaid',
        'air logistics prepaid': 'airLogistics_prepaid',
        'air logistics postpaid': 'airLogistics_postpaid',
        'royalty charges prepaid': 'royaltyCharges_prepaid',
        'royalty charges postpaid': 'royaltyCharges_postpaid',
        'royalty percent prepaid': 'royaltyPercent_prepaid',
        'royalty percent postpaid': 'royaltyPercent_postpaid',
        'marketing charges prepaid': 'marketingCharges_prepaid',
        'marketing charges postpaid': 'marketingCharges_postpaid',
        'marketing percent prepaid': 'marketingPercent_prepaid',
        'marketing percent postpaid': 'marketingPercent_postpaid',
        'marketing contribution prepaid': 'marketingContribution_prepaid',
        'marketing contribution postpaid': 'marketingContribution_postpaid',
        'forward additional charges prepaid': 'forwardAdditionalCharges_prepaid',
        'forward additional charges postpaid': 'forwardAdditionalCharges_postpaid'
    }
}

def validate_file_columns(df: pd.DataFrame, file_type: str) -> bool:
    """Validate that the DataFrame has all required columns for the given file type."""
    if file_type not in REQUIRED_COLUMNS:
        raise ValueError(f"Invalid file type: {file_type}")
    
    required = set(REQUIRED_COLUMNS[file_type])
    actual = set(df.columns)
    
    missing = required - actual
    if missing:
        logger.error(f"Missing required columns in {file_type} file: {missing}")
        return False
    
    return True

def get_file_path(file_type: str, month: str, year: str) -> Path:
    """Get the path for a specific file type and month/year."""
    if file_type not in ['orders', 'returns', 'settlements']:
        raise ValueError(f"Invalid file type: {file_type}")
    
    filename = f"{file_type}-{month}-{year}.csv"
    return DATA_DIR / filename

def format_currency(value: float) -> str:
    """
    Format a number as currency.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted currency string
    """
    return f"₹{value:,.2f}"

def format_date(date_str: str) -> str:
    """Format date string to standard format."""
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%Y-%m-%d')
    except:
        return date_str

def get_current_month_year() -> tuple:
    """Get current month and year."""
    now = datetime.now()
    return now.strftime('%m'), now.strftime('%Y')

def ensure_directory(path: Path) -> None:
    """Ensure directory exists, create if it doesn't."""
    path.mkdir(parents=True, exist_ok=True)

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

def format_percentage(value: float) -> str:
    """
    Format a number as percentage.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted percentage string
    """
    return f"{value:.2f}%" 

def convert_date(date_str: str) -> Optional[datetime]:
    """
    Convert date string to datetime object.
    
    Args:
        date_str: Date string in various formats
    
    Returns:
        datetime object if successful, None otherwise
    """
    if not date_str or pd.isna(date_str):
        return None
    
    try:
        # Try common date formats
        for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d']:
            try:
                return datetime.strptime(str(date_str).strip(), fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None

def convert_boolean(value: str) -> bool:
    """
    Convert string to boolean.
    
    Args:
        value: String value to convert
    
    Returns:
        Boolean value
    """
    if not value or pd.isna(value):
        return False
    return str(value).lower() in ['true', '1', 'yes', 'y']

def convert_float(value: str) -> Optional[float]:
    """
    Convert string to float.
    
    Args:
        value: String value to convert
    
    Returns:
        Float value if successful, None otherwise
    """
    if not value or pd.isna(value):
        return None
    try:
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return None

def convert_int(value: str) -> Optional[int]:
    """
    Convert string to integer.
    
    Args:
        value: String value to convert
    
    Returns:
        Integer value if successful, None otherwise
    """
    if not value or pd.isna(value):
        return None
    try:
        return int(float(str(value).replace(',', '')))
    except (ValueError, TypeError):
        return None

def convert_string(value: str) -> Optional[str]:
    """
    Convert value to string.
    
    Args:
        value: Value to convert
    
    Returns:
        String value if successful, None otherwise
    """
    if pd.isna(value):
        return None
    return str(value).strip()

def convert_data_types(df: pd.DataFrame, file_type: str) -> pd.DataFrame:
    """
    Convert DataFrame columns to appropriate data types.
    
    Args:
        df: DataFrame to convert
        file_type: Type of file (orders, returns, settlement)
    
    Returns:
        DataFrame with converted data types
    """
    # Common date columns for all file types
    date_columns = [
        'created_on', 'packed_on', 'fmpu_date', 'inscanned_on', 'shipped_on',
        'delivered_on', 'cancelled_on', 'rto_creation_date', 'lost_date',
        'return_creation_date', 'return_date', 'packing_date', 'delivery_date',
        'settlement_date_prepaid_comm_deduction', 'settlement_date_prepaid_logistics_deduction',
        'settlement_date_prepaid_payment', 'settlement_date_postpaid_comm_deduction',
        'settlement_date_postpaid_logistics_deduction', 'settlement_date_postpaid_payment'
    ]
    
    # Common numeric columns for all file types
    numeric_columns = [
        'final_amount', 'total_mrp', 'coupon_discount', 'shipping_charge',
        'gift_charge', 'tax_recovery', 'customer_paid_amount', 'postpaid_amount',
        'prepaid_amount', 'total_discount_amount', 'total_tax_rate', 'igst_amount',
        'cgst_amount', 'sgst_amount', 'tcs_amount', 'tds_amount', 'commission_percentage',
        'minimum_commission', 'platform_fees', 'total_commission',
        'total_commission_plus_tcs_tds_deduction', 'total_logistics_deduction',
        'shipping_fee', 'fixed_fee', 'pick_and_pack_fee', 'payment_gateway_fee',
        'total_tax_on_logistics', 'customer_paid_amt', 'total_expected_settlement',
        'total_actual_settlement', 'amount_pending_settlement', 'prepaid_commission_deduction',
        'prepaid_logistics_deduction', 'prepaid_payment', 'postpaid_commission_deduction',
        'postpaid_logistics_deduction', 'postpaid_payment', 'postpaid_amount_other',
        'prepaid_amount_other', 'shipping_amount', 'gift_amount', 'additional_amount',
        'cess_amount', 'taxable_amount', 'igst_rate', 'cgst_rate', 'sgst_rate',
        'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate', 'tcs_cgst_rate', 'tds_rate',
        'prepaid_commission_percentage', 'prepaid_minimum_commission', 'prepaid_platform_fees',
        'prepaid_total_commission', 'prepaid_ship_commission_charge', 'prepaid_gift_commission_charge',
        'prepaid_cod_commission_charge', 'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission', 'postpaid_platform_fees',
        'postpaid_total_commission', 'postpaid_ship_commission_charge', 'postpaid_gift_commission_charge',
        'postpaid_cod_commission_charge', 'postpaid_cart_discount', 'postpaid_coupon_discount',
        'tcs_amount_prepaid', 'tcs_amount_postpaid', 'tds_amount_prepaid', 'tds_amount_postpaid',
        'royaltyCharges_prepaid', 'royaltyCharges_postpaid', 'royaltyPercent_prepaid',
        'royaltyPercent_postpaid', 'marketingCharges_prepaid', 'marketingCharges_postpaid',
        'marketingPercent_prepaid', 'marketingPercent_postpaid', 'marketingContribution_prepaid',
        'marketingContribution_postpaid', 'reverseAdditionalCharges_prepaid', 'reverseAdditionalCharges_postpaid',
        'techEnablement_prepaid', 'techEnablement_postpaid', 'airLogistics_prepaid', 'airLogistics_postpaid',
        'forwardAdditionalCharges_prepaid', 'forwardAdditionalCharges_postpaid'
    ]
    
    # Common integer columns for all file types
    integer_columns = [
        'seller_id', 'warehouse_id', 'store_order_id', 'order_id_fk', 'core_item_id',
        'style_id', 'sku_id', 'seller_warehouse_id', 'cancellation_reason_id_fk',
        'article_type_id', 'packet_id', 'seller_pack_id', 'customer_delivery_pin_code',
        'shipping_pin_code', 'shipping_state_code'
    ]
    
    # Common boolean columns for all file types
    boolean_columns = [
        'is_ship_rel', 'try_and_buy_purchase'
    ]
    
    # Convert date columns
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_date)
    
    # Convert numeric columns
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_float)
    
    # Convert integer columns
    for col in integer_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_int)
    
    # Convert boolean columns
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_boolean)
    
    # Convert remaining columns to string
    for col in df.columns:
        if col not in date_columns + numeric_columns + integer_columns + boolean_columns:
            df[col] = df[col].apply(convert_string)
    
    return df 