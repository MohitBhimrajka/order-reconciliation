"""
Data processing module for importing and analyzing data.
"""
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from .models import Order, Return, Settlement, SettlementDate, MonthlyReconciliation, SettlementHistory
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
from .utils import convert_data_types, validate_file_columns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str: str) -> datetime:
    """Parse date string in various formats to datetime object."""
    if pd.isna(date_str):
        return None
    try:
        return pd.to_datetime(date_str)
    except:
        return None

def process_orders(df: pd.DataFrame, db: Session, source_file: str) -> List[Order]:
    """Process orders data and create Order records."""
    orders = []
    for _, row in df.iterrows():
        try:
            order = Order(
                seller_id=row['seller_id'],
                warehouse_id=row['warehouse_id'],
                store_order_id=row['store_order_id'],
                order_release_id=row['order_release_id'],
                order_line_id=row['order_line_id'],
                seller_order_id=row['seller_order_id'],
                order_id_fk=row['order_id_fk'],
                core_item_id=row['core_item_id'],
                created_on=row['created_on'],
                style_id=row['style_id'],
                seller_sku_code=row['seller_sku_code'],
                sku_id=row['sku_id'],
                myntra_sku_code=row['myntra_sku_code'],
                size=row['size'],
                vendor_article_number=row['vendor_article_number'],
                brand=row['brand'],
                style_name=row['style_name'],
                article_type=row['article_type'],
                article_type_id=row['article_type_id'],
                order_status=row['order_status'],
                packet_id=row['packet_id'],
                seller_pack_id=row['seller_pack_id'],
                courier_code=row['courier_code'],
                order_tracking_number=row['order_tracking_number'],
                seller_warehouse_id=row['seller_warehouse_id'],
                cancellation_reason_id_fk=row['cancellation_reason_id_fk'],
                cancellation_reason=row['cancellation_reason'],
                packed_on=row['packed_on'],
                fmpu_date=row['fmpu_date'],
                inscanned_on=row['inscanned_on'],
                shipped_on=row['shipped_on'],
                delivered_on=row['delivered_on'],
                cancelled_on=row['cancelled_on'],
                rto_creation_date=row['rto_creation_date'],
                lost_date=row['lost_date'],
                return_creation_date=row['return_creation_date'],
                final_amount=row['final_amount'],
                total_mrp=row['total_mrp'],
                discount=row['discount'],
                coupon_discount=row['coupon_discount'],
                shipping_charge=row['shipping_charge'],
                gift_charge=row['gift_charge'],
                tax_recovery=row['tax_recovery'],
                city=row['city'],
                state=row['state'],
                zipcode=row['zipcode'],
                is_ship_rel=row['is_ship_rel'],
                source_file=source_file
            )
            orders.append(order)
        except Exception as e:
            logger.error(f"Error processing order {row['order_release_id']}: {str(e)}")
    return orders

def process_returns(df: pd.DataFrame, db: Session, source_file: str) -> List[Return]:
    """Process returns data and create Return records."""
    returns = []
    for _, row in df.iterrows():
        try:
            # Get the associated order
            order = db.query(Order).filter_by(order_release_id=row['order_release_id']).first()
            if not order:
                logger.warning(f"Order not found for return: {row['order_release_id']}")
                continue

            return_record = Return(
                order_id=order.id,
                order_release_id=row['order_release_id'],
                order_line_id=row['order_line_id'],
                return_type=row['return_type'],
                return_date=row['return_date'],
                packing_date=row['packing_date'],
                delivery_date=row['delivery_date'],
                ecommerce_portal_name=row['ecommerce_portal_name'],
                sku_code=row['sku_code'],
                invoice_number=row['invoice_number'],
                packet_id=row['packet_id'],
                hsn_code=row['hsn_code'],
                product_tax_category=row['product_tax_category'],
                currency=row['currency'],
                customer_paid_amount=row['customer_paid_amount'],
                postpaid_amount=row['postpaid_amount'],
                prepaid_amount=row['prepaid_amount'],
                mrp=row['mrp'],
                total_discount_amount=row['total_discount_amount'],
                shipping_case_s=row['shipping_case_s'],
                total_tax_rate=row['total_tax_rate'],
                igst_amount=row['igst_amount'],
                cgst_amount=row['cgst_amount'],
                sgst_amount=row['sgst_amount'],
                tcs_amount=row['tcs_amount'],
                tds_amount=row['tds_amount'],
                commission_percentage=row['commission_percentage'],
                minimum_commission=row['minimum_commission'],
                platform_fees=row['platform_fees'],
                total_commission=row['total_commission'],
                total_commission_plus_tcs_tds_deduction=row['total_commission_plus_tcs_tds_deduction'],
                total_logistics_deduction=row['total_logistics_deduction'],
                shipping_fee=row['shipping_fee'],
                fixed_fee=row['fixed_fee'],
                pick_and_pack_fee=row['pick_and_pack_fee'],
                payment_gateway_fee=row['payment_gateway_fee'],
                total_tax_on_logistics=row['total_tax_on_logistics'],
                article_level=row['article_level'],
                shipment_zone_classification=row['shipment_zone_classification'],
                customer_paid_amt=row['customer_paid_amt'],
                total_settlement=row['total_settlement'],
                total_actual_settlement=row['total_actual_settlement'],
                amount_pending_settlement=row['amount_pending_settlement'],
                prepaid_commission_deduction=row['prepaid_commission_deduction'],
                prepaid_logistics_deduction=row['prepaid_logistics_deduction'],
                prepaid_payment=row['prepaid_payment'],
                postpaid_commission_deduction=row['postpaid_commission_deduction'],
                postpaid_logistics_deduction=row['postpaid_logistics_deduction'],
                postpaid_payment=row['postpaid_payment'],
                settlement_date_prepaid_comm_deduction=row['settlement_date_prepaid_comm_deduction'],
                settlement_date_prepaid_logistics_deduction=row['settlement_date_prepaid_logistics_deduction'],
                settlement_date_prepaid_payment=row['settlement_date_prepaid_payment'],
                settlement_date_postpaid_comm_deduction=row['settlement_date_postpaid_comm_deduction'],
                settlement_date_postpaid_logistics_deduction=row['settlement_date_postpaid_logistics_deduction'],
                settlement_date_postpaid_payment=row['settlement_date_postpaid_payment'],
                bank_utr_no_prepaid_comm_deduction=row['bank_utr_no_prepaid_comm_deduction'],
                bank_utr_no_prepaid_logistics_deduction=row['bank_utr_no_prepaid_logistics_deduction'],
                bank_utr_no_prepaid_payment=row['bank_utr_no_prepaid_payment'],
                bank_utr_no_postpaid_comm_deduction=row['bank_utr_no_postpaid_comm_deduction'],
                bank_utr_no_postpaid_logistics_deduction=row['bank_utr_no_postpaid_logistics_deduction'],
                bank_utr_no_postpaid_payment=row['bank_utr_no_postpaid_payment'],
                postpaid_amount_other=row['postpaid_amount_other'],
                prepaid_amount_other=row['prepaid_amount_other'],
                shipping_amount=row['shipping_amount'],
                gift_amount=row['gift_amount'],
                additional_amount=row['additional_amount'],
                cess_amount=row['cess_amount'],
                taxable_amount=row['taxable_amount'],
                igst_rate=row['igst_rate'],
                cgst_rate=row['cgst_rate'],
                sgst_rate=row['sgst_rate'],
                cess_rate=row['cess_rate'],
                tcs_igst_rate=row['tcs_igst_rate'],
                tcs_sgst_rate=row['tcs_sgst_rate'],
                tcs_cgst_rate=row['tcs_cgst_rate'],
                tds_rate=row['tds_rate'],
                try_and_buy_purchase=row['try_and_buy_purchase'],
                customer_name=row['customer_name'],
                customer_delivery_pin_code=row['customer_delivery_pin_code'],
                seller_gstn=row['seller_gstn'],
                seller_name=row['seller_name'],
                myntra_gstn=row['myntra_gstn'],
                shipping_city=row['shipping_city'],
                shipping_pin_code=row['shipping_pin_code'],
                shipping_state=row['shipping_state'],
                shipping_state_code=row['shipping_state_code'],
                prepaid_commission_percentage=row['prepaid_commission_percentage'],
                prepaid_minimum_commission=row['prepaid_minimum_commission'],
                prepaid_platform_fees=row['prepaid_platform_fees'],
                prepaid_total_commission=row['prepaid_total_commission'],
                prepaid_ship_commission_charge=row['prepaid_ship_commission_charge'],
                prepaid_gift_commission_charge=row['prepaid_gift_commission_charge'],
                prepaid_cod_commission_charge=row['prepaid_cod_commission_charge'],
                prepaid_cart_discount=row['prepaid_cart_discount'],
                prepaid_coupon_discount=row['prepaid_coupon_discount'],
                postpaid_commission_percentage=row['postpaid_commission_percentage'],
                postpaid_minimum_commission=row['postpaid_minimum_commission'],
                postpaid_platform_fees=row['postpaid_platform_fees'],
                postpaid_total_commission=row['postpaid_total_commission'],
                postpaid_ship_commission_charge=row['postpaid_ship_commission_charge'],
                postpaid_gift_commission_charge=row['postpaid_gift_commission_charge'],
                postpaid_cod_commission_charge=row['postpaid_cod_commission_charge'],
                postpaid_cart_discount=row['postpaid_cart_discount'],
                postpaid_coupon_discount=row['postpaid_coupon_discount'],
                seller_order_id=row['seller_order_id'],
                tcs_amount_prepaid=row['tcs_amount_prepaid'],
                tcs_amount_postpaid=row['tcs_amount_postpaid'],
                tds_amount_prepaid=row['tds_amount_prepaid'],
                tds_amount_postpaid=row['tds_amount_postpaid'],
                seller_tier=row['seller_tier'],
                royaltyCharges_prepaid=row['royaltyCharges_prepaid'],
                royaltyCharges_postpaid=row['royaltyCharges_postpaid'],
                royaltyPercent_prepaid=row['royaltyPercent_prepaid'],
                royaltyPercent_postpaid=row['royaltyPercent_postpaid'],
                marketingCharges_prepaid=row['marketingCharges_prepaid'],
                marketingCharges_postpaid=row['marketingCharges_postpaid'],
                marketingPercent_prepaid=row['marketingPercent_prepaid'],
                marketingPercent_postpaid=row['marketingPercent_postpaid'],
                marketingContribution_prepaid=row['marketingContribution_prepaid'],
                marketingContribution_postpaid=row['marketingContribution_postpaid'],
                reverseAdditionalCharges_prepaid=row['reverseAdditionalCharges_prepaid'],
                reverseAdditionalCharges_postpaid=row['reverseAdditionalCharges_postpaid'],
                source_file=source_file
            )
            returns.append(return_record)
        except Exception as e:
            logger.error(f"Error processing return for order {row['order_release_id']}: {str(e)}")
    return returns

def process_settlements(df: pd.DataFrame, db: Session, source_file: str) -> List[Settlement]:
    """Process settlements data and create Settlement records."""
    settlements = []
    for _, row in df.iterrows():
        try:
            # Check if order exists
            order = db.query(Order).filter(Order.order_release_id == row['order_release_id']).first()
            if not order:
                logger.warning(f"Order not found for settlement: {row['order_release_id']}")
                continue

            # Check if return exists
            return_record = db.query(Return).filter(Return.order_release_id == row['order_release_id']).first()
            if not return_record:
                logger.warning(f"Return not found for settlement: {row['order_release_id']}")
                continue

            # Calculate settlement status
            total_expected = row['total_expected_settlement']
            total_actual = row['total_actual_settlement']
            amount_pending = row['amount_pending_settlement']

            if amount_pending == 0:
                status = 'settled'
            elif total_actual > 0:
                status = 'partial'
            else:
                status = 'pending'

            settlement = Settlement(
                order_release_id=row['order_release_id'],
                order_line_id=row['order_line_id'],
                return_type=row['return_type'],
                return_date=row['return_date'],
                packing_date=row['packing_date'],
                delivery_date=row['delivery_date'],
                ecommerce_portal_name=row['ecommerce_portal_name'],
                sku_code=row['sku_code'],
                invoice_number=row['invoice_number'],
                packet_id=row['packet_id'],
                hsn_code=row['hsn_code'],
                product_tax_category=row['product_tax_category'],
                currency=row['currency'],
                customer_paid_amount=row['customer_paid_amount'],
                postpaid_amount=row['postpaid_amount'],
                prepaid_amount=row['prepaid_amount'],
                mrp=row['mrp'],
                total_discount_amount=row['total_discount_amount'],
                shipping_case=row['shipping_case'],
                total_tax_rate=row['total_tax_rate'],
                igst_amount=row['igst_amount'],
                cgst_amount=row['cgst_amount'],
                sgst_amount=row['sgst_amount'],
                tcs_amount=row['tcs_amount'],
                tds_amount=row['tds_amount'],
                commission_percentage=row['commission_percentage'],
                minimum_commission=row['minimum_commission'],
                platform_fees=row['platform_fees'],
                total_commission=row['total_commission'],
                total_commission_plus_tcs_tds_deduction=row['total_commission_plus_tcs_tds_deduction'],
                total_logistics_deduction=row['total_logistics_deduction'],
                shipping_fee=row['shipping_fee'],
                fixed_fee=row['fixed_fee'],
                pick_and_pack_fee=row['pick_and_pack_fee'],
                payment_gateway_fee=row['payment_gateway_fee'],
                total_tax_on_logistics=row['total_tax_on_logistics'],
                article_level=row['article_level'],
                shipment_zone_classification=row['shipment_zone_classification'],
                customer_paid_amt=row['customer_paid_amt'],
                total_expected_settlement=row['total_expected_settlement'],
                total_actual_settlement=row['total_actual_settlement'],
                amount_pending_settlement=row['amount_pending_settlement'],
                prepaid_commission_deduction=row['prepaid_commission_deduction'],
                prepaid_logistics_deduction=row['prepaid_logistics_deduction'],
                prepaid_payment=row['prepaid_payment'],
                postpaid_commission_deduction=row['postpaid_commission_deduction'],
                postpaid_logistics_deduction=row['postpaid_logistics_deduction'],
                postpaid_payment=row['postpaid_payment'],
                settlement_date_prepaid_comm_deduction=row['settlement_date_prepaid_comm_deduction'],
                settlement_date_prepaid_logistics_deduction=row['settlement_date_prepaid_logistics_deduction'],
                settlement_date_prepaid_payment=row['settlement_date_prepaid_payment'],
                settlement_date_postpaid_comm_deduction=row['settlement_date_postpaid_comm_deduction'],
                settlement_date_postpaid_logistics_deduction=row['settlement_date_postpaid_logistics_deduction'],
                settlement_date_postpaid_payment=row['settlement_date_postpaid_payment'],
                bank_utr_no_prepaid_comm_deduction=row['bank_utr_no_prepaid_comm_deduction'],
                bank_utr_no_prepaid_logistics_deduction=row['bank_utr_no_prepaid_logistics_deduction'],
                bank_utr_no_prepaid_payment=row['bank_utr_no_prepaid_payment'],
                bank_utr_no_postpaid_comm_deduction=row['bank_utr_no_postpaid_comm_deduction'],
                bank_utr_no_postpaid_logistics_deduction=row['bank_utr_no_postpaid_logistics_deduction'],
                bank_utr_no_postpaid_payment=row['bank_utr_no_postpaid_payment'],
                postpaid_amount_other=row['postpaid_amount_other'],
                prepaid_amount_other=row['prepaid_amount_other'],
                shipping_amount=row['shipping_amount'],
                gift_amount=row['gift_amount'],
                additional_amount=row['additional_amount'],
                cess_amount=row['cess_amount'],
                taxable_amount=row['taxable_amount'],
                igst_rate=row['igst_rate'],
                cgst_rate=row['cgst_rate'],
                sgst_rate=row['sgst_rate'],
                cess_rate=row['cess_rate'],
                tcs_igst_rate=row['tcs_igst_rate'],
                tcs_sgst_rate=row['tcs_sgst_rate'],
                tcs_cgst_rate=row['tcs_cgst_rate'],
                tds_rate=row['tds_rate'],
                try_and_buy_purchase=row['try_and_buy_purchase'],
                customer_name=row['customer_name'],
                customer_delivery_pin_code=row['customer_delivery_pin_code'],
                seller_gstn=row['seller_gstn'],
                seller_name=row['seller_name'],
                myntra_gstn=row['myntra_gstn'],
                shipping_city=row['shipping_city'],
                shipping_pin_code=row['shipping_pin_code'],
                shipping_state=row['shipping_state'],
                shipping_state_code=row['shipping_state_code'],
                prepaid_commission_percentage=row['prepaid_commission_percentage'],
                prepaid_minimum_commission=row['prepaid_minimum_commission'],
                prepaid_platform_fees=row['prepaid_platform_fees'],
                prepaid_total_commission=row['prepaid_total_commission'],
                prepaid_ship_commission_charge=row['prepaid_ship_commission_charge'],
                prepaid_gift_commission_charge=row['prepaid_gift_commission_charge'],
                prepaid_cod_commission_charge=row['prepaid_cod_commission_charge'],
                prepaid_cart_discount=row['prepaid_cart_discount'],
                prepaid_coupon_discount=row['prepaid_coupon_discount'],
                postpaid_commission_percentage=row['postpaid_commission_percentage'],
                postpaid_minimum_commission=row['postpaid_minimum_commission'],
                postpaid_platform_fees=row['postpaid_platform_fees'],
                postpaid_total_commission=row['postpaid_total_commission'],
                postpaid_ship_commission_charge=row['postpaid_ship_commission_charge'],
                postpaid_gift_commission_charge=row['postpaid_gift_commission_charge'],
                postpaid_cod_commission_charge=row['postpaid_cod_commission_charge'],
                postpaid_cart_discount=row['postpaid_cart_discount'],
                postpaid_coupon_discount=row['postpaid_coupon_discount'],
                seller_order_id=row['seller_order_id'],
                tcs_amount_prepaid=row['tcs_amount_prepaid'],
                tcs_amount_postpaid=row['tcs_amount_postpaid'],
                tds_amount_prepaid=row['tds_amount_prepaid'],
                tds_amount_postpaid=row['tds_amount_postpaid'],
                seller_tier=row['seller_tier'],
                royaltyCharges_prepaid=row['royaltyCharges_prepaid'],
                royaltyCharges_postpaid=row['royaltyCharges_postpaid'],
                royaltyPercent_prepaid=row['royaltyPercent_prepaid'],
                royaltyPercent_postpaid=row['royaltyPercent_postpaid'],
                marketingCharges_prepaid=row['marketingCharges_prepaid'],
                marketingCharges_postpaid=row['marketingCharges_postpaid'],
                marketingPercent_prepaid=row['marketingPercent_prepaid'],
                marketingPercent_postpaid=row['marketingPercent_postpaid'],
                marketingContribution_prepaid=row['marketingContribution_prepaid'],
                marketingContribution_postpaid=row['marketingContribution_postpaid'],
                reverseAdditionalCharges_prepaid=row['reverseAdditionalCharges_prepaid'],
                reverseAdditionalCharges_postpaid=row['reverseAdditionalCharges_postpaid'],
                tech_enablement_charges=row['tech_enablement_charges'],
                air_logistics_charges=row['air_logistics_charges'],
                forward_additional_charges=row['forward_additional_charges'],
                status=status,
                source_file=source_file
            )

            # Check if settlement already exists
            existing_settlement = db.query(Settlement).filter(
                Settlement.order_release_id == row['order_release_id']
            ).first()

            if existing_settlement:
                # Update existing settlement
                for key, value in settlement.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing_settlement, key, value)
                settlements.append(existing_settlement)
            else:
                # Create new settlement
                settlements.append(settlement)

        except Exception as e:
            logger.error(f"Error processing settlement for order {row['order_release_id']}: {str(e)}")
    return settlements

def process_settlement_dates(df: pd.DataFrame, db: Session) -> List[SettlementDate]:
    """Process settlement dates from settlement data."""
    settlement_dates = []
    for _, row in df.iterrows():
        try:
            # Get all settlement date columns
            settlement_columns = [col for col in df.columns if col.startswith('Settlement_on_') or col.startswith('SettlementOn')]
            for col in settlement_columns:
                if pd.notna(row[col]) and float(row[col]) != 0:
                    try:
                        # Handle both date formats
                        if col.startswith('Settlement_on_'):
                            date_str = col.replace('Settlement_on_', '')
                            settlement_date = datetime.strptime(date_str, '%Y_%m_%d')
                        else:  # SettlementOnDD_ format
                            date_str = col.replace('SettlementOn', '').replace('_', '')
                            # Get current year and month from the first row's data
                            current_year = datetime.now().year
                            current_month = datetime.now().month
                            settlement_date = datetime(current_year, current_month, int(date_str))
                        
                        # Get corresponding UTR number if available
                        utr_col = f'bank_utr_no_{col.lower()}'
                        bank_utr_no = str(row[utr_col]) if utr_col in row and pd.notna(row[utr_col]) else None
                        
                        settlement_date_record = SettlementDate(
                            date=settlement_date,
                            bank_utr_no=bank_utr_no,
                            amount=float(row[col])
                        )
                        settlement_dates.append(settlement_date_record)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error processing settlement date for column {col}: {str(e)}")
                        continue
        except Exception as e:
            logger.error(f"Error processing settlement dates for order {row['order_release_id']}: {str(e)}")
    return settlement_dates

def process_files(orders_file: str, returns_file: str, settlements_file: str, db: Session, month: Optional[datetime] = None) -> None:
    """
    Process all files and populate the database.
    
    Args:
        orders_file: Path to orders file
        returns_file: Path to returns file
        settlements_file: Path to settlements file
        db: Database session
        month: Optional month to process data for (for historical data)
    """
    try:
        # Read CSV files
        orders_df = pd.read_csv(orders_file)
        returns_df = pd.read_csv(returns_file)
        settlements_df = pd.read_csv(settlements_file)

        # Convert data types
        orders_df = convert_data_types(orders_df, 'orders')
        returns_df = convert_data_types(returns_df, 'returns')
        settlements_df = convert_data_types(settlements_df, 'settlements')

        # Filter by month if specified
        if month:
            orders_df['created_on'] = pd.to_datetime(orders_df['created_on'])
            orders_df = orders_df[orders_df['created_on'].dt.to_period('M') == month.to_period('M')]
            
            returns_df['return_date'] = pd.to_datetime(returns_df['return_date'])
            returns_df = returns_df[returns_df['return_date'].dt.to_period('M') == month.to_period('M')]
            
            settlements_df['created_at'] = pd.to_datetime(settlements_df['created_at'])
            settlements_df = settlements_df[settlements_df['created_at'].dt.to_period('M') == month.to_period('M')]

        # Process and add orders
        orders = process_orders(orders_df, db, Path(orders_file).name)
        db.add_all(orders)
        db.commit()

        # Process and add returns
        returns = process_returns(returns_df, db, Path(returns_file).name)
        db.add_all(returns)
        db.commit()

        # Process and add settlements
        settlements = process_settlements(settlements_df, db, Path(settlements_file).name)
        db.add_all(settlements)
        db.commit()

        # Process and add settlement dates
        settlement_dates = process_settlement_dates(settlements_df, db)
        db.add_all(settlement_dates)
        db.commit()

        # Calculate and add monthly reconciliation
        if month:
            monthly_reconciliation = calculate_monthly_reconciliation(db, month)
            db.add(monthly_reconciliation)
            db.commit()

        logger.info("Successfully processed all files and populated database")
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        db.rollback()
        raise

def calculate_monthly_reconciliation(db: Session, month: datetime) -> MonthlyReconciliation:
    """Calculate monthly reconciliation metrics."""
    try:
        # Get all orders for the month
        orders = db.query(Order).filter(
            Order.created_on >= month.replace(day=1),
            Order.created_on < (month.replace(day=1) + pd.DateOffset(months=1))
        ).all()

        # Get all returns for the month
        returns = db.query(Return).filter(
            Return.return_date >= month.replace(day=1),
            Return.return_date < (month.replace(day=1) + pd.DateOffset(months=1))
        ).all()

        # Get all settlements for the month
        settlements = db.query(Settlement).filter(
            Settlement.created_at >= month.replace(day=1),
            Settlement.created_at < (month.replace(day=1) + pd.DateOffset(months=1))
        ).all()

        # Calculate metrics
        total_orders = len(orders)
        total_returns = len(returns)
        total_settlements = sum(s.total_actual_settlement for s in settlements if s.total_actual_settlement > 0)
        pending_settlements = sum(s.amount_pending_settlement for s in settlements)
        completed_settlements = sum(s.total_actual_settlement for s in settlements if s.status == 'settled')
        return_losses = abs(sum(s.total_actual_settlement for s in settlements if s.total_actual_settlement < 0))
        net_profit = total_settlements - return_losses

        return MonthlyReconciliation(
            month=month.date(),
            total_orders=total_orders,
            total_returns=total_returns,
            total_settlements=total_settlements,
            pending_settlements=pending_settlements,
            completed_settlements=completed_settlements,
            return_losses=return_losses,
            net_profit=net_profit
        )
    except Exception as e:
        logger.error(f"Error calculating monthly reconciliation for {month}: {str(e)}")
        raise

def analyze_settlements(session: Session, month: str) -> Dict[str, Any]:
    """Analyze settlements for a specific month."""
    try:
        # Get all settlements for the month
        settlements = session.query(Settlement).filter(
            Settlement.month == month
        ).all()
        
        # Get pending settlements from previous months
        previous_month = (datetime.strptime(month, '%Y-%m') - pd.DateOffset(months=1)).strftime('%Y-%m')
        pending_from_previous = session.query(Settlement).filter(
            Settlement.status == 'pending',
            Settlement.month == previous_month
        ).all()
        
        # Calculate metrics
        total_settlements = len(settlements)
        completed_settlements = sum(1 for s in settlements if s.status == 'settled')
        partial_settlements = sum(1 for s in settlements if s.status == 'partial')
        pending_settlements = sum(1 for s in settlements if s.status == 'pending')
        
        total_amount_settled = sum(s.total_actual_settlement for s in settlements)
        total_amount_pending = sum(s.amount_pending_settlement for s in settlements)
        
        # Calculate completion rates
        completion_rate = (completed_settlements / total_settlements * 100) if total_settlements > 0 else 0
        amount_completion_rate = (total_amount_settled / (total_amount_settled + total_amount_pending) * 100) if (total_amount_settled + total_amount_pending) > 0 else 0
        
        # Calculate average settlement time
        completed_settlements_with_dates = [
            s for s in settlements 
            if s.status == 'settled' and s.settlement_date
        ]
        if completed_settlements_with_dates:
            avg_settlement_time = sum(
                (s.settlement_date - s.order.created_on.date()).days 
                for s in completed_settlements_with_dates
            ) / len(completed_settlements_with_dates)
        else:
            avg_settlement_time = 0
        
        # Get settlement trends
        trends = SettlementHistory.get_settlement_trends(
            session,
            start_month=previous_month,
            end_month=month
        )
        
        return {
            'total_settlements': total_settlements,
            'completed_settlements': completed_settlements,
            'partial_settlements': partial_settlements,
            'pending_settlements': pending_settlements,
            'total_amount_settled': total_amount_settled,
            'total_amount_pending': total_amount_pending,
            'completion_rate': completion_rate,
            'amount_completion_rate': amount_completion_rate,
            'avg_settlement_time': avg_settlement_time,
            'pending_from_previous': len(pending_from_previous),
            'trends': trends
        }
        
    except Exception as e:
        logger.error(f"Error analyzing settlements for {month}: {str(e)}")
        raise Exception(f"Error analyzing settlements: {str(e)}")

def convert_float(value: str) -> Optional[float]:
    """
    Convert string to float with better error handling.
    
    Args:
        value: String value to convert
    
    Returns:
        Float value if successful, None otherwise
    """
    if not value or pd.isna(value):
        return None
    try:
        # Remove any currency symbols and commas
        cleaned_value = str(value).replace('₹', '').replace(',', '').strip()
        return float(cleaned_value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Error converting value to float: {value}, error: {str(e)}")
        return None

def convert_date(date_str: str) -> Optional[datetime]:
    """
    Convert date string to datetime object with better error handling.
    
    Args:
        date_str: Date string in various formats
    
    Returns:
        datetime object if successful, None otherwise
    """
    if not date_str or pd.isna(date_str):
        return None
    
    try:
        # Try common date formats
        for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d', '%m/%d/%y %H:%M']:
            try:
                return datetime.strptime(str(date_str).strip(), fmt)
            except ValueError:
                continue
        logger.warning(f"Could not parse date string: {date_str}")
        return None
    except Exception as e:
        logger.warning(f"Error converting date string: {date_str}, error: {str(e)}")
        return None 