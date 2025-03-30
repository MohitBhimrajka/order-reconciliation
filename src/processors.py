import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from .models import Order, Return, Settlement, SettlementDate, MonthlyReconciliation
from typing import List, Dict, Any
import logging

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

def process_orders(df: pd.DataFrame, db: Session) -> List[Order]:
    """Process orders data and create Order records."""
    orders = []
    for _, row in df.iterrows():
        try:
            order = Order(
                order_release_id=str(row['order_release_id']),
                order_line_id=str(row['order_line_id']),
                seller_order_id=str(row['seller_order_id']) if pd.notna(row['seller_order_id']) else None,
                created_on=parse_date(row['created on']),
                delivered_on=parse_date(row['delivered on']),
                cancelled_on=parse_date(row['cancelled on']),
                final_amount=float(row['final amount']),
                total_mrp=float(row['total mrp']),
                discount=float(row['discount']),
                shipping_charge=float(row['shipping charge']),
                order_status=str(row['order status']),
                payment_type='prepaid' if float(row['prepaid_amount']) > 0 else 'postpaid',
                city=str(row['city']) if pd.notna(row['city']) else None,
                state=str(row['state']) if pd.notna(row['state']) else None,
                zipcode=str(row['zipcode']) if pd.notna(row['zipcode']) else None
            )
            orders.append(order)
        except Exception as e:
            logger.error(f"Error processing order {row['order_release_id']}: {str(e)}")
    return orders

def process_returns(df: pd.DataFrame, db: Session) -> List[Return]:
    """Process returns data and create Return records."""
    returns = []
    for _, row in df.iterrows():
        try:
            return_record = Return(
                order_release_id=str(row['order_release_id']),
                order_line_id=str(row['order_line_id']),
                return_type=str(row['return_type']),
                return_date=parse_date(row['return_date']),
                packing_date=parse_date(row['packing_date']),
                delivery_date=parse_date(row['delivery_date']),
                customer_paid_amount=float(row['customer_paid_amount']),
                prepaid_amount=float(row['prepaid_amount']),
                postpaid_amount=float(row['postpaid_amount']),
                mrp=float(row['mrp']),
                total_discount_amount=float(row['total_discount_amount']),
                total_settlement=float(row['total_settlement']),
                total_actual_settlement=float(row['total_actual_settlement']),
                amount_pending_settlement=float(row['amount_pending_settlement'])
            )
            returns.append(return_record)
        except Exception as e:
            logger.error(f"Error processing return for order {row['order_release_id']}: {str(e)}")
    return returns

def process_settlements(df: pd.DataFrame, db: Session) -> List[Settlement]:
    """Process settlements data and create Settlement records."""
    settlements = []
    for _, row in df.iterrows():
        try:
            settlement = Settlement(
                order_release_id=str(row['order_release_id']),
                order_line_id=str(row['order_line_id']),
                total_expected_settlement=float(row['total_expected_settlement']),
                total_actual_settlement=float(row['total_actual_settlement']),
                amount_pending_settlement=float(row['amount_pending_settlement']),
                prepaid_commission_deduction=float(row['prepaid_commission_deduction']),
                prepaid_logistics_deduction=float(row['prepaid_logistics_deduction']),
                prepaid_payment=float(row['prepaid_payment']),
                postpaid_commission_deduction=float(row['postpaid_commission_deduction']),
                postpaid_logistics_deduction=float(row['postpaid_logistics_deduction']),
                postpaid_payment=float(row['postpaid_payment']),
                settlement_status='completed' if float(row['amount_pending_settlement']) == 0 else 'partial' if float(row['amount_pending_settlement']) < float(row['total_expected_settlement']) else 'pending'
            )
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
            settlement_columns = [col for col in df.columns if col.startswith('Settlement_on_')]
            for col in settlement_columns:
                if pd.notna(row[col]) and float(row[col]) != 0:
                    settlement_date = SettlementDate(
                        settlement_date=parse_date(col.replace('Settlement_on_', '')),
                        settlement_amount=float(row[col]),
                        bank_utr_no=str(row[f'bank_utr_no_{col.lower()}']) if f'bank_utr_no_{col.lower()}' in row else None
                    )
                    settlement_dates.append(settlement_date)
        except Exception as e:
            logger.error(f"Error processing settlement dates for order {row['order_release_id']}: {str(e)}")
    return settlement_dates

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
        completed_settlements = sum(s.total_actual_settlement for s in settlements if s.settlement_status == 'completed')
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

def process_files(orders_file: str, returns_file: str, settlements_file: str, db: Session) -> None:
    """Process all files and populate the database."""
    try:
        # Read CSV files
        orders_df = pd.read_csv(orders_file)
        returns_df = pd.read_csv(returns_file)
        settlements_df = pd.read_csv(settlements_file)

        # Process and add orders
        orders = process_orders(orders_df, db)
        db.add_all(orders)
        db.commit()

        # Process and add returns
        returns = process_returns(returns_df, db)
        db.add_all(returns)
        db.commit()

        # Process and add settlements
        settlements = process_settlements(settlements_df, db)
        db.add_all(settlements)
        db.commit()

        # Process and add settlement dates
        settlement_dates = process_settlement_dates(settlements_df, db)
        db.add_all(settlement_dates)
        db.commit()

        # Calculate and add monthly reconciliation
        current_month = datetime.now().replace(day=1)
        monthly_reconciliation = calculate_monthly_reconciliation(db, current_month)
        db.add(monthly_reconciliation)
        db.commit()

        logger.info("Successfully processed all files and populated database")
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        db.rollback()
        raise 