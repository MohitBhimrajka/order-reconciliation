import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from .models import Order, Return, Settlement, SettlementDate, MonthlyReconciliation, SettlementHistory
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

def process_settlements(session: Session, settlements_data: List[Dict]) -> None:
    """Process settlement data and create/update settlement records with history tracking."""
    try:
        for settlement_data in settlements_data:
            # Get or create settlement record
            settlement = session.query(Settlement).filter(
                Settlement.order_release_id == settlement_data['order_release_id']
            ).first()
            
            # Get the order to check its month and amount
            order = session.query(Order).filter(
                Order.order_release_id == settlement_data['order_release_id']
            ).first()
            
            if not order:
                logger.warning(f"Order not found for settlement: {settlement_data['order_release_id']}")
                continue
            
            # Calculate settlement status based on amounts
            amount_settled = float(settlement_data['amount_settled'])
            amount_pending = float(settlement_data['amount_pending'])
            total_amount = float(order.final_amount)
            
            # Determine settlement status
            if amount_pending == 0:
                status = 'completed'
            elif amount_settled > 0:
                status = 'partial'
            else:
                status = 'pending'
            
            if not settlement:
                # Create new settlement
                settlement = Settlement(
                    order_release_id=settlement_data['order_release_id'],
                    settlement_date=settlement_data['settlement_date'],
                    settlement_status=status,
                    amount_settled=amount_settled,
                    amount_pending=amount_pending,
                    month=settlement_data['month']
                )
                session.add(settlement)
                
                # Create initial history record
                history = SettlementHistory(
                    order_release_id=settlement_data['order_release_id'],
                    settlement_date=settlement_data['settlement_date'],
                    settlement_status=status,
                    amount_settled=amount_settled,
                    amount_pending=amount_pending,
                    month=settlement_data['month']
                )
                session.add(history)
            else:
                # Check if this is a status change
                status_changed = settlement.settlement_status != status
                amount_changed = settlement.amount_settled != amount_settled
                
                if status_changed or amount_changed:
                    # Update settlement
                    settlement.update_settlement(
                        amount_settled=amount_settled,
                        status=status
                    )
                    
                    # Create history record for the change
                    history = SettlementHistory(
                        order_release_id=settlement_data['order_release_id'],
                        settlement_date=settlement_data['settlement_date'],
                        settlement_status=status,
                        amount_settled=amount_settled,
                        amount_pending=amount_pending,
                        month=settlement_data['month']
                    )
                    session.add(history)
            
            # Handle cross-month settlement tracking
            if status == 'pending' and order.month != settlement_data['month']:
                # This is a pending settlement from a previous month
                # Create a history record to track it
                history = SettlementHistory(
                    order_release_id=settlement_data['order_release_id'],
                    settlement_date=settlement_data['settlement_date'],
                    settlement_status='pending',
                    amount_settled=amount_settled,
                    amount_pending=amount_pending,
                    month=order.month  # Use the original order month
                )
                session.add(history)
        
        session.commit()
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing settlements: {str(e)}")
        raise Exception(f"Error processing settlements: {str(e)}")

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
        settlements = process_settlements(db, settlements_df.to_dict(orient='records'))

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
            Settlement.settlement_status == 'pending',
            Settlement.month == previous_month
        ).all()
        
        # Calculate metrics
        total_settlements = len(settlements)
        completed_settlements = sum(1 for s in settlements if s.settlement_status == 'completed')
        partial_settlements = sum(1 for s in settlements if s.settlement_status == 'partial')
        pending_settlements = sum(1 for s in settlements if s.settlement_status == 'pending')
        
        total_amount_settled = sum(s.amount_settled for s in settlements)
        total_amount_pending = sum(s.amount_pending for s in settlements)
        
        # Calculate completion rates
        completion_rate = (completed_settlements / total_settlements * 100) if total_settlements > 0 else 0
        amount_completion_rate = (total_amount_settled / (total_amount_settled + total_amount_pending) * 100) if (total_amount_settled + total_amount_pending) > 0 else 0
        
        # Calculate average settlement time
        completed_settlements_with_dates = [
            s for s in settlements 
            if s.settlement_status == 'completed' and s.settlement_date
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