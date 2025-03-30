"""
Core analysis module for reconciliation application.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from src.database.models import Order, Return, Settlement, OrderStatusHistory
from src.database.config import get_db

logger = logging.getLogger(__name__)

def analyze_order_metrics(session: Session) -> Dict:
    """
    Calculate key metrics for orders.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary containing metrics
    """
    try:
        # Get total orders
        total_orders = session.query(func.count(Order.id)).scalar()
        
        # Get latest status for each order
        latest_statuses = session.query(
            OrderStatusHistory.order_id,
            OrderStatusHistory.status
        ).distinct(
            OrderStatusHistory.order_id
        ).order_by(
            OrderStatusHistory.order_id,
            OrderStatusHistory.created_at.desc()
        ).subquery()
        
        # Count orders by status
        status_counts = session.query(
            latest_statuses.c.status,
            func.count()
        ).group_by(
            latest_statuses.c.status
        ).all()
        
        # Calculate status percentages
        status_metrics = {
            status: {
                'count': count,
                'percentage': round((count / total_orders) * 100, 2)
            }
            for status, count in status_counts
        }
        
        return {
            'total_orders': total_orders,
            'status_metrics': status_metrics
        }
        
    except Exception as e:
        logger.error(f"Error analyzing order metrics: {e}")
        return {}

def analyze_financial_metrics(session: Session) -> Dict:
    """
    Calculate financial metrics.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary containing financial metrics
    """
    try:
        # Calculate total profit from settled orders
        settled_profit = session.query(
            func.sum(Settlement.total_actual_settlement)
        ).filter(
            Settlement.return_type.is_(None)
        ).scalar() or Decimal('0')
        
        # Calculate total loss from returns
        return_loss = session.query(
            func.sum(Settlement.total_actual_settlement)
        ).filter(
            Settlement.return_type.is_not(None)
        ).scalar() or Decimal('0')
        
        # Calculate net profit/loss
        net_profit_loss = settled_profit - abs(return_loss)
        
        # Calculate average metrics
        avg_profit_per_settled = session.query(
            func.avg(Settlement.total_actual_settlement)
        ).filter(
            Settlement.return_type.is_(None)
        ).scalar() or Decimal('0')
        
        avg_loss_per_return = session.query(
            func.avg(Settlement.total_actual_settlement)
        ).filter(
            Settlement.return_type.is_not(None)
        ).scalar() or Decimal('0')
        
        return {
            'total_profit_settled': float(settled_profit),
            'total_loss_returns': float(return_loss),
            'net_profit_loss': float(net_profit_loss),
            'avg_profit_per_settled': float(avg_profit_per_settled),
            'avg_loss_per_return': float(abs(avg_loss_per_return))
        }
        
    except Exception as e:
        logger.error(f"Error analyzing financial metrics: {e}")
        return {}

def analyze_settlement_metrics(session: Session) -> Dict:
    """
    Calculate settlement-related metrics.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary containing settlement metrics
    """
    try:
        # Get total settlements
        total_settlements = session.query(
            func.count(Settlement.id)
        ).scalar()
        
        # Get total return settlements
        return_settlements = session.query(
            func.count(Settlement.id)
        ).filter(
            Settlement.return_type.is_not(None)
        ).scalar()
        
        # Get total order settlements
        order_settlements = session.query(
            func.count(Settlement.id)
        ).filter(
            Settlement.return_type.is_(None)
        ).scalar()
        
        # Calculate potential settlement value
        potential_settlement = session.query(
            func.sum(Order.final_amount)
        ).outerjoin(
            Settlement,
            Order.id == Settlement.order_id
        ).filter(
            Settlement.id.is_(None)
        ).scalar() or Decimal('0')
        
        # Calculate settlement rates
        total_orders = session.query(func.count(Order.id)).scalar()
        settlement_rate = (order_settlements / total_orders * 100) if total_orders > 0 else 0
        
        total_returns = session.query(func.count(Return.id)).scalar()
        return_settlement_rate = (return_settlements / total_returns * 100) if total_returns > 0 else 0
        
        return {
            'total_settlements': total_settlements,
            'return_settlements': return_settlements,
            'order_settlements': order_settlements,
            'potential_settlement_value': float(potential_settlement),
            'settlement_rate': round(settlement_rate, 2),
            'return_settlement_rate': round(return_settlement_rate, 2)
        }
        
    except Exception as e:
        logger.error(f"Error analyzing settlement metrics: {e}")
        return {}

def analyze_return_metrics(session: Session) -> Dict:
    """
    Calculate return-related metrics.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary containing return metrics
    """
    try:
        # Get total returns
        total_returns = session.query(func.count(Return.id)).scalar()
        
        # Calculate return rate
        total_orders = session.query(func.count(Order.id)).scalar()
        return_rate = (total_returns / total_orders * 100) if total_orders > 0 else 0
        
        # Get returns by type
        return_types = session.query(
            Return.return_type,
            func.count()
        ).group_by(
            Return.return_type
        ).all()
        
        # Calculate type percentages
        type_metrics = {
            rtype: {
                'count': count,
                'percentage': round((count / total_returns) * 100, 2)
            }
            for rtype, count in return_types
        }
        
        # Calculate average return processing time
        avg_processing_time = session.query(
            func.avg(
                func.extract('epoch', Return.delivery_date - Return.return_date) / 86400
            )
        ).scalar() or 0
        
        return {
            'total_returns': total_returns,
            'return_rate': round(return_rate, 2),
            'return_types': type_metrics,
            'avg_processing_days': round(avg_processing_time, 1)
        }
        
    except Exception as e:
        logger.error(f"Error analyzing return metrics: {e}")
        return {}

def identify_anomalies(session: Session) -> Dict:
    """
    Identify potential anomalies in the data.
    
    Args:
        session: Database session
    
    Returns:
        Dictionary containing identified anomalies
    """
    try:
        anomalies = {
            'negative_settlements': [],
            'high_value_returns': [],
            'delayed_settlements': [],
            'multiple_returns': []
        }
        
        # Find negative settlements
        negative_settlements = session.query(
            Settlement
        ).filter(
            Settlement.total_actual_settlement < 0,
            Settlement.return_type.is_(None)
        ).all()
        
        anomalies['negative_settlements'] = [
            {
                'order_id': s.order_release_id,
                'amount': float(s.total_actual_settlement)
            }
            for s in negative_settlements
        ]
        
        # Find high value returns (> 2x average)
        avg_return = session.query(
            func.avg(Return.customer_paid_amount)
        ).scalar() or 0
        
        high_returns = session.query(
            Return
        ).filter(
            Return.customer_paid_amount > (avg_return * 2)
        ).all()
        
        anomalies['high_value_returns'] = [
            {
                'order_id': r.order_release_id,
                'amount': float(r.customer_paid_amount)
            }
            for r in high_returns
        ]
        
        # Find delayed settlements (> 30 days)
        delayed = session.query(
            Settlement
        ).join(
            Order,
            Settlement.order_id == Order.id
        ).filter(
            func.extract('epoch', Settlement.delivery_date - Order.delivered_on) > (30 * 86400)
        ).all()
        
        anomalies['delayed_settlements'] = [
            {
                'order_id': s.order_release_id,
                'days_delayed': round(
                    (s.delivery_date - s.order.delivered_on).total_seconds() / 86400, 1
                )
            }
            for s in delayed
        ]
        
        # Find orders with multiple returns
        multiple_returns = session.query(
            Return.order_id,
            func.count(Return.id).label('return_count')
        ).group_by(
            Return.order_id
        ).having(
            func.count(Return.id) > 1
        ).all()
        
        anomalies['multiple_returns'] = [
            {
                'order_id': str(order_id),
                'return_count': count
            }
            for order_id, count in multiple_returns
        ]
        
        return anomalies
        
    except Exception as e:
        logger.error(f"Error identifying anomalies: {e}")
        return {}

def generate_analysis_report() -> Dict:
    """
    Generate a comprehensive analysis report.
    
    Returns:
        Dictionary containing all analysis results
    """
    try:
        with get_db() as session:
            report = {
                'generated_at': datetime.utcnow().isoformat(),
                'order_metrics': analyze_order_metrics(session),
                'financial_metrics': analyze_financial_metrics(session),
                'settlement_metrics': analyze_settlement_metrics(session),
                'return_metrics': analyze_return_metrics(session),
                'anomalies': identify_anomalies(session)
            }
            
            # Generate recommendations based on metrics
            recommendations = []
            
            # Check settlement rate
            if report['settlement_metrics']['settlement_rate'] < 80:
                recommendations.append(
                    "Monitor orders with status 'Completed - Pending Settlement' "
                    "to ensure they get settled."
                )
            
            # Check return rate
            if report['return_metrics']['return_rate'] > 5:
                recommendations.append(
                    "Investigate return patterns to reduce return rates."
                )
            
            # Check negative settlements
            if len(report['anomalies']['negative_settlements']) > 0:
                recommendations.append(
                    "Review orders with negative settlement amounts."
                )
            
            # Check delayed settlements
            if len(report['anomalies']['delayed_settlements']) > 0:
                recommendations.append(
                    "Follow up on settlements that are delayed by more than 30 days."
                )
            
            report['recommendations'] = recommendations
            
            return report
            
    except Exception as e:
        logger.error(f"Error generating analysis report: {e}")
        return {} 