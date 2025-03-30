"""
Reporting and visualization module for reconciliation application.
"""
import os
import logging
from typing import Dict, List, Optional, Any
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns
from pathlib import Path
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text, func, and_, or_
from sqlalchemy.orm import Session
from src.cache import cache
from src.optimization import QueryOptimizer

from utils import (
    ensure_directories_exist, REPORT_OUTPUT,
    VISUALIZATION_DIR, REPORT_DIR
)

logger = logging.getLogger(__name__)

def generate_report(db: Session, month: Optional[datetime] = None) -> str:
    """
    Generate a text report from database analysis.
    
    Args:
        db: Database session
        month: Optional month to analyze
    
    Returns:
        Formatted report string
    """
    try:
        # Get order counts and status distribution
        status_query = text("""
            SELECT 
                COUNT(*) as total_orders,
                SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_count,
                SUM(CASE WHEN order_status = 'Returned' THEN 1 ELSE 0 END) as returned_count,
                SUM(CASE WHEN order_status = 'Completed' AND s.status = 'settled' THEN 1 ELSE 0 END) as settled_count,
                SUM(CASE WHEN order_status = 'Completed' AND s.status = 'pending' THEN 1 ELSE 0 END) as pending_count
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            WHERE (:month IS NULL OR DATE_TRUNC('month', o.created_on) = :month)
        """)
        
        status_result = db.execute(status_query, {'month': month}).first()
        total_orders = status_result.total_orders
        cancelled_count = status_result.cancelled_count
        returned_count = status_result.returned_count
        settled_count = status_result.settled_count
        pending_count = status_result.pending_count
        
        # Calculate percentages
        cancelled_pct = (cancelled_count / total_orders * 100) if total_orders > 0 else 0
        returned_pct = (returned_count / total_orders * 100) if total_orders > 0 else 0
        settled_pct = (settled_count / total_orders * 100) if total_orders > 0 else 0
        pending_pct = (pending_count / total_orders * 100) if total_orders > 0 else 0
        
        # Get financial metrics
        financial_query = text("""
            SELECT 
                SUM(CASE WHEN s.status = 'settled' THEN s.total_actual_settlement ELSE 0 END) as total_settled,
                SUM(CASE WHEN s.status = 'pending' THEN s.total_expected_settlement ELSE 0 END) as pending_settlement,
                SUM(CASE WHEN r.return_date IS NOT NULL THEN r.total_settlement ELSE 0 END) as total_return_settlement
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            LEFT JOIN returns r ON o.order_release_id = r.order_release_id
            WHERE (:month IS NULL OR DATE_TRUNC('month', o.created_on) = :month)
        """)
        
        financial_result = db.execute(financial_query, {'month': month}).first()
        total_settled = financial_result.total_settled or 0
        pending_settlement = financial_result.pending_settlement or 0
        total_return_settlement = financial_result.total_return_settlement or 0
        
        # Calculate net profit/loss
        net_profit_loss = total_settled + total_return_settlement
        
        # Get settlement metrics
        settlement_query = text("""
            SELECT 
                COUNT(*) as total_settlements,
                SUM(CASE WHEN status = 'settled' THEN 1 ELSE 0 END) as completed_settlements,
                AVG(CASE WHEN status = 'settled' 
                    THEN EXTRACT(EPOCH FROM (s.created_at - o.created_on))/86400 
                    ELSE NULL END) as avg_settlement_days
            FROM settlements s
            JOIN orders o ON s.order_release_id = o.order_release_id
            WHERE (:month IS NULL OR DATE_TRUNC('month', o.created_on) = :month)
        """)
        
        settlement_result = db.execute(settlement_query, {'month': month}).first()
        total_settlements = settlement_result.total_settlements
        completed_settlements = settlement_result.completed_settlements
        avg_settlement_days = settlement_result.avg_settlement_days or 0
        
        # Calculate rates
        settlement_rate = (completed_settlements / total_settlements * 100) if total_settlements > 0 else 0
        return_rate = (returned_count / total_orders * 100) if total_orders > 0 else 0
        
        # Generate report
        report = [
            "=== Order Reconciliation Report ===",
            "",
            "=== Order Counts ===",
            f"Total Orders: {total_orders}",
            f"Cancelled Orders: {cancelled_count} ({cancelled_pct:.2f}%)",
            f"Returned Orders: {returned_count} ({returned_pct:.2f}%)",
            f"Completed and Settled Orders: {settled_count} ({settled_pct:.2f}%)",
            f"Completed but Pending Settlement Orders: {pending_count} ({pending_pct:.2f}%)",
            "",
            "=== Financial Analysis ===",
            f"Total Profit from Settled Orders: ₹{total_settled:,.2f}",
            f"Total Loss from Returned Orders: ₹{abs(total_return_settlement):,.2f}",
            f"Net Profit/Loss: ₹{net_profit_loss:,.2f}",
            "",
            "=== Settlement Information ===",
            f"Total Return Settlement Amount: ₹{abs(total_return_settlement):,.2f}",
            f"Total Order Settlement Amount: ₹{total_settled:,.2f}",
            f"Potential Settlement Value (Pending): ₹{pending_settlement:,.2f}",
            "",
            "=== Key Metrics ===",
            f"Settlement Rate: {settlement_rate:.2f}%",
            f"Return Rate: {return_rate:.2f}%",
            f"Average Settlement Time: {avg_settlement_days:.1f} days",
            "",
            "=== Recommendations ===",
            "1. Monitor orders with status 'Completed - Pending Settlement' to ensure they get settled.",
            "2. Analyze orders with high losses to identify patterns and potential issues.",
            "3. Investigate return patterns to reduce return rates.",
            "4. Consider strategies to increase settlement rates for shipped orders."
        ]
        
        return "\n".join(report)
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise

def save_report(report: str) -> None:
    """
    Save report to text file.
    
    Args:
        report: Report text to save
    """
    ensure_directories_exist()
    
    # Generate timestamp for unique filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    timestamped_report_file = REPORT_DIR / f'reconciliation_report_{timestamp}.txt'
    
    # Save timestamped report
    with open(timestamped_report_file, 'w') as f:
        f.write(report)
    logger.info(f"Timestamped report saved to {timestamped_report_file}")
    
    # Save to standard location
    with open(REPORT_OUTPUT, 'w') as f:
        f.write(report)
    logger.info(f"Standard report saved to {REPORT_OUTPUT}")
    
    # Log report summary
    logger.info("Report Summary:")
    for line in report.split('\n'):
        if line.startswith('===') or line.startswith('Total Orders:'):
            logger.info(line)

def generate_visualizations(db: Session, month: Optional[datetime] = None) -> Dict[str, go.Figure]:
    """
    Generate interactive visualizations using Plotly.
    
    Args:
        db: Database session
        month: Optional month to analyze
    
    Returns:
        Dictionary mapping visualization names to Plotly figures
    """
    figures = {}
    
    try:
        # Order Status Distribution
        status_query = text("""
            SELECT 
                CASE 
                    WHEN order_status = 'Cancelled' THEN 'Cancelled'
                    WHEN order_status = 'Returned' THEN 'Returned'
                    WHEN order_status = 'Completed' AND s.status = 'settled' THEN 'Completed - Settled'
                    WHEN order_status = 'Completed' AND s.status = 'pending' THEN 'Completed - Pending'
                    ELSE order_status
                END as status,
                COUNT(*) as count
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            WHERE (:month IS NULL OR DATE_TRUNC('month', o.created_on) = :month)
            GROUP BY status
        """)
        
        status_results = db.execute(status_query, {'month': month}).fetchall()
        status_data = [{'status': r.status, 'count': r.count} for r in status_results]
        
        fig = px.pie(
            values=[d['count'] for d in status_data],
            names=[d['status'] for d in status_data],
            title="Order Status Distribution"
        )
        figures['status_distribution'] = fig
        
        # Monthly Trends
        monthly_query = text("""
            SELECT 
                DATE_TRUNC('month', o.created_on) as month,
                COUNT(*) as total_orders,
                SUM(CASE WHEN s.status = 'settled' THEN s.total_actual_settlement ELSE 0 END) as total_settled,
                SUM(CASE WHEN r.return_date IS NOT NULL THEN r.total_settlement ELSE 0 END) as total_return_settlement,
                SUM(CASE WHEN s.status = 'settled' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as settlement_rate
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            LEFT JOIN returns r ON o.order_release_id = r.order_release_id
            GROUP BY DATE_TRUNC('month', o.created_on)
            ORDER BY month
        """)
        
        monthly_results = db.execute(monthly_query).fetchall()
        monthly_data = [{
            'month': r.month.strftime('%Y-%m'),
            'total_orders': r.total_orders,
            'total_settled': r.total_settled or 0,
            'total_return_settlement': r.total_return_settlement or 0,
            'settlement_rate': r.settlement_rate or 0
        } for r in monthly_results]
        
        # Orders Trend
        fig = px.line(
            monthly_data,
            x='month',
            y='total_orders',
            title="Monthly Orders Trend"
        )
        figures['monthly_orders_trend'] = fig
        
        # Profit/Loss Trend
        fig = px.line(
            monthly_data,
            x='month',
            y=['total_settled', 'total_return_settlement'],
            title="Monthly Profit/Loss Trend"
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red")
        figures['monthly_profit_loss_trend'] = fig
        
        # Settlement Rate Trend
        fig = px.line(
            monthly_data,
            x='month',
            y='settlement_rate',
            title="Monthly Settlement Rate Trend"
        )
        figures['monthly_settlement_rate_trend'] = fig
        
        # Save the figures to files
        save_visualizations(figures)
        
        return figures
        
    except Exception as e:
        logger.error(f"Error generating visualizations: {str(e)}")
        raise

def save_visualizations(figures: Dict[str, go.Figure]) -> None:
    """
    Save Plotly figures to HTML files in the visualizations directory.
    
    Args:
        figures: Dictionary mapping visualization names to Plotly figures
    """
    ensure_directories_exist()
    for name, fig in figures.items():
        output_path = VISUALIZATION_DIR / f"{name}.html"
        fig.write_html(str(output_path))
        logger.info(f"Saved visualization to {output_path}")

class RealTimeReporter:
    """Real-time reporting and monitoring class."""
    
    def __init__(self, session: Session):
        self.session = session
        self.query_optimizer = QueryOptimizer()
    
    @cache(ttl=300)  # Cache for 5 minutes
    def get_daily_summary(self, date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get daily summary of orders and settlements."""
        if date is None:
            date = datetime.now().date()
            
        query = text("""
            SELECT 
                COUNT(*) as total_orders,
                SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) as completed_orders,
                SUM(CASE WHEN order_status = 'Returned' THEN 1 ELSE 0 END) as returned_orders,
                SUM(CASE WHEN s.status = 'settled' THEN s.total_actual_settlement ELSE 0 END) as total_settled,
                SUM(CASE WHEN s.status = 'pending' THEN s.total_expected_settlement ELSE 0 END) as pending_settlement
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            WHERE DATE(o.created_on) = :date
        """)
        
        result = self.session.execute(query, {'date': date}).first()
        return {
            'total_orders': result.total_orders,
            'completed_orders': result.completed_orders,
            'returned_orders': result.returned_orders,
            'total_settled': result.total_settled or 0,
            'pending_settlement': result.pending_settlement or 0
        }
    
    @cache(ttl=300)
    def check_data_consistency(self) -> List[Dict[str, Any]]:
        """Check data consistency across tables."""
        issues = []
        
        # Check for orders without settlements
        query = text("""
            SELECT o.order_release_id, o.order_status, o.created_on
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            WHERE s.order_release_id IS NULL
            AND o.order_status = 'Completed'
            ORDER BY o.created_on DESC
            LIMIT 100
        """)
        
        results = self.session.execute(query).fetchall()
        if results:
            issues.append({
                'type': 'missing_settlements',
                'count': len(results),
                'details': [{
                    'order_release_id': r.order_release_id,
                    'status': r.order_status,
                    'created_on': r.created_on
                } for r in results]
            })
        
        # Check for settlements without orders
        query = text("""
            SELECT s.order_release_id, s.status, s.created_at
            FROM settlements s
            LEFT JOIN orders o ON s.order_release_id = o.order_release_id
            WHERE o.order_release_id IS NULL
            ORDER BY s.created_at DESC
            LIMIT 100
        """)
        
        results = self.session.execute(query).fetchall()
        if results:
            issues.append({
                'type': 'orphaned_settlements',
                'count': len(results),
                'details': [{
                    'order_release_id': r.order_release_id,
                    'status': r.status,
                    'created_at': r.created_at
                } for r in results]
            })
        
        return issues
    
    @cache(ttl=300)
    def get_reconciliation_status(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get reconciliation status for a date range."""
        query = text("""
            SELECT 
                COUNT(*) as total_orders,
                SUM(CASE WHEN s.status = 'settled' THEN 1 ELSE 0 END) as settled_count,
                SUM(CASE WHEN s.status = 'pending' THEN 1 ELSE 0 END) as pending_count,
                SUM(CASE WHEN s.status = 'partial' THEN 1 ELSE 0 END) as partial_count,
                SUM(CASE WHEN s.status = 'settled' THEN s.total_actual_settlement ELSE 0 END) as total_settled,
                SUM(CASE WHEN s.status = 'pending' THEN s.total_expected_settlement ELSE 0 END) as pending_amount
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            WHERE o.created_on BETWEEN :start_date AND :end_date
        """)
        
        result = self.session.execute(query, {
            'start_date': start_date,
            'end_date': end_date
        }).first()
        
        return {
            'total_orders': result.total_orders,
            'settled_count': result.settled_count,
            'pending_count': result.pending_count,
            'partial_count': result.partial_count,
            'total_settled': result.total_settled or 0,
            'pending_amount': result.pending_amount or 0
        }
    
    @cache(ttl=300)
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for the system."""
        query = text("""
            SELECT 
                COUNT(*) as total_orders,
                AVG(CASE WHEN s.status = 'settled' 
                    THEN EXTRACT(EPOCH FROM (s.created_at - o.created_on))/86400 
                    ELSE NULL END) as avg_settlement_days,
                SUM(CASE WHEN s.status = 'settled' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as settlement_rate,
                SUM(CASE WHEN r.return_date IS NOT NULL THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as return_rate
            FROM orders o
            LEFT JOIN settlements s ON o.order_release_id = s.order_release_id
            LEFT JOIN returns r ON o.order_release_id = r.order_release_id
            WHERE o.created_on >= NOW() - INTERVAL '30 days'
        """)
        
        result = self.session.execute(query).first()
        
        return {
            'total_orders': result.total_orders,
            'avg_settlement_days': result.avg_settlement_days or 0,
            'settlement_rate': result.settlement_rate or 0,
            'return_rate': result.return_rate or 0
        } 