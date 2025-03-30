"""
Reporting and visualization module for reconciliation application.
"""
import os
import logging
from typing import Dict, List, Optional, Any
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns
from pathlib import Path
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from src.cache import cache
from src.optimization import QueryOptimizer

from utils import (
    ensure_directories_exist, read_file, ANALYSIS_OUTPUT, REPORT_OUTPUT,
    VISUALIZATION_DIR, ANOMALIES_OUTPUT, REPORT_DIR
)

logger = logging.getLogger(__name__)

def save_analysis_results(analysis_df: pd.DataFrame) -> None:
    """
    Save analysis results to CSV file.
    
    Args:
        analysis_df: Analysis results DataFrame
    """
    ensure_directories_exist()
    analysis_df.to_csv(ANALYSIS_OUTPUT, index=False)

def generate_report(summary: Dict) -> str:
    """
    Generate a text report from analysis summary.
    
    Args:
        summary: Analysis summary dictionary
    
    Returns:
        Formatted report string
    """
    # Calculate percentages
    total_orders = summary['total_orders']
    cancelled_pct = (summary['status_counts'].get('Cancelled', 0) / total_orders * 100) if total_orders > 0 else 0
    returned_pct = (summary['status_counts'].get('Returned', 0) / total_orders * 100) if total_orders > 0 else 0
    settled_pct = (summary['status_counts'].get('Completed - Settled', 0) / total_orders * 100) if total_orders > 0 else 0
    pending_pct = (summary['status_counts'].get('Completed - Pending Settlement', 0) / total_orders * 100) if total_orders > 0 else 0
    
    # Calculate average values
    avg_profit = (summary['total_order_settlement'] / summary['status_counts'].get('Completed - Settled', 1)) if summary['status_counts'].get('Completed - Settled', 0) > 0 else 0
    avg_loss = (summary['total_return_settlement'] / summary['status_counts'].get('Returned', 1)) if summary['status_counts'].get('Returned', 0) > 0 else 0
    
    report = [
        "=== Order Reconciliation Report ===",
        "",
        "=== Order Counts ===",
        f"Total Orders: {total_orders}",
        f"Cancelled Orders: {summary['status_counts'].get('Cancelled', 0)} ({cancelled_pct:.2f}%)",
        f"Returned Orders: {summary['status_counts'].get('Returned', 0)} ({returned_pct:.2f}%)",
        f"Completed and Settled Orders: {summary['status_counts'].get('Completed - Settled', 0)} ({settled_pct:.2f}%)",
        f"Completed but Pending Settlement Orders: {summary['status_counts'].get('Completed - Pending Settlement', 0)} ({pending_pct:.2f}%)",
        "",
        "=== Financial Analysis ===",
        f"Total Profit from Settled Orders: {summary['total_order_settlement']:.2f}",
        f"Total Loss from Returned Orders: {abs(summary['total_return_settlement']):.2f}",
        f"Net Profit/Loss: {summary['net_profit_loss']:.2f}",
        "",
        "=== Settlement Information ===",
        f"Total Return Settlement Amount: ₹{abs(summary['total_return_settlement']):,.2f}",
        f"Total Order Settlement Amount: ₹{summary['total_order_settlement']:,.2f}",
        f"Potential Settlement Value (Pending): ₹{summary.get('pending_settlement_value', 0):,.2f}",
        f"Status Changes in This Run: {summary['status_changes']}",
        f"Orders Newly Settled in This Run: {summary['settlement_changes']}",
        f"Orders Newly Pending in This Run: {summary['pending_changes']}",
        "",
        "=== Key Metrics ===",
        f"Settlement Rate: {summary['settlement_rate']:.2f}%",
        f"Return Rate: {summary['return_rate']:.2f}%",
        f"Average Profit per Settled Order: {avg_profit:.2f}",
        f"Average Loss per Returned Order: {abs(avg_loss):.2f}",
        "",
        "=== Recommendations ===",
        "1. Monitor orders with status 'Completed - Pending Settlement' to ensure they get settled.",
        "2. Analyze orders with high losses to identify patterns and potential issues.",
        "3. Investigate return patterns to reduce return rates.",
        "4. Consider strategies to increase settlement rates for shipped orders."
    ]
    
    return "\n".join(report)

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

def generate_visualizations(analysis_df: pd.DataFrame, summary: Dict) -> Dict[str, go.Figure]:
    """
    Generate interactive visualizations using Plotly.
    
    Args:
        analysis_df: Analysis results DataFrame
        summary: Analysis summary dictionary
    
    Returns:
        Dictionary mapping visualization names to Plotly figures
    """
    figures = {}
    
    # Order Status Distribution
    status_counts = analysis_df['status'].value_counts()
    fig = px.pie(
        values=status_counts.values,
        names=status_counts.index,
        title="Order Status Distribution"
    )
    figures['status_distribution'] = fig
    
    # Profit/Loss Distribution
    profit_loss_data = analysis_df[analysis_df['profit_loss'].notna()]
    fig = px.histogram(
        profit_loss_data,
        x='profit_loss',
        title="Profit/Loss Distribution",
        nbins=50
    )
    fig.add_vline(x=0, line_dash="dash", line_color="red")
    figures['profit_loss_distribution'] = fig
    
    # Monthly Trends
    if 'source_file' in analysis_df.columns:
        # Extract month and year from source file names
        def extract_month_year(filename: str) -> str:
            # Example filename: 'orders-02-2025.csv' -> '2025-02'
            parts = filename.split('-')
            if len(parts) >= 3:
                month = parts[1]
                year = parts[2].split('.')[0]
                return f"{year}-{month}"
            return "Unknown"
        
        analysis_df['month_year'] = analysis_df['source_file'].apply(extract_month_year)
        
        monthly_stats = analysis_df.groupby('month_year').agg({
            'order_release_id': 'count',
            'profit_loss': 'sum',
            'status': lambda x: (x == 'Completed - Settled').mean() * 100
        }).reset_index()
        
        monthly_stats.columns = ['Month', 'Total Orders', 'Net Profit/Loss', 'Settlement Rate']
        
        # Sort by month-year
        monthly_stats['Month'] = pd.to_datetime(monthly_stats['Month'])
        monthly_stats = monthly_stats.sort_values('Month')
        monthly_stats['Month'] = monthly_stats['Month'].dt.strftime('%Y-%m')
        
        # Orders Trend
        fig = px.line(
            monthly_stats,
            x='Month',
            y='Total Orders',
            title="Monthly Orders Trend"
        )
        figures['monthly_orders_trend'] = fig
        
        # Profit/Loss Trend
        fig = px.line(
            monthly_stats,
            x='Month',
            y='Net Profit/Loss',
            title="Monthly Profit/Loss Trend"
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red")
        figures['monthly_profit_loss_trend'] = fig
        
        # Settlement Rate Trend
        fig = px.line(
            monthly_stats,
            x='Month',
            y='Settlement Rate',
            title="Monthly Settlement Rate Trend"
        )
        figures['monthly_settlement_rate_trend'] = fig
    
    # Settlement Changes
    if 'status_changed_this_run' in analysis_df.columns:
        settlement_changes = analysis_df[
            (analysis_df['status_changed_this_run']) &
            (analysis_df['status'] == 'Completed - Settled')
        ]
        
        if not settlement_changes.empty:
            fig = px.bar(
                settlement_changes,
                x='settlement_update_run_timestamp',
                y='profit_loss',
                title="Settlement Changes in Last Run",
                labels={
                    'settlement_update_run_timestamp': 'Settlement Date',
                    'profit_loss': 'Profit/Loss'
                }
            )
            fig.add_hline(y=0, line_dash="dash", line_color="red")
            figures['settlement_changes'] = fig
    
    # Save the figures to files
    save_visualizations(figures)
    
    return figures

def identify_anomalies(
    analysis_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    returns_df: pd.DataFrame,
    settlement_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Identify anomalies in the data.
    
    Args:
        analysis_df: Analysis results DataFrame
        orders_df: Orders DataFrame
        returns_df: Returns DataFrame
        settlement_df: Settlement DataFrame
    
    Returns:
        DataFrame containing identified anomalies
    """
    anomalies = []
    
    # Check for orders with negative profit/loss
    negative_profit = analysis_df[analysis_df['profit_loss'] < 0]
    if not negative_profit.empty:
        anomalies.extend([
            {
                'type': 'Negative Profit',
                'order_release_id': row['order_release_id'],
                'details': f"Profit/Loss: ₹{row['profit_loss']:,.2f}"
            }
            for _, row in negative_profit.iterrows()
        ])
    
    # Check for orders with missing settlement data
    pending_settlement = analysis_df[
        analysis_df['status'] == 'Completed - Pending Settlement'
    ]
    if not pending_settlement.empty:
        anomalies.extend([
            {
                'type': 'Missing Settlement',
                'order_release_id': row['order_release_id'],
                'details': f"Order Amount: ₹{row['final_amount']:,.2f}"
            }
            for _, row in pending_settlement.iterrows()
        ])
    
    # Check for orders with both return and settlement data
    conflict_orders = analysis_df[
        (analysis_df['return_settlement'] > 0) &
        (analysis_df['order_settlement'] > 0)
    ]
    if not conflict_orders.empty:
        anomalies.extend([
            {
                'type': 'Return/Settlement Conflict',
                'order_release_id': row['order_release_id'],
                'details': f"Return: ₹{row['return_settlement']:,.2f}, Settlement: ₹{row['order_settlement']:,.2f}"
            }
            for _, row in conflict_orders.iterrows()
        ])
    
    # Create anomalies DataFrame
    anomalies_df = pd.DataFrame(anomalies)
    
    # Save anomalies to file
    if not anomalies_df.empty:
        anomalies_df.to_csv(ANOMALIES_OUTPUT, index=False)
    
    return anomalies_df

class RealTimeReporter:
    def __init__(self, session: Session):
        self.session = session
        self.query_optimizer = QueryOptimizer(session)
        self.cache = cache

    def get_daily_summary(self, date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get daily summary of orders, returns, and settlements."""
        date = date or datetime.now()
        cache_key = f"daily_summary:{date.strftime('%Y-%m-%d')}"

        def fetch_summary():
            try:
                # Get orders summary
                orders_query = """
                SELECT 
                    COUNT(*) as total_orders,
                    SUM(CASE WHEN order_status = 'D' THEN 1 ELSE 0 END) as delivered_orders,
                    SUM(CASE WHEN order_status = 'R' THEN 1 ELSE 0 END) as returned_orders,
                    SUM(final_amount) as total_order_amount
                FROM orders
                WHERE DATE(created_on) = :date
                """
                
                # Get returns summary
                returns_query = """
                SELECT 
                    COUNT(*) as total_returns,
                    SUM(CASE WHEN return_type = 'return_refund' THEN 1 ELSE 0 END) as refund_returns,
                    SUM(CASE WHEN return_type = 'exchange' THEN 1 ELSE 0 END) as exchange_returns,
                    SUM(amount_pending_settlement) as pending_settlements
                FROM returns
                WHERE DATE(return_date) = :date
                """
                
                # Get settlements summary
                settlements_query = """
                SELECT 
                    COUNT(*) as total_settlements,
                    SUM(CASE WHEN settlement_status = 'completed' THEN 1 ELSE 0 END) as completed_settlements,
                    SUM(CASE WHEN settlement_status = 'pending' THEN 1 ELSE 0 END) as pending_settlements,
                    SUM(total_actual_settlement) as total_settlement_amount
                FROM settlements
                WHERE DATE(created_at) = :date
                """
                
                orders_summary = self.session.execute(text(orders_query), {"date": date}).first()
                returns_summary = self.session.execute(text(returns_query), {"date": date}).first()
                settlements_summary = self.session.execute(text(settlements_query), {"date": date}).first()
                
                return {
                    "date": date.strftime("%Y-%m-%d"),
                    "orders": dict(orders_summary) if orders_summary else {},
                    "returns": dict(returns_summary) if returns_summary else {},
                    "settlements": dict(settlements_summary) if settlements_summary else {}
                }
            except Exception as e:
                logger.error(f"Error fetching daily summary: {str(e)}")
                raise

        return self.query_optimizer.get_cached_query(cache_key, fetch_summary, expire=300)

    def check_data_consistency(self) -> List[Dict[str, Any]]:
        """Perform data consistency checks."""
        try:
            issues = []

            # Check for orphaned returns
            orphaned_returns_query = """
            SELECT r.order_release_id, r.order_line_id
            FROM returns r
            LEFT JOIN orders o ON r.order_release_id = o.order_release_id
            WHERE o.order_release_id IS NULL
            """
            orphaned_returns = self.session.execute(text(orphaned_returns_query)).fetchall()
            if orphaned_returns:
                issues.append({
                    "type": "orphaned_returns",
                    "count": len(orphaned_returns),
                    "details": [dict(row) for row in orphaned_returns]
                })

            # Check for mismatched settlement amounts
            settlement_mismatch_query = """
            SELECT s.order_release_id, s.order_line_id,
                   s.total_expected_settlement, s.total_actual_settlement,
                   s.amount_pending_settlement
            FROM settlements s
            WHERE s.total_expected_settlement != 
                  (s.total_actual_settlement + s.amount_pending_settlement)
            """
            settlement_mismatches = self.session.execute(text(settlement_mismatch_query)).fetchall()
            if settlement_mismatches:
                issues.append({
                    "type": "settlement_mismatch",
                    "count": len(settlement_mismatches),
                    "details": [dict(row) for row in settlement_mismatches]
                })

            # Check for invalid return dates
            invalid_return_dates_query = """
            SELECT r.order_release_id, r.order_line_id,
                   r.return_date, o.created_on
            FROM returns r
            JOIN orders o ON r.order_release_id = o.order_release_id
            WHERE r.return_date < o.created_on
            """
            invalid_return_dates = self.session.execute(text(invalid_return_dates_query)).fetchall()
            if invalid_return_dates:
                issues.append({
                    "type": "invalid_return_dates",
                    "count": len(invalid_return_dates),
                    "details": [dict(row) for row in invalid_return_dates]
                })

            return issues
        except Exception as e:
            logger.error(f"Error checking data consistency: {str(e)}")
            raise

    def get_reconciliation_status(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get reconciliation status for a date range."""
        cache_key = f"reconciliation_status:{start_date.strftime('%Y-%m-%d')}:{end_date.strftime('%Y-%m-%d')}"

        def fetch_status():
            try:
                # Get orders and returns summary
                summary_query = """
                SELECT 
                    COUNT(DISTINCT o.order_release_id) as total_orders,
                    COUNT(DISTINCT r.order_release_id) as total_returns,
                    SUM(o.final_amount) as total_order_amount,
                    SUM(r.amount_pending_settlement) as total_pending_settlements
                FROM orders o
                LEFT JOIN returns r ON o.order_release_id = r.order_release_id
                WHERE o.created_on BETWEEN :start_date AND :end_date
                """
                
                # Get settlement status
                settlement_query = """
                SELECT 
                    COUNT(*) as total_settlements,
                    SUM(CASE WHEN settlement_status = 'completed' THEN 1 ELSE 0 END) as completed_settlements,
                    SUM(CASE WHEN settlement_status = 'partial' THEN 1 ELSE 0 END) as partial_settlements,
                    SUM(CASE WHEN settlement_status = 'pending' THEN 1 ELSE 0 END) as pending_settlements
                FROM settlements
                WHERE created_at BETWEEN :start_date AND :end_date
                """
                
                summary = self.session.execute(text(summary_query), {
                    "start_date": start_date,
                    "end_date": end_date
                }).first()
                
                settlements = self.session.execute(text(settlement_query), {
                    "start_date": start_date,
                    "end_date": end_date
                }).first()
                
                return {
                    "period": {
                        "start": start_date.strftime("%Y-%m-%d"),
                        "end": end_date.strftime("%Y-%m-%d")
                    },
                    "summary": dict(summary) if summary else {},
                    "settlements": dict(settlements) if settlements else {}
                }
            except Exception as e:
                logger.error(f"Error fetching reconciliation status: {str(e)}")
                raise

        return self.query_optimizer.get_cached_query(cache_key, fetch_status, expire=300)

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics."""
        try:
            # Get database statistics
            db_stats_query = """
            SELECT 
                COUNT(*) as total_orders,
                COUNT(DISTINCT order_release_id) as unique_orders,
                COUNT(*) FILTER (WHERE order_status = 'D') as delivered_orders,
                COUNT(*) FILTER (WHERE order_status = 'R') as returned_orders
            FROM orders
            """
            
            # Get cache statistics
            cache_stats = {
                "keys": len(self.cache.redis_client.keys("*")),
                "memory_usage": self.cache.redis_client.info(section="memory")["used_memory_human"]
            }
            
            # Get query performance statistics
            query_stats = self.query_optimizer.get_query_stats(db_stats_query)
            
            return {
                "database": dict(self.session.execute(text(db_stats_query)).first()),
                "cache": cache_stats,
                "query_performance": query_stats
            }
        except Exception as e:
            logger.error(f"Error fetching performance metrics: {str(e)}")
            raise 