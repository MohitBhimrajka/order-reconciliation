"""
Report generation module for reconciliation application.
"""
import logging
from datetime import datetime
from pathlib import Path
import json
from typing import Dict, Optional
import pandas as pd

from src.analysis.core import generate_analysis_report

logger = logging.getLogger(__name__)

def format_currency(value: float) -> str:
    """Format a currency value."""
    return f"₹{value:,.2f}"

def generate_text_report(report: Dict, output_file: Optional[Path] = None) -> str:
    """
    Generate a text report from analysis results.
    
    Args:
        report: Analysis report dictionary
        output_file: Optional file path to save the report
    
    Returns:
        Formatted text report
    """
    try:
        # Format the report
        text = "=== Order Reconciliation Report ===\n\n"
        
        # Order Metrics
        text += "=== Order Counts ===\n"
        order_metrics = report['order_metrics']
        text += f"Total Orders: {order_metrics['total_orders']}\n"
        for status, metrics in order_metrics['status_metrics'].items():
            text += f"{status} Orders: {metrics['count']} ({metrics['percentage']}%)\n"
        text += "\n"
        
        # Financial Metrics
        text += "=== Financial Analysis ===\n"
        financial = report['financial_metrics']
        text += f"Total Profit from Settled Orders: {format_currency(financial['total_profit_settled'])}\n"
        text += f"Total Loss from Returns: {format_currency(abs(financial['total_loss_returns']))}\n"
        text += f"Net Profit/Loss: {format_currency(financial['net_profit_loss'])}\n"
        text += f"Average Profit per Settled Order: {format_currency(financial['avg_profit_per_settled'])}\n"
        text += f"Average Loss per Return: {format_currency(financial['avg_loss_per_return'])}\n"
        text += "\n"
        
        # Settlement Metrics
        text += "=== Settlement Information ===\n"
        settlement = report['settlement_metrics']
        text += f"Total Settlements: {settlement['total_settlements']}\n"
        text += f"Order Settlements: {settlement['order_settlements']}\n"
        text += f"Return Settlements: {settlement['return_settlements']}\n"
        text += f"Settlement Rate: {settlement['settlement_rate']}%\n"
        text += f"Return Settlement Rate: {settlement['return_settlement_rate']}%\n"
        text += f"Potential Settlement Value: {format_currency(settlement['potential_settlement_value'])}\n"
        text += "\n"
        
        # Return Metrics
        text += "=== Return Analysis ===\n"
        returns = report['return_metrics']
        text += f"Total Returns: {returns['total_returns']}\n"
        text += f"Return Rate: {returns['return_rate']}%\n"
        text += f"Average Processing Time: {returns['avg_processing_days']} days\n"
        text += "\nReturn Types:\n"
        for rtype, metrics in returns['return_types'].items():
            text += f"- {rtype}: {metrics['count']} ({metrics['percentage']}%)\n"
        text += "\n"
        
        # Anomalies
        text += "=== Anomalies ===\n"
        anomalies = report['anomalies']
        
        text += f"\nNegative Settlements ({len(anomalies['negative_settlements'])}):\n"
        for item in anomalies['negative_settlements']:
            text += f"- Order {item['order_id']}: {format_currency(item['amount'])}\n"
        
        text += f"\nHigh Value Returns ({len(anomalies['high_value_returns'])}):\n"
        for item in anomalies['high_value_returns']:
            text += f"- Order {item['order_id']}: {format_currency(item['amount'])}\n"
        
        text += f"\nDelayed Settlements ({len(anomalies['delayed_settlements'])}):\n"
        for item in anomalies['delayed_settlements']:
            text += f"- Order {item['order_id']}: {item['days_delayed']} days\n"
        
        text += f"\nMultiple Returns ({len(anomalies['multiple_returns'])}):\n"
        for item in anomalies['multiple_returns']:
            text += f"- Order {item['order_id']}: {item['return_count']} returns\n"
        text += "\n"
        
        # Recommendations
        text += "=== Recommendations ===\n"
        for i, rec in enumerate(report['recommendations'], 1):
            text += f"{i}. {rec}\n"
        
        # Save to file if specified
        if output_file:
            output_file.write_text(text)
        
        return text
        
    except Exception as e:
        logger.error(f"Error generating text report: {e}")
        return ""

def generate_json_report(report: Dict, output_file: Optional[Path] = None) -> str:
    """
    Generate a JSON report from analysis results.
    
    Args:
        report: Analysis report dictionary
        output_file: Optional file path to save the report
    
    Returns:
        JSON string
    """
    try:
        # Convert to JSON
        json_str = json.dumps(report, indent=2)
        
        # Save to file if specified
        if output_file:
            output_file.write_text(json_str)
        
        return json_str
        
    except Exception as e:
        logger.error(f"Error generating JSON report: {e}")
        return ""

def generate_excel_report(report: Dict, output_file: Path) -> bool:
    """
    Generate an Excel report from analysis results.
    
    Args:
        report: Analysis report dictionary
        output_file: File path to save the Excel report
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with pd.ExcelWriter(output_file) as writer:
            # Order Metrics
            order_df = pd.DataFrame([
                {
                    'Status': status,
                    'Count': metrics['count'],
                    'Percentage': metrics['percentage']
                }
                for status, metrics in report['order_metrics']['status_metrics'].items()
            ])
            order_df.to_excel(writer, sheet_name='Order Metrics', index=False)
            
            # Financial Metrics
            financial_df = pd.DataFrame([report['financial_metrics']])
            financial_df.to_excel(writer, sheet_name='Financial Metrics', index=False)
            
            # Settlement Metrics
            settlement_df = pd.DataFrame([report['settlement_metrics']])
            settlement_df.to_excel(writer, sheet_name='Settlement Metrics', index=False)
            
            # Return Metrics
            return_df = pd.DataFrame([
                {
                    'Type': rtype,
                    'Count': metrics['count'],
                    'Percentage': metrics['percentage']
                }
                for rtype, metrics in report['return_metrics']['return_types'].items()
            ])
            return_df.to_excel(writer, sheet_name='Return Metrics', index=False)
            
            # Anomalies
            for name, data in report['anomalies'].items():
                if data:  # Only create sheets for non-empty anomalies
                    anomaly_df = pd.DataFrame(data)
                    anomaly_df.to_excel(
                        writer,
                        sheet_name=name.replace('_', ' ').title(),
                        index=False
                    )
        
        return True
        
    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")
        return False

def generate_report(format: str = 'text', output_file: Optional[Path] = None) -> str:
    """
    Generate a report in the specified format.
    
    Args:
        format: Report format ('text', 'json', or 'excel')
        output_file: Optional file path to save the report
    
    Returns:
        Report content (for text/json) or empty string (for excel)
    """
    try:
        # Generate analysis report
        report = generate_analysis_report()
        
        if format == 'text':
            return generate_text_report(report, output_file)
        elif format == 'json':
            return generate_json_report(report, output_file)
        elif format == 'excel':
            if not output_file:
                raise ValueError("output_file is required for Excel format")
            generate_excel_report(report, output_file)
            return ""
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return "" 