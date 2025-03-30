import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, Any, List, Optional
import logging
from pathlib import Path
import sys
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import io
import base64
import json
from sqlalchemy import text
import time
from sqlalchemy.orm import Session
import os

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.database import SessionLocal
from src.reporting import RealTimeReporter
from src.cache import cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / 'streamlit.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Reconciliation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric:hover {
        background-color: #e6e9ef;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #666;
    }
    .metric-value {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-delta {
        font-size: 0.8rem;
        color: #28a745;
    }
    .metric-delta.negative {
        color: #dc3545;
    }
    .section-header {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border-left: 4px solid #1f77b4;
    }
    .download-button {
        background-color: #1f77b4;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 0.5rem;
        border: none;
        cursor: pointer;
        font-weight: bold;
        margin: 1rem 0;
    }
    .download-button:hover {
        background-color: #1664a0;
    }
    </style>
""", unsafe_allow_html=True)

def generate_pdf_report(metrics: Dict[str, Any]) -> bytes:
    """Generate a PDF report with the current metrics."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=30
    )
    elements.append(Paragraph("Reconciliation Dashboard Report", title_style))
    elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
    elements.append(Spacer(1, 20))
    
    # Financial Overview
    elements.append(Paragraph("Financial Overview", styles['Heading2']))
    elements.append(Spacer(1, 10))
    
    financial_data = [
        ["Metric", "Value", "Details"],
        ["Total Sales", f"₹{metrics.get('orders', {}).get('total_order_amount', 0):,.2f}", 
         f"{metrics.get('orders', {}).get('delivered_orders', 0)} Orders"],
        ["Pending Settlements", f"₹{metrics.get('returns', {}).get('pending_settlements', 0):,.2f}",
         f"{metrics.get('settlements', {}).get('pending_settlements', 0)} Orders"],
        ["Net Profit", f"₹{(metrics.get('orders', {}).get('total_order_amount', 0) - metrics.get('returns', {}).get('pending_settlements', 0)):,.2f}",
         f"₹{metrics.get('orders', {}).get('total_order_amount', 0):,.2f} Gross"],
        ["Return Rate", f"{(metrics.get('returns', {}).get('total_returns', 0) / metrics.get('orders', {}).get('total_orders', 1)) * 100:.1f}%",
         f"{metrics.get('returns', {}).get('total_returns', 0)} Returns"]
    ]
    
    financial_table = Table(financial_data, colWidths=[2*inch, 2*inch, 2*inch])
    financial_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(financial_table)
    elements.append(Spacer(1, 20))
    
    # Order Performance
    elements.append(Paragraph("Order Performance", styles['Heading2']))
    elements.append(Spacer(1, 10))
    
    order_data = [
        ["Metric", "Value", "Details"],
        ["Total Orders", str(metrics.get('orders', {}).get('total_orders', 0)),
         f"{metrics.get('orders', {}).get('delivered_orders', 0)} Delivered"],
        ["Returns", str(metrics.get('returns', {}).get('total_returns', 0)),
         f"{metrics.get('returns', {}).get('refund_returns', 0)} Refunds"],
        ["Exchange Rate", str(metrics.get('returns', {}).get('exchange_returns', 0)),
         f"{metrics.get('returns', {}).get('refund_returns', 0)} Refunds"]
    ]
    
    order_table = Table(order_data, colWidths=[2*inch, 2*inch, 2*inch])
    order_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(order_table)
    elements.append(Spacer(1, 20))
    
    # Settlement Status
    elements.append(Paragraph("Settlement Status", styles['Heading2']))
    elements.append(Spacer(1, 10))
    
    settlement_data = [
        ["Metric", "Value", "Details"],
        ["Completed Settlements", str(metrics.get('settlements', {}).get('completed_settlements', 0)),
         f"₹{metrics.get('settlements', {}).get('total_settlement_amount', 0):,.2f}"],
        ["Partial Settlements", str(metrics.get('settlements', {}).get('partial_settlements', 0)),
         f"{metrics.get('settlements', {}).get('pending_settlements', 0)} Pending"],
        ["Pending Settlements", str(metrics.get('settlements', {}).get('pending_settlements', 0)),
         f"₹{metrics.get('returns', {}).get('pending_settlements', 0):,.2f}"]
    ]
    
    settlement_table = Table(settlement_data, colWidths=[2*inch, 2*inch, 2*inch])
    settlement_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(settlement_table)
    
    # Build PDF
    doc.build(elements)
    return buffer.getvalue()

def get_daily_metrics() -> Dict[str, Any]:
    """Get daily metrics from the database."""
    try:
        db = SessionLocal()
        reporter = RealTimeReporter(db)
        return reporter.get_daily_summary()
    except Exception as e:
        logger.error(f"Error fetching daily metrics: {str(e)}")
        return {}
    finally:
        db.close()

def create_financial_metrics(metrics: Dict[str, Any]):
    """Create financial metrics section."""
    st.markdown('<div class="section-header"><h2>Financial Overview</h2></div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_amount = metrics.get("orders", {}).get("total_order_amount", 0)
        st.metric(
            "Total Sales",
            f"₹{total_amount:,.2f}",
            delta=f"{metrics.get('orders', {}).get('delivered_orders', 0)} Orders"
        )
    
    with col2:
        pending_settlements = metrics.get("returns", {}).get("pending_settlements", 0)
        st.metric(
            "Pending Settlements",
            f"₹{pending_settlements:,.2f}",
            delta=f"{metrics.get('settlements', {}).get('pending_settlements', 0)} Orders",
            delta_color="inverse"
        )
    
    with col3:
        total_amount = metrics.get("orders", {}).get("total_order_amount", 0)
        pending_settlements = metrics.get("returns", {}).get("pending_settlements", 0)
        net_profit = total_amount - pending_settlements
        st.metric(
            "Net Profit",
            f"₹{net_profit:,.2f}",
            delta=f"₹{total_amount:,.2f} Gross",
            delta_color="normal"
        )
    
    with col4:
        return_rate = (metrics.get("returns", {}).get("total_returns", 0) / 
                      metrics.get("orders", {}).get("total_orders", 1)) * 100
        st.metric(
            "Return Rate",
            f"{return_rate:.1f}%",
            delta=f"{metrics.get('returns', {}).get('total_returns', 0)} Returns",
            delta_color="inverse"
        )

def create_order_metrics(metrics: Dict[str, Any]):
    """Create order metrics section."""
    st.markdown('<div class="section-header"><h2>Order Performance</h2></div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Total Orders",
            metrics.get("orders", {}).get("total_orders", 0),
            delta=f"{metrics.get('orders', {}).get('delivered_orders', 0)} Delivered"
        )
    
    with col2:
        st.metric(
            "Returns",
            metrics.get("returns", {}).get("total_returns", 0),
            delta=f"{metrics.get('returns', {}).get('refund_returns', 0)} Refunds"
        )
    
    with col3:
        st.metric(
            "Exchange Rate",
            f"{metrics.get('returns', {}).get('exchange_returns', 0)}",
            delta=f"{metrics.get('returns', {}).get('refund_returns', 0)} Refunds"
        )

def create_status_distribution(metrics: Dict[str, Any]):
    """Create status distribution pie chart."""
    st.markdown('<div class="section-header"><h2>Order Status Distribution</h2></div>', unsafe_allow_html=True)
    
    # Prepare data for pie chart
    status_data = {
        "Delivered": metrics.get("orders", {}).get("delivered_orders", 0),
        "Returned": metrics.get("orders", {}).get("returned_orders", 0),
        "Pending": metrics.get("orders", {}).get("total_orders", 0) - 
                 metrics.get("orders", {}).get("delivered_orders", 0) - 
                 metrics.get("orders", {}).get("returned_orders", 0)
    }
    
    fig = px.pie(
        values=list(status_data.values()),
        names=list(status_data.keys()),
        title="Order Status Distribution",
        color_discrete_sequence=['#28a745', '#dc3545', '#ffc107']
    )
    
    fig.update_layout(
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_settlement_status(metrics: Dict[str, Any]):
    """Create settlement status section."""
    st.markdown('<div class="section-header"><h2>Settlement Status</h2></div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Completed Settlements",
            metrics.get("settlements", {}).get("completed_settlements", 0),
            delta=f"₹{metrics.get('settlements', {}).get('total_settlement_amount', 0):,.2f}"
        )
    
    with col2:
        st.metric(
            "Partial Settlements",
            metrics.get("settlements", {}).get("partial_settlements", 0),
            delta=f"{metrics.get('settlements', {}).get('pending_settlements', 0)} Pending"
        )
    
    with col3:
        st.metric(
            "Pending Settlements",
            metrics.get("settlements", {}).get("pending_settlements", 0),
            delta=f"₹{metrics.get('returns', {}).get('pending_settlements', 0):,.2f}",
            delta_color="inverse"
        )

def create_recent_activity(metrics: Dict[str, Any]):
    """Create recent activity timeline."""
    st.markdown('<div class="section-header"><h2>Recent Activity</h2></div>', unsafe_allow_html=True)
    
    # Get recent activity data
    try:
        db = SessionLocal()
        recent_orders = db.execute("""
            SELECT created_on, order_release_id, order_status, final_amount
            FROM orders
            ORDER BY created_on DESC
            LIMIT 5
        """).fetchall()
        
        recent_returns = db.execute("""
            SELECT return_date, order_release_id, return_type, amount_pending_settlement
            FROM returns
            ORDER BY return_date DESC
            LIMIT 5
        """).fetchall()
        
        recent_settlements = db.execute("""
            SELECT created_at, order_release_id, settlement_status, total_actual_settlement
            FROM settlements
            ORDER BY created_at DESC
            LIMIT 5
        """).fetchall()
        
        # Combine and sort activities
        activities = []
        for order in recent_orders:
            activities.append({
                "timestamp": order.created_on,
                "type": "Order",
                "id": order.order_release_id,
                "status": order.order_status,
                "amount": order.final_amount
            })
        
        for ret in recent_returns:
            activities.append({
                "timestamp": ret.return_date,
                "type": "Return",
                "id": ret.order_release_id,
                "status": ret.return_type,
                "amount": ret.amount_pending_settlement
            })
        
        for settlement in recent_settlements:
            activities.append({
                "timestamp": settlement.created_at,
                "type": "Settlement",
                "id": settlement.order_release_id,
                "status": settlement.settlement_status,
                "amount": settlement.total_actual_settlement
            })
        
        activities.sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Display activities with enhanced formatting
        for activity in activities:
            status_color = {
                "Order": "#1f77b4",
                "Return": "#dc3545",
                "Settlement": "#28a745"
            }.get(activity["type"], "#1f77b4")
            
            st.markdown(f"""
                <div style="padding: 1rem; border-left: 4px solid {status_color}; margin: 0.5rem 0; background-color: #f8f9fa; border-radius: 0.5rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong style="color: {status_color};">{activity['type']}</strong> - {activity['id']}<br>
                            Status: {activity['status']}
                        </div>
                        <div style="text-align: right;">
                            <strong>₹{activity['amount']:,.2f}</strong><br>
                            {activity['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
    except Exception as e:
        logger.error(f"Error fetching recent activity: {str(e)}")
        st.error("Failed to load recent activity")
    finally:
        db.close()

def create_performance_metrics():
    """Create performance metrics section."""
    st.subheader("System Performance")
    
    try:
        db = SessionLocal()
        reporter = RealTimeReporter(db)
        metrics = reporter.get_performance_metrics()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Database Statistics")
            st.markdown(f"""
                - Total Orders: {metrics['database']['total_orders']}
                - Unique Orders: {metrics['database']['unique_orders']}
                - Delivered Orders: {metrics['database']['delivered_orders']}
                - Returned Orders: {metrics['database']['returned_orders']}
            """)
        
        with col2:
            st.markdown("### Cache Statistics")
            st.markdown(f"""
                - Cache Keys: {metrics['cache']['keys']}
                - Memory Usage: {metrics['cache']['memory_usage']}
                - Query Execution Time: {metrics['query_performance']['execution_time']:.2f} ms
            """)
            
    except Exception as e:
        logger.error(f"Error fetching performance metrics: {str(e)}")
        st.error("Failed to load performance metrics")
    finally:
        db.close()

def get_orders(
    start_date: datetime = None,
    end_date: datetime = None,
    status: str = None,
    payment_status: str = None,
    search_term: str = None,
    warehouse_id: str = None,
    seller_id: str = None,
    payment_type: str = None,
    amount_range: tuple = None,
    return_status: str = None,
    page: int = 1,
    page_size: int = 10,
    sort_by: str = 'created_on',
    sort_order: str = 'DESC'
) -> tuple:
    """Get orders with filters, pagination, and sorting."""
    try:
        db = SessionLocal()
        
        # Base query
        query = """
            SELECT 
                o.order_release_id,
                o.created_on,
                o.order_status,
                o.payment_status,
                o.final_amount,
                o.warehouse_id,
                o.seller_id,
                o.payment_type,
                o.customer_name,
                o.customer_email,
                o.customer_phone,
                o.shipping_address,
                o.billing_address,
                o.item_name,
                o.item_sku,
                o.item_quantity,
                o.item_price,
                o.shipping_charges,
                o.tax_amount,
                o.discount_amount,
                o.total_amount,
                o.shipping_method,
                o.tracking_number,
                o.delivery_date,
                o.return_status,
                o.return_date,
                o.return_reason,
                o.return_amount,
                o.settlement_status,
                o.settlement_date,
                o.settlement_amount,
                o.settlement_reference,
                o.notes,
                o.updated_on,
                COUNT(*) OVER() as total_count
            FROM orders o
            WHERE 1=1
        """
        params = {}
        
        # Add filters
        if start_date:
            query += " AND o.created_on >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND o.created_on <= :end_date"
            params['end_date'] = end_date
        if status:
            query += " AND o.order_status = :status"
            params['status'] = status
        if payment_status:
            query += " AND o.payment_status = :payment_status"
            params['payment_status'] = payment_status
        if search_term:
            query += """ AND (
                o.order_release_id ILIKE :search_term OR
                o.customer_name ILIKE :search_term OR
                o.item_name ILIKE :search_term
            )"""
            params['search_term'] = f"%{search_term}%"
        if warehouse_id:
            query += " AND o.warehouse_id = :warehouse_id"
            params['warehouse_id'] = warehouse_id
        if seller_id:
            query += " AND o.seller_id = :seller_id"
            params['seller_id'] = seller_id
        if payment_type:
            query += " AND o.payment_type = :payment_type"
            params['payment_type'] = payment_type
        if amount_range:
            query += " AND o.final_amount BETWEEN :min_amount AND :max_amount"
            params['min_amount'] = amount_range[0]
            params['max_amount'] = amount_range[1]
        if return_status:
            query += " AND o.return_status = :return_status"
            params['return_status'] = return_status
        
        # Add sorting
        valid_sort_columns = {
            'created_on': 'o.created_on',
            'order_id': 'o.order_release_id',
            'customer': 'o.customer_name',
            'amount': 'o.final_amount',
            'status': 'o.order_status',
            'payment': 'o.payment_status',
            'return': 'o.return_status'
        }
        
        sort_column = valid_sort_columns.get(sort_by, 'o.created_on')
        sort_direction = 'DESC' if sort_order.upper() == 'DESC' else 'ASC'
        query += f" ORDER BY {sort_column} {sort_direction}"
        
        # Add pagination
        query += " LIMIT :limit OFFSET :offset"
        params['limit'] = page_size
        params['offset'] = (page - 1) * page_size
        
        result = db.execute(text(query), params)
        orders = result.fetchall()
        
        if orders:
            total_count = orders[0].total_count
        else:
            total_count = 0
        
        return orders, total_count
    except Exception as e:
        logger.error(f"Error fetching orders: {str(e)}")
        return [], 0
    finally:
        db.close()

def create_order_details_modal(order: Any):
    """Create a modal with order details."""
    st.markdown("### Order Details")
    
    # Create tabs for different detail views
    tab1, tab2 = st.tabs(["Summary", "Full Details"])
    
    with tab1:
        # Summary view
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Order Information")
            st.write(f"**Order ID:** {order.order_release_id}")
            st.write(f"**Date:** {order.created_on.strftime('%Y-%m-%d %H:%M:%S')}")
            st.write(f"**Status:** {order.order_status}")
            st.write(f"**Payment Status:** {order.payment_status}")
            st.write(f"**Return Status:** {order.return_status}")
            st.write(f"**Settlement Status:** {order.settlement_status}")
        
        with col2:
            st.markdown("#### Customer Information")
            st.write(f"**Name:** {order.customer_name}")
            st.write(f"**Email:** {order.customer_email}")
            st.write(f"**Phone:** {order.customer_phone}")
            st.write(f"**Shipping Address:** {order.shipping_address}")
    
    with tab2:
        # Full details view
        st.markdown("#### Complete Order Information")
        st.json({
            "Order Details": {
                "Order ID": order.order_release_id,
                "Created On": order.created_on.strftime('%Y-%m-%d %H:%M:%S'),
                "Updated On": order.updated_on.strftime('%Y-%m-%d %H:%M:%S') if order.updated_on else None,
                "Status": order.order_status,
                "Payment Status": order.payment_status,
                "Return Status": order.return_status,
                "Settlement Status": order.settlement_status,
                "Notes": order.notes
            },
            "Customer Information": {
                "Name": order.customer_name,
                "Email": order.customer_email,
                "Phone": order.customer_phone,
                "Shipping Address": order.shipping_address,
                "Billing Address": order.billing_address
            },
            "Item Information": {
                "Name": order.item_name,
                "SKU": order.item_sku,
                "Quantity": order.item_quantity,
                "Price": order.item_price,
                "Shipping Charges": order.shipping_charges,
                "Tax Amount": order.tax_amount,
                "Discount Amount": order.discount_amount,
                "Total Amount": order.total_amount,
                "Final Amount": order.final_amount
            },
            "Shipping Information": {
                "Method": order.shipping_method,
                "Tracking Number": order.tracking_number,
                "Delivery Date": order.delivery_date.strftime('%Y-%m-%d') if order.delivery_date else None
            },
            "Return Information": {
                "Return Date": order.return_date.strftime('%Y-%m-%d') if order.return_date else None,
                "Return Reason": order.return_reason,
                "Return Amount": order.return_amount
            },
            "Settlement Information": {
                "Settlement Date": order.settlement_date.strftime('%Y-%m-%d') if order.settlement_date else None,
                "Settlement Amount": order.settlement_amount,
                "Settlement Reference": order.settlement_reference
            }
        })

def export_orders(orders: List[Any], format: str = 'csv'):
    """Export orders to CSV or Excel."""
    if not orders:
        return None
    
    # Convert orders to DataFrame
    df = pd.DataFrame([{
        'Order ID': order.order_release_id,
        'Date': order.created_on,
        'Status': order.order_status,
        'Payment Status': order.payment_status,
        'Return Status': order.return_status,
        'Settlement Status': order.settlement_status,
        'Customer Name': order.customer_name,
        'Customer Email': order.customer_email,
        'Customer Phone': order.customer_phone,
        'Shipping Address': order.shipping_address,
        'Billing Address': order.billing_address,
        'Item Name': order.item_name,
        'Item SKU': order.item_sku,
        'Item Quantity': order.item_quantity,
        'Item Price': order.item_price,
        'Shipping Charges': order.shipping_charges,
        'Tax Amount': order.tax_amount,
        'Discount Amount': order.discount_amount,
        'Total Amount': order.total_amount,
        'Final Amount': order.final_amount,
        'Shipping Method': order.shipping_method,
        'Tracking Number': order.tracking_number,
        'Delivery Date': order.delivery_date,
        'Return Date': order.return_date,
        'Return Reason': order.return_reason,
        'Return Amount': order.return_amount,
        'Settlement Date': order.settlement_date,
        'Settlement Amount': order.settlement_amount,
        'Settlement Reference': order.settlement_reference,
        'Notes': order.notes
    } for order in orders])
    
    if format == 'csv':
        return df.to_csv(index=False).encode('utf-8')
    else:
        return df.to_excel(index=False)

def save_filter_preset(name: str, filters: Dict[str, Any]):
    """Save filter preset to session state."""
    if 'filter_presets' not in st.session_state:
        st.session_state.filter_presets = {}
    st.session_state.filter_presets[name] = filters

def load_filter_preset(name: str) -> Dict[str, Any]:
    """Load filter preset from session state."""
    if 'filter_presets' in st.session_state and name in st.session_state.filter_presets:
        return st.session_state.filter_presets[name]
    return {}

def orders_management_tab():
    """Orders Management tab implementation."""
    st.title("Orders Management")
    
    # Initialize session state for filters and pagination
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'sort_by' not in st.session_state:
        st.session_state.sort_by = 'created_on'
    if 'sort_order' not in st.session_state:
        st.session_state.sort_order = 'DESC'
    if 'selected_orders' not in st.session_state:
        st.session_state.selected_orders = set()
    
    # Search and Filter Section
    st.markdown("### Search & Filter Orders")
    
    # Filter Presets
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Quick Search
        search_term = st.text_input("🔍 Quick Search (Order ID, Customer Name, Item Name)")
        
        # Essential Filters
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            date_range = st.selectbox(
                "Date Range",
                ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
                index=1
            )
            
            if date_range == "Custom":
                custom_col1, custom_col2 = st.columns(2)
                with custom_col1:
                    start_date = st.date_input("Start Date")
                with custom_col2:
                    end_date = st.date_input("End Date")
            else:
                days = int(date_range.split()[1])
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
        
        with filter_col2:
            status = st.selectbox(
                "Order Status",
                ["All", "Delivered", "Returned", "Pending", "Cancelled"],
                index=0
            )
            status = None if status == "All" else status
        
        with filter_col3:
            payment_status = st.selectbox(
                "Payment Status",
                ["All", "Paid", "Pending", "Failed"],
                index=0
            )
            payment_status = None if payment_status == "All" else payment_status
    
    with col2:
        # Save/Load Filter Presets
        st.markdown("### Filter Presets")
        preset_name = st.text_input("Preset Name")
        
        col2a, col2b = st.columns(2)
        with col2a:
            if st.button("Save Preset"):
                if preset_name:
                    filters = {
                        'date_range': date_range,
                        'start_date': start_date if date_range == "Custom" else None,
                        'end_date': end_date if date_range == "Custom" else None,
                        'status': status,
                        'payment_status': payment_status,
                        'search_term': search_term
                    }
                    save_filter_preset(preset_name, filters)
                    st.success(f"Preset '{preset_name}' saved!")
        
        with col2b:
            if st.button("Load Preset"):
                if preset_name and preset_name in st.session_state.get('filter_presets', {}):
                    filters = load_filter_preset(preset_name)
                    st.session_state.current_page = 1  # Reset to first page
                    st.experimental_rerun()
    
    # Advanced Filters (Collapsible)
    with st.expander("Advanced Filters"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            warehouse_id = st.text_input("Warehouse ID")
            seller_id = st.text_input("Seller ID")
        
        with col2:
            payment_type = st.selectbox(
                "Payment Type",
                ["All", "COD", "Online", "Wallet"],
                index=0
            )
            payment_type = None if payment_type == "All" else payment_type
        
        with col3:
            amount_range = st.slider(
                "Order Amount Range",
                min_value=0.0,
                max_value=100000.0,
                value=(0.0, 100000.0),
                step=1000.0
            )
    
    # Sorting Options
    sort_col1, sort_col2 = st.columns(2)
    
    with sort_col1:
        sort_by = st.selectbox(
            "Sort By",
            ["Date", "Order ID", "Customer", "Amount", "Status", "Payment", "Return"],
            index=0
        )
        sort_by_map = {
            "Date": "created_on",
            "Order ID": "order_id",
            "Customer": "customer",
            "Amount": "amount",
            "Status": "status",
            "Payment": "payment",
            "Return": "return"
        }
    
    with sort_col2:
        sort_order = st.selectbox(
            "Sort Order",
            ["Descending", "Ascending"],
            index=0
        )
    
    # Get orders with filters
    with st.spinner("Loading orders..."):
        orders, total_count = get_orders(
            start_date=start_date if date_range == "Custom" else None,
            end_date=end_date if date_range == "Custom" else None,
            status=status,
            payment_status=payment_status,
            search_term=search_term,
            warehouse_id=warehouse_id if warehouse_id else None,
            seller_id=seller_id if seller_id else None,
            payment_type=payment_type,
            amount_range=amount_range,
            page=st.session_state.current_page,
            page_size=10,
            sort_by=sort_by_map[sort_by],
            sort_order=sort_order[:3].upper()
        )
    
    # Display results count
    st.markdown(f"Found {total_count} orders")
    
    # Bulk Actions
    if orders:
        st.markdown("### Bulk Actions")
        bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
        
        with bulk_col1:
            if st.button("Export Selected"):
                selected_orders = [order for i, order in enumerate(orders) if i in st.session_state.selected_orders]
                if selected_orders:
                    data = export_orders(selected_orders, format='csv')
                    if data is not None:
                        st.download_button(
                            label="Download Selected Orders",
                            data=data,
                            file_name=f"selected_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
        
        with bulk_col2:
            if st.button("Generate Report for Selected"):
                selected_orders = [order for i, order in enumerate(orders) if i in st.session_state.selected_orders]
                if selected_orders:
                    pdf_bytes = generate_pdf_report({"orders": selected_orders})
                    b64 = base64.b64encode(pdf_bytes).decode()
                    href = f'<a href="data:application/pdf;base64,{b64}" download="selected_orders_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf">Click here to download the report</a>'
                    st.markdown(href, unsafe_allow_html=True)
    
    # Orders Table
    st.markdown("### Orders")
    
    if orders:
        # Create DataFrame with visible columns
        df = pd.DataFrame([{
            'Order ID': order.order_release_id,
            'Date': order.created_on.strftime('%Y-%m-%d %H:%M:%S'),
            'Customer': order.customer_name,
            'Amount': f"₹{order.final_amount:,.2f}",
            'Status': order.order_status,
            'Payment': order.payment_status,
            'Return': order.return_status
        } for order in orders])
        
        # Add checkboxes for selection
        df['Select'] = [f'<input type="checkbox" id="order_{i}" {"checked" if i in st.session_state.selected_orders else ""}>' for i in range(len(orders))]
        
        # Display table with checkboxes
        st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
        
        # Handle checkbox selections
        for i in range(len(orders)):
            if st.checkbox(f"Select Order {orders[i].order_release_id}", key=f"order_{i}"):
                st.session_state.selected_orders.add(i)
            else:
                st.session_state.selected_orders.discard(i)
        
        # Pagination
        total_pages = (total_count + 9) // 10
        st.markdown("### Pagination")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.button("Previous Page") and st.session_state.current_page > 1:
                st.session_state.current_page -= 1
                st.experimental_rerun()
        
        with col2:
            st.markdown(f"Page {st.session_state.current_page} of {total_pages}")
        
        with col3:
            if st.button("Next Page") and st.session_state.current_page < total_pages:
                st.session_state.current_page += 1
                st.experimental_rerun()
        
        # Display order details
        for index, row in df.iterrows():
            with st.expander(f"Order {row['Order ID']} - {row['Customer']}"):
                create_order_details_modal(orders[index])
    else:
        st.info("No orders found matching the selected filters.")

def export_returns(returns: List[Any], format: str = 'csv') -> Optional[str]:
    """Export returns data to CSV or Excel format."""
    try:
        # Create DataFrame with all fields
        df = pd.DataFrame([{
            'Return ID': return_data.return_id,
            'Order ID': return_data.order_release_id,
            'Created On': return_data.created_on.strftime('%Y-%m-%d %H:%M:%S'),
            'Updated On': return_data.updated_on.strftime('%Y-%m-%d %H:%M:%S'),
            'Return Type': return_data.return_type,
            'Return Status': return_data.return_status,
            'Return Reason': return_data.return_reason,
            'Return Amount': return_data.return_amount,
            'Settlement Status': return_data.settlement_status,
            'Settlement Date': return_data.settlement_date.strftime('%Y-%m-%d %H:%M:%S') if return_data.settlement_date else None,
            'Settlement Amount': return_data.settlement_amount,
            'Settlement Reference': return_data.settlement_reference,
            'Notes': return_data.notes,
            'Customer Name': return_data.customer_name,
            'Customer Email': return_data.customer_email,
            'Customer Phone': return_data.customer_phone,
            'Shipping Address': return_data.shipping_address,
            'Billing Address': return_data.billing_address,
            'Item Name': return_data.item_name,
            'Item SKU': return_data.item_sku,
            'Item Quantity': return_data.item_quantity,
            'Item Price': return_data.item_price,
            'Shipping Charges': return_data.shipping_charges,
            'Tax Amount': return_data.tax_amount,
            'Discount Amount': return_data.discount_amount,
            'Total Amount': return_data.total_amount,
            'Shipping Method': return_data.shipping_method,
            'Tracking Number': return_data.tracking_number,
            'Delivery Date': return_data.delivery_date.strftime('%Y-%m-%d %H:%M:%S') if return_data.delivery_date else None,
            'Order Status': return_data.order_status,
            'Payment Status': return_data.payment_status,
            'Payment Type': return_data.payment_type,
            'Warehouse ID': return_data.warehouse_id,
            'Seller ID': return_data.seller_id
        } for return_data in returns])
        
        if format.lower() == 'excel':
            # Create Excel writer
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Returns', index=False)
                
                # Get workbook and worksheet objects
                workbook = writer.book
                worksheet = writer.sheets['Returns']
                
                # Add formats
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#4CAF50',
                    'font_color': 'white',
                    'border': 1
                })
                
                # Format headers
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Auto-adjust column widths
                for idx, col in enumerate(df):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    )
                    worksheet.set_column(idx, idx, max_length + 2)
            
            return output.getvalue()
        else:
            # CSV format
            return df.to_csv(index=False)
    except Exception as e:
        logger.error(f"Error exporting returns: {str(e)}")
        st.error(f"Error exporting returns: {str(e)}")
        return None

def get_returns_metrics() -> Dict[str, Any]:
    """Get returns metrics for overview."""
    try:
        db = SessionLocal()
        
        # Total returns
        total_query = "SELECT COUNT(*) as count FROM returns"
        total_result = db.execute(text(total_query))
        total_returns = total_result.scalar()
        
        # Return types distribution
        type_query = """
            SELECT return_type, COUNT(*) as count
            FROM returns
            GROUP BY return_type
        """
        type_result = db.execute(text(type_query))
        type_distribution = {row.return_type: row.count for row in type_result}
        
        # Return reasons
        reason_query = """
            SELECT return_reason, COUNT(*) as count
            FROM returns
            GROUP BY return_reason
            ORDER BY count DESC
            LIMIT 5
        """
        reason_result = db.execute(text(reason_query))
        top_reasons = {row.return_reason: row.count for row in reason_result}
        
        # Settlement status
        settlement_query = """
            SELECT settlement_status, COUNT(*) as count
            FROM returns
            GROUP BY settlement_status
        """
        settlement_result = db.execute(text(settlement_query))
        settlement_status = {row.settlement_status: row.count for row in settlement_result}
        
        # Average return amount
        amount_query = "SELECT AVG(return_amount) as avg_amount FROM returns"
        amount_result = db.execute(text(amount_query))
        avg_amount = amount_result.scalar() or 0
        
        # Return processing time metrics
        processing_time_query = """
            SELECT 
                AVG(EXTRACT(EPOCH FROM (r.created_on - o.delivery_date))) as avg_processing_time,
                AVG(EXTRACT(EPOCH FROM (r.settlement_date - r.created_on))) as avg_settlement_time
            FROM returns r
            JOIN orders o ON r.order_release_id = o.order_release_id
            WHERE o.delivery_date IS NOT NULL
        """
        processing_time_result = db.execute(text(processing_time_query))
        processing_times = processing_time_result.fetchone()
        
        # Return rate by product category
        category_query = """
            SELECT 
                o.item_name,
                COUNT(*) as total_orders,
                COUNT(r.return_id) as returns,
                ROUND(COUNT(r.return_id)::float / COUNT(*) * 100, 2) as return_rate
            FROM orders o
            LEFT JOIN returns r ON o.order_release_id = r.order_release_id
            GROUP BY o.item_name
            HAVING COUNT(*) > 10
            ORDER BY return_rate DESC
            LIMIT 5
        """
        category_result = db.execute(text(category_query))
        category_rates = {
            row.item_name: {
                'total_orders': row.total_orders,
                'returns': row.returns,
                'return_rate': row.return_rate
            } for row in category_result
        }
        
        return {
            'total_returns': total_returns,
            'type_distribution': type_distribution,
            'top_reasons': top_reasons,
            'settlement_status': settlement_status,
            'avg_amount': avg_amount,
            'processing_times': {
                'avg_processing_time': processing_times.avg_processing_time if processing_times else 0,
                'avg_settlement_time': processing_times.avg_settlement_time if processing_times else 0
            },
            'category_rates': category_rates
        }
    except Exception as e:
        logger.error(f"Error fetching returns metrics: {str(e)}")
        return {
            'total_returns': 0,
            'type_distribution': {},
            'top_reasons': {},
            'settlement_status': {},
            'avg_amount': 0,
            'processing_times': {'avg_processing_time': 0, 'avg_settlement_time': 0},
            'category_rates': {}
        }
    finally:
        db.close()

def create_return_details_modal(return_data: Any):
    """Create a modal with return details."""
    # Summary Tab
    summary_col1, summary_col2 = st.columns(2)
    
    with summary_col1:
        st.markdown("#### Basic Information")
        st.markdown(f"""
            - **Return ID:** {return_data.return_id}
            - **Order ID:** {return_data.order_release_id}
            - **Customer:** {return_data.customer_name}
            - **Type:** {return_data.return_type}
            - **Status:** {return_data.return_status}
            - **Reason:** {return_data.return_reason}
            - **Amount:** ₹{return_data.return_amount:,.2f}
        """)
    
    with summary_col2:
        st.markdown("#### Dates & Settlement")
        st.markdown(f"""
            - **Created:** {return_data.created_on.strftime('%Y-%m-%d %H:%M:%S')}
            - **Updated:** {return_data.updated_on.strftime('%Y-%m-%d %H:%M:%S')}
            - **Settlement Status:** {return_data.settlement_status}
            - **Settlement Date:** {return_data.settlement_date.strftime('%Y-%m-%d %H:%M:%S') if return_data.settlement_date else 'N/A'}
            - **Settlement Amount:** ₹{return_data.settlement_amount:,.2f if return_data.settlement_amount else 0}
        """)
    
    # Full Details Tab
    st.markdown("#### Order Details")
    st.markdown(f"""
        - **Item:** {return_data.item_name} (SKU: {return_data.item_sku})
        - **Quantity:** {return_data.item_quantity}
        - **Price:** ₹{return_data.item_price:,.2f}
        - **Shipping:** ₹{return_data.shipping_charges:,.2f}
        - **Tax:** ₹{return_data.tax_amount:,.2f}
        - **Discount:** ₹{return_data.discount_amount:,.2f}
        - **Total:** ₹{return_data.total_amount:,.2f}
        - **Shipping Method:** {return_data.shipping_method}
        - **Tracking:** {return_data.tracking_number}
        - **Delivery Date:** {return_data.delivery_date.strftime('%Y-%m-%d %H:%M:%S') if return_data.delivery_date else 'N/A'}
    """)
    
    st.markdown("#### Customer Information")
    st.markdown(f"""
        - **Email:** {return_data.customer_email}
        - **Phone:** {return_data.customer_phone}
        - **Shipping Address:** {return_data.shipping_address}
        - **Billing Address:** {return_data.billing_address}
    """)
    
    if return_data.notes:
        st.markdown("#### Notes")
        st.markdown(return_data.notes)

def returns_analysis_tab():
    """Returns Analysis tab implementation."""
    st.title("Returns Analysis")
    
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'sort_by' not in st.session_state:
        st.session_state.sort_by = 'created_on'
    if 'sort_order' not in st.session_state:
        st.session_state.sort_order = 'DESC'
    if 'selected_returns' not in st.session_state:
        st.session_state.selected_returns = set()
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    
    # Add refresh controls
    refresh_col1, refresh_col2 = st.columns([1, 3])
    with refresh_col1:
        if st.button("🔄 Refresh Data"):
            st.session_state.current_page = 1
            st.experimental_rerun()
    with refresh_col2:
        st.session_state.auto_refresh = st.checkbox("Auto-refresh every 5 minutes", value=st.session_state.auto_refresh)
    
    # Returns Overview Section
    st.markdown("### Returns Overview")
    
    # Get metrics with progress bar
    with st.spinner("Loading metrics..."):
        progress_bar = st.progress(0)
        metrics = get_returns_metrics()
        progress_bar.progress(100)
    
    # Key Metrics Cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Returns",
            f"{metrics['total_returns']:,}",
            help="Total number of returns"
        )
    
    with col2:
        st.metric(
            "Average Return Amount",
            f"₹{metrics['avg_amount']:,.2f}",
            help="Average value of returns"
        )
    
    with col3:
        pending = metrics['settlement_status'].get('Pending', 0)
        st.metric(
            "Pending Returns",
            f"{pending:,}",
            help="Returns pending settlement"
        )
    
    with col4:
        total_orders = metrics['total_returns']  # This should be replaced with actual total orders
        return_rate = (metrics['total_returns'] / total_orders * 100) if total_orders > 0 else 0
        st.metric(
            "Return Rate",
            f"{return_rate:.1f}%",
            help="Percentage of orders returned"
        )
    
    # Processing Time Metrics
    st.markdown("#### Processing Time Metrics")
    time_col1, time_col2 = st.columns(2)
    
    with time_col1:
        avg_processing_time = metrics['processing_times']['avg_processing_time']
        st.metric(
            "Average Processing Time",
            f"{avg_processing_time/86400:.1f} days",
            help="Average time between delivery and return initiation"
        )
    
    with time_col2:
        avg_settlement_time = metrics['processing_times']['avg_settlement_time']
        st.metric(
            "Average Settlement Time",
            f"{avg_settlement_time/86400:.1f} days",
            help="Average time for return settlement"
        )
    
    # Charts Row 1
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("#### Return Type Distribution")
        type_fig = px.pie(
            values=list(metrics['type_distribution'].values()),
            names=list(metrics['type_distribution'].keys()),
            title="Return Types"
        )
        st.plotly_chart(type_fig, use_container_width=True)
    
    with chart_col2:
        st.markdown("#### Settlement Status Distribution")
        settlement_fig = px.pie(
            values=list(metrics['settlement_status'].values()),
            names=list(metrics['settlement_status'].keys()),
            title="Settlement Status"
        )
        st.plotly_chart(settlement_fig, use_container_width=True)
    
    # Charts Row 2
    chart_col3, chart_col4 = st.columns(2)
    
    with chart_col3:
        st.markdown("#### Top Return Reasons")
        reason_fig = px.bar(
            x=list(metrics['top_reasons'].keys()),
            y=list(metrics['top_reasons'].values()),
            title="Most Common Return Reasons"
        )
        st.plotly_chart(reason_fig, use_container_width=True)
    
    with chart_col4:
        st.markdown("#### Return Rate by Product Category")
        category_data = metrics['category_rates']
        category_df = pd.DataFrame([
            {
                'Product': item,
                'Return Rate (%)': data['return_rate'],
                'Total Orders': data['total_orders'],
                'Returns': data['returns']
            }
            for item, data in category_data.items()
        ])
        category_fig = px.bar(
            category_df,
            x='Product',
            y='Return Rate (%)',
            title="Return Rate by Product Category",
            hover_data=['Total Orders', 'Returns']
        )
        st.plotly_chart(category_fig, use_container_width=True)
    
    # Search and Filter Section
    st.markdown("### Search & Filter Returns")
    
    # Filter Presets
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Quick Search
        search_term = st.text_input("🔍 Quick Search (Return ID, Order ID, Customer Name)")
        
        # Essential Filters
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            date_range = st.selectbox(
                "Date Range",
                ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
                index=1
            )
            
            if date_range == "Custom":
                custom_col1, custom_col2 = st.columns(2)
                with custom_col1:
                    start_date = st.date_input("Start Date")
                with custom_col2:
                    end_date = st.date_input("End Date")
            else:
                days = int(date_range.split()[1])
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
        
        with filter_col2:
            return_type = st.selectbox(
                "Return Type",
                ["All", "Refund", "Exchange", "Partial Return"],
                index=0
            )
            return_type = None if return_type == "All" else return_type
        
        with filter_col3:
            return_status = st.selectbox(
                "Return Status",
                ["All", "Pending", "Processing", "Completed", "Cancelled"],
                index=0
            )
            return_status = None if return_status == "All" else return_status
    
    with col2:
        # Save/Load Filter Presets
        st.markdown("### Filter Presets")
        preset_name = st.text_input("Preset Name")
        
        col2a, col2b = st.columns(2)
        with col2a:
            if st.button("Save Preset"):
                if preset_name:
                    filters = {
                        'date_range': date_range,
                        'start_date': start_date if date_range == "Custom" else None,
                        'end_date': end_date if date_range == "Custom" else None,
                        'return_type': return_type,
                        'return_status': return_status,
                        'search_term': search_term
                    }
                    save_filter_preset(preset_name, filters)
                    st.success(f"Preset '{preset_name}' saved!")
        
        with col2b:
            if st.button("Load Preset"):
                if preset_name and preset_name in st.session_state.get('filter_presets', {}):
                    filters = load_filter_preset(preset_name)
                    st.session_state.current_page = 1  # Reset to first page
                    st.experimental_rerun()
    
    # Advanced Filters (Collapsible)
    with st.expander("Advanced Filters"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            warehouse_id = st.text_input("Warehouse ID")
            seller_id = st.text_input("Seller ID")
        
        with col2:
            settlement_status = st.selectbox(
                "Settlement Status",
                ["All", "Pending", "Partial", "Completed"],
                index=0
            )
            settlement_status = None if settlement_status == "All" else settlement_status
        
        with col3:
            amount_range = st.slider(
                "Return Amount Range",
                min_value=0.0,
                max_value=100000.0,
                value=(0.0, 100000.0),
                step=1000.0
            )
    
    # Sorting Options
    sort_col1, sort_col2 = st.columns(2)
    
    with sort_col1:
        sort_by = st.selectbox(
            "Sort By",
            ["Date", "Return ID", "Order ID", "Customer", "Amount", "Type", "Status", "Settlement"],
            index=0
        )
        sort_by_map = {
            "Date": "created_on",
            "Return ID": "return_id",
            "Order ID": "order_id",
            "Customer": "customer",
            "Amount": "amount",
            "Type": "type",
            "Status": "status",
            "Settlement": "settlement"
        }
    
    with sort_col2:
        sort_order = st.selectbox(
            "Sort Order",
            ["Descending", "Ascending"],
            index=0
        )
    
    # Get returns with filters
    with st.spinner("Loading returns..."):
        returns, total_count = get_returns(
            start_date=start_date if date_range == "Custom" else None,
            end_date=end_date if date_range == "Custom" else None,
            return_type=return_type,
            return_status=return_status,
            settlement_status=settlement_status,
            search_term=search_term,
            warehouse_id=warehouse_id if warehouse_id else None,
            seller_id=seller_id if seller_id else None,
            amount_range=amount_range,
            page=st.session_state.current_page,
            page_size=10,
            sort_by=sort_by_map[sort_by],
            sort_order=sort_order[:3].upper()
        )
    
    # Display results count
    st.markdown(f"Found {total_count} returns")
    
    # Bulk Actions
    if returns:
        st.markdown("### Bulk Actions")
        bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
        
        with bulk_col1:
            if st.button("Export Selected"):
                selected_returns = [return_data for i, return_data in enumerate(returns) if i in st.session_state.selected_returns]
                if selected_returns:
                    data = export_returns(selected_returns, format='csv')
                    if data is not None:
                        st.download_button(
                            label="Download Selected Returns",
                            data=data,
                            file_name=f"selected_returns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
        
        with bulk_col2:
            if st.button("Generate Report for Selected"):
                selected_returns = [return_data for i, return_data in enumerate(returns) if i in st.session_state.selected_returns]
                if selected_returns:
                    pdf_bytes = generate_pdf_report({"returns": selected_returns})
                    b64 = base64.b64encode(pdf_bytes).decode()
                    href = f'<a href="data:application/pdf;base64,{b64}" download="selected_returns_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf">Click here to download the report</a>'
                    st.markdown(href, unsafe_allow_html=True)
    
    # Returns Table
    st.markdown("### Returns")
    
    if returns:
        # Create DataFrame with visible columns
        df = pd.DataFrame([{
            'Return ID': return_data.return_id,
            'Order ID': return_data.order_release_id,
            'Date': return_data.created_on.strftime('%Y-%m-%d %H:%M:%S'),
            'Customer': return_data.customer_name,
            'Amount': f"₹{return_data.return_amount:,.2f}",
            'Type': return_data.return_type,
            'Status': return_data.return_status,
            'Settlement': return_data.settlement_status
        } for return_data in returns])
        
        # Add checkboxes for selection
        df['Select'] = [f'<input type="checkbox" id="return_{i}" {"checked" if i in st.session_state.selected_returns else ""}>' for i in range(len(returns))]
        
        # Display table with checkboxes
        st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
        
        # Handle checkbox selections
        for i in range(len(returns)):
            if st.checkbox(f"Select Return {returns[i].return_id}", key=f"return_{i}"):
                st.session_state.selected_returns.add(i)
            else:
                st.session_state.selected_returns.discard(i)
        
        # Pagination
        total_pages = (total_count + 9) // 10
        st.markdown("### Pagination")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.button("Previous Page") and st.session_state.current_page > 1:
                st.session_state.current_page -= 1
                st.experimental_rerun()
        
        with col2:
            st.markdown(f"Page {st.session_state.current_page} of {total_pages}")
        
        with col3:
            if st.button("Next Page") and st.session_state.current_page < total_pages:
                st.session_state.current_page += 1
                st.experimental_rerun()
        
        # Display return details
        for index, row in df.iterrows():
            with st.expander(f"Return {row['Return ID']} - {row['Customer']}"):
                create_return_details_modal(returns[index])
    else:
        st.info("No returns found matching the selected filters.")
    
    # Export Options Note
    st.markdown("""
        ---
        **Note:** Detailed information including full customer details, item information, shipping details, 
        processing timeline, and all associated order data is available in the export options above.
    """)

def get_settlements(
    start_date: datetime = None,
    end_date: datetime = None,
    settlement_status: str = None,
    payment_type: str = None,
    search_term: str = None,
    warehouse_id: str = None,
    seller_id: str = None,
    amount_range: tuple = None,
    commission_range: tuple = None,
    logistics_range: tuple = None,
    page: int = 1,
    page_size: int = 10,
    sort_by: str = 'created_at',
    sort_order: str = 'DESC'
) -> tuple:
    """Get settlements with filters, pagination, and sorting."""
    try:
        db = SessionLocal()
        
        # Base query
        query = """
            SELECT 
                s.id,
                s.order_release_id,
                s.order_line_id,
                s.total_expected_settlement,
                s.total_actual_settlement,
                s.amount_pending_settlement,
                s.settlement_status,
                s.created_at,
                s.updated_at,
                s.prepaid_commission_deduction,
                s.prepaid_logistics_deduction,
                s.prepaid_payment,
                s.postpaid_commission_deduction,
                s.postpaid_logistics_deduction,
                s.postpaid_payment,
                o.customer_name,
                o.customer_email,
                o.customer_phone,
                o.shipping_address,
                o.billing_address,
                o.item_name,
                o.item_sku,
                o.item_quantity,
                o.item_price,
                o.shipping_charges,
                o.tax_amount,
                o.discount_amount,
                o.total_amount,
                o.shipping_method,
                o.tracking_number,
                o.delivery_date,
                o.order_status,
                o.payment_status,
                o.payment_type,
                o.warehouse_id,
                o.seller_id,
                r.return_type,
                r.return_date,
                r.return_reason,
                COUNT(*) OVER() as total_count
            FROM settlements s
            JOIN orders o ON s.order_release_id = o.order_release_id
            LEFT JOIN returns r ON s.order_release_id = r.order_release_id
            WHERE 1=1
        """
        params = {}
        
        # Add filters
        if start_date:
            query += " AND s.created_at >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND s.created_at <= :end_date"
            params['end_date'] = end_date
        if settlement_status:
            query += " AND s.settlement_status = :settlement_status"
            params['settlement_status'] = settlement_status
        if payment_type:
            query += " AND o.payment_type = :payment_type"
            params['payment_type'] = payment_type
        if search_term:
            query += """ AND (
                s.order_release_id ILIKE :search_term OR
                o.customer_name ILIKE :search_term OR
                o.item_name ILIKE :search_term
            )"""
            params['search_term'] = f"%{search_term}%"
        if warehouse_id:
            query += " AND o.warehouse_id = :warehouse_id"
            params['warehouse_id'] = warehouse_id
        if seller_id:
            query += " AND o.seller_id = :seller_id"
            params['seller_id'] = seller_id
        if amount_range:
            query += " AND s.total_expected_settlement BETWEEN :min_amount AND :max_amount"
            params['min_amount'] = amount_range[0]
            params['max_amount'] = amount_range[1]
        if commission_range:
            query += """ AND (
                s.prepaid_commission_deduction BETWEEN :min_commission AND :max_commission OR
                s.postpaid_commission_deduction BETWEEN :min_commission AND :max_commission
            )"""
            params['min_commission'] = commission_range[0]
            params['max_commission'] = commission_range[1]
        if logistics_range:
            query += """ AND (
                s.prepaid_logistics_deduction BETWEEN :min_logistics AND :max_logistics OR
                s.postpaid_logistics_deduction BETWEEN :min_logistics AND :max_logistics
            )"""
            params['min_logistics'] = logistics_range[0]
            params['max_logistics'] = logistics_range[1]
        
        # Add sorting
        valid_sort_columns = {
            'created_at': 's.created_at',
            'order_id': 's.order_release_id',
            'customer': 'o.customer_name',
            'amount': 's.total_expected_settlement',
            'status': 's.settlement_status',
            'payment_type': 'o.payment_type'
        }
        
        sort_column = valid_sort_columns.get(sort_by, 's.created_at')
        sort_direction = 'DESC' if sort_order.upper() == 'DESC' else 'ASC'
        query += f" ORDER BY {sort_column} {sort_direction}"
        
        # Add pagination
        query += " LIMIT :limit OFFSET :offset"
        params['limit'] = page_size
        params['offset'] = (page - 1) * page_size
        
        result = db.execute(text(query), params)
        settlements = result.fetchall()
        
        if settlements:
            total_count = settlements[0].total_count
        else:
            total_count = 0
        
        return settlements, total_count
    except Exception as e:
        logger.error(f"Error fetching settlements: {str(e)}")
        return [], 0
    finally:
        db.close()

def get_settlement_metrics() -> Dict[str, Any]:
    """Get settlement metrics for overview."""
    try:
        db = SessionLocal()
        
        # Total settlements amount
        total_query = """
            SELECT 
                SUM(total_expected_settlement) as total_amount,
                SUM(total_actual_settlement) as actual_amount,
                SUM(amount_pending_settlement) as pending_amount
            FROM settlements
        """
        total_result = db.execute(text(total_query))
        total_amounts = total_result.fetchone()
        
        # Settlement status distribution
        status_query = """
            SELECT settlement_status, COUNT(*) as count
            FROM settlements
            GROUP BY settlement_status
        """
        status_result = db.execute(text(status_query))
        status_distribution = {row.settlement_status: row.count for row in status_result}
        
        # Settlement completion rate
        completion_query = """
            SELECT 
                COUNT(*) FILTER (WHERE settlement_status = 'completed') as completed,
                COUNT(*) as total
            FROM settlements
        """
        completion_result = db.execute(text(completion_query))
        completion_data = completion_result.fetchone()
        completion_rate = (completion_data.completed / completion_data.total * 100) if completion_data.total > 0 else 0
        
        # Average settlement time
        time_query = """
            SELECT 
                AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_time
            FROM settlements
            WHERE settlement_status = 'completed'
        """
        time_result = db.execute(text(time_query))
        avg_time = time_result.scalar() or 0
        
        # Settlement trends
        trend_query = """
            SELECT 
                DATE_TRUNC('day', created_at) as date,
                COUNT(*) as count,
                SUM(total_expected_settlement) as expected_amount,
                SUM(total_actual_settlement) as actual_amount
            FROM settlements
            WHERE created_at >= NOW() - INTERVAL '30 days'
            GROUP BY DATE_TRUNC('day', created_at)
            ORDER BY date
        """
        trend_result = db.execute(text(trend_query))
        trends = {
            'dates': [row.date for row in trend_result],
            'counts': [row.count for row in trend_result],
            'expected_amounts': [row.expected_amount for row in trend_result],
            'actual_amounts': [row.actual_amount for row in trend_result]
        }
        
        # Amount distribution
        distribution_query = """
            SELECT 
                CASE 
                    WHEN total_expected_settlement < 1000 THEN '0-1K'
                    WHEN total_expected_settlement < 5000 THEN '1K-5K'
                    WHEN total_expected_settlement < 10000 THEN '5K-10K'
                    ELSE '10K+'
                END as range,
                COUNT(*) as count
            FROM settlements
            GROUP BY 
                CASE 
                    WHEN total_expected_settlement < 1000 THEN '0-1K'
                    WHEN total_expected_settlement < 5000 THEN '1K-5K'
                    WHEN total_expected_settlement < 10000 THEN '5K-10K'
                    ELSE '10K+'
                END
            ORDER BY 
                CASE range
                    WHEN '0-1K' THEN 1
                    WHEN '1K-5K' THEN 2
                    WHEN '5K-10K' THEN 3
                    ELSE 4
                END
        """
        distribution_result = db.execute(text(distribution_query))
        amount_distribution = {row.range: row.count for row in distribution_result}
        
        return {
            'total_amounts': {
                'expected': total_amounts.total_amount or 0,
                'actual': total_amounts.actual_amount or 0,
                'pending': total_amounts.pending_amount or 0
            },
            'status_distribution': status_distribution,
            'completion_rate': completion_rate,
            'avg_time': avg_time,
            'trends': trends,
            'amount_distribution': amount_distribution
        }
    except Exception as e:
        logger.error(f"Error fetching settlement metrics: {str(e)}")
        return {
            'total_amounts': {'expected': 0, 'actual': 0, 'pending': 0},
            'status_distribution': {},
            'completion_rate': 0,
            'avg_time': 0,
            'trends': {'dates': [], 'counts': [], 'expected_amounts': [], 'actual_amounts': []},
            'amount_distribution': {}
        }
    finally:
        db.close()

def export_settlements(settlements: List[Any], format: str = 'csv') -> Optional[str]:
    """Export settlements data to CSV or Excel format."""
    try:
        # Create DataFrame with all fields
        df = pd.DataFrame([{
            'Settlement ID': settlement.id,
            'Order ID': settlement.order_release_id,
            'Order Line ID': settlement.order_line_id,
            'Expected Amount': settlement.total_expected_settlement,
            'Actual Amount': settlement.total_actual_settlement,
            'Pending Amount': settlement.amount_pending_settlement,
            'Status': settlement.settlement_status,
            'Created On': settlement.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'Updated On': settlement.updated_at.strftime('%Y-%m-%d %H:%M:%S'),
            'Prepaid Commission': settlement.prepaid_commission_deduction,
            'Prepaid Logistics': settlement.prepaid_logistics_deduction,
            'Prepaid Payment': settlement.prepaid_payment,
            'Postpaid Commission': settlement.postpaid_commission_deduction,
            'Postpaid Logistics': settlement.postpaid_logistics_deduction,
            'Postpaid Payment': settlement.postpaid_payment,
            'Customer Name': settlement.customer_name,
            'Customer Email': settlement.customer_email,
            'Customer Phone': settlement.customer_phone,
            'Shipping Address': settlement.shipping_address,
            'Billing Address': settlement.billing_address,
            'Item Name': settlement.item_name,
            'Item SKU': settlement.item_sku,
            'Item Quantity': settlement.item_quantity,
            'Item Price': settlement.item_price,
            'Shipping Charges': settlement.shipping_charges,
            'Tax Amount': settlement.tax_amount,
            'Discount Amount': settlement.discount_amount,
            'Total Amount': settlement.total_amount,
            'Shipping Method': settlement.shipping_method,
            'Tracking Number': settlement.tracking_number,
            'Delivery Date': settlement.delivery_date.strftime('%Y-%m-%d %H:%M:%S') if settlement.delivery_date else None,
            'Order Status': settlement.order_status,
            'Payment Status': settlement.payment_status,
            'Payment Type': settlement.payment_type,
            'Warehouse ID': settlement.warehouse_id,
            'Seller ID': settlement.seller_id,
            'Return Type': settlement.return_type,
            'Return Date': settlement.return_date.strftime('%Y-%m-%d %H:%M:%S') if settlement.return_date else None,
            'Return Reason': settlement.return_reason
        } for settlement in settlements])
        
        if format.lower() == 'excel':
            # Create Excel writer
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Settlements', index=False)
                
                # Get workbook and worksheet objects
                workbook = writer.book
                worksheet = writer.sheets['Settlements']
                
                # Add formats
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#4CAF50',
                    'font_color': 'white',
                    'border': 1
                })
                
                # Format headers
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Auto-adjust column widths
                for idx, col in enumerate(df):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    )
                    worksheet.set_column(idx, idx, max_length + 2)
            
            return output.getvalue()
        else:
            # CSV format
            return df.to_csv(index=False)
    except Exception as e:
        logger.error(f"Error exporting settlements: {str(e)}")
        st.error(f"Error exporting settlements: {str(e)}")
        return None

def settlements_tab():
    """Settlements tab implementation."""
    st.title("Settlements Management")
    
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'sort_by' not in st.session_state:
        st.session_state.sort_by = 'created_at'
    if 'sort_order' not in st.session_state:
        st.session_state.sort_order = 'DESC'
    if 'selected_settlements' not in st.session_state:
        st.session_state.selected_settlements = set()
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    
    # Add refresh controls
    refresh_col1, refresh_col2 = st.columns([1, 3])
    with refresh_col1:
        if st.button("🔄 Refresh Data"):
            st.session_state.current_page = 1
            st.experimental_rerun()
    with refresh_col2:
        st.session_state.auto_refresh = st.checkbox("Auto-refresh every 5 minutes", value=st.session_state.auto_refresh)
    
    # Settlement Overview Section
    st.markdown("### Settlement Overview")
    
    # Get metrics with progress bar
    with st.spinner("Loading metrics..."):
        progress_bar = st.progress(0)
        metrics = get_settlement_metrics()
        progress_bar.progress(100)
    
    # Key Metrics Cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Expected Amount",
            f"₹{metrics['total_amounts']['expected']:,.2f}",
            help="Total expected settlement amount"
        )
    
    with col2:
        st.metric(
            "Total Actual Amount",
            f"₹{metrics['total_amounts']['actual']:,.2f}",
            help="Total actual settlement amount"
        )
    
    with col3:
        st.metric(
            "Pending Amount",
            f"₹{metrics['total_amounts']['pending']:,.2f}",
            help="Total pending settlement amount"
        )
    
    with col4:
        st.metric(
            "Completion Rate",
            f"{metrics['completion_rate']:.1f}%",
            help="Percentage of completed settlements"
        )
    
    # Processing Time Metrics
    st.markdown("#### Processing Time Metrics")
    time_col1, time_col2 = st.columns(2)
    
    with time_col1:
        avg_time = metrics['avg_time']
        st.metric(
            "Average Processing Time",
            f"{avg_time/86400:.1f} days",
            help="Average time for settlement processing"
        )
    
    with time_col2:
        st.metric(
            "Total Settlements",
            f"{sum(metrics['status_distribution'].values()):,}",
            help="Total number of settlements"
        )
    
    # Charts Row 1
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("#### Settlement Status Distribution")
        status_fig = px.pie(
            values=list(metrics['status_distribution'].values()),
            names=list(metrics['status_distribution'].keys()),
            title="Settlement Status"
        )
        st.plotly_chart(status_fig, use_container_width=True)
    
    with chart_col2:
        st.markdown("#### Settlement Amount Distribution")
        amount_fig = px.bar(
            x=list(metrics['amount_distribution'].keys()),
            y=list(metrics['amount_distribution'].values()),
            title="Settlement Amount Ranges"
        )
        st.plotly_chart(amount_fig, use_container_width=True)
    
    # Settlement Trends
    st.markdown("#### Settlement Trends")
    trend_fig = go.Figure()
    trend_fig.add_trace(go.Scatter(
        x=metrics['trends']['dates'],
        y=metrics['trends']['expected_amounts'],
        name='Expected Amount',
        line=dict(color='#4CAF50')
    ))
    trend_fig.add_trace(go.Scatter(
        x=metrics['trends']['dates'],
        y=metrics['trends']['actual_amounts'],
        name='Actual Amount',
        line=dict(color='#2196F3')
    ))
    trend_fig.update_layout(
        title='Settlement Amount Trends',
        xaxis_title='Date',
        yaxis_title='Amount (₹)',
        hovermode='x unified'
    )
    st.plotly_chart(trend_fig, use_container_width=True)
    
    # Search and Filter Section
    st.markdown("### Search & Filter Settlements")
    
    # Quick Search
    search_term = st.text_input("🔍 Quick Search", placeholder="Search by Order ID, Customer Name, or Item Name")
    
    # Essential Filters
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        date_range = st.date_input(
            "📅 Date Range",
            value=(
                datetime.now() - timedelta(days=30),
                datetime.now()
            ),
            max_value=datetime.now()
        )
    
    with filter_col2:
        settlement_status = st.selectbox(
            "Status",
            options=["All", "Completed", "Partial", "Pending"],
            index=0
        )
    
    with filter_col3:
        payment_type = st.selectbox(
            "Payment Type",
            options=["All", "Prepaid", "Postpaid"],
            index=0
        )
    
    # Advanced Filters
    with st.expander("Advanced Filters"):
        adv_col1, adv_col2, adv_col3 = st.columns(3)
        
        with adv_col1:
            warehouse_id = st.text_input("Warehouse ID")
            seller_id = st.text_input("Seller ID")
        
        with adv_col2:
            amount_range = st.number_input(
                "Min Amount",
                min_value=0.0,
                value=0.0,
                step=100.0
            ), st.number_input(
                "Max Amount",
                min_value=0.0,
                value=1000000.0,
                step=100.0
            )
        
        with adv_col3:
            commission_range = st.number_input(
                "Min Commission",
                min_value=0.0,
                value=0.0,
                step=10.0
            ), st.number_input(
                "Max Commission",
                min_value=0.0,
                value=1000.0,
                step=10.0
            )
    
    # Get settlements with filters
    settlements, total_count = get_settlements(
        start_date=date_range[0],
        end_date=date_range[1],
        settlement_status=settlement_status if settlement_status != "All" else None,
        payment_type=payment_type if payment_type != "All" else None,
        search_term=search_term if search_term else None,
        warehouse_id=warehouse_id if warehouse_id else None,
        seller_id=seller_id if seller_id else None,
        amount_range=amount_range if amount_range[0] > 0 or amount_range[1] < 1000000 else None,
        commission_range=commission_range if commission_range[0] > 0 or commission_range[1] < 1000 else None,
        page=st.session_state.current_page,
        page_size=10,
        sort_by=st.session_state.sort_by,
        sort_order=st.session_state.sort_order
    )
    
    # Sort Options
    sort_col1, sort_col2 = st.columns([3, 1])
    with sort_col1:
        st.markdown(f"**Total Results:** {total_count:,}")
    with sort_col2:
        sort_by = st.selectbox(
            "Sort By",
            options=['created_at', 'order_id', 'customer', 'amount', 'status', 'payment_type'],
            format_func=lambda x: x.replace('_', ' ').title(),
            index=list(st.session_state.sort_by.split('_')).index(st.session_state.sort_by)
        )
        st.session_state.sort_by = sort_by
    
    # Settlements Table
    st.markdown("### Settlements")
    
    # Table Header with Bulk Actions
    header_col1, header_col2, header_col3 = st.columns([2, 1, 1])
    with header_col1:
        st.markdown("**Select All**")
        if st.checkbox("", key="select_all"):
            st.session_state.selected_settlements = set(settlement.id for settlement in settlements)
    
    with header_col2:
        if st.button("Export Selected"):
            selected_settlements = [s for s in settlements if s.id in st.session_state.selected_settlements]
            if selected_settlements:
                csv_data = export_settlements(selected_settlements, format='csv')
                if csv_data:
                    st.download_button(
                        "Download CSV",
                        csv_data,
                        "selected_settlements.csv",
                        "text/csv"
                    )
    
    with header_col3:
        if st.button("Export All"):
            csv_data = export_settlements(settlements, format='csv')
            if csv_data:
                st.download_button(
                    "Download CSV",
                    csv_data,
                    "all_settlements.csv",
                    "text/csv"
                )
    
    # Table
    table_data = []
    for settlement in settlements:
        row = {
            "Select": settlement.id in st.session_state.selected_settlements,
            "ID": settlement.id,
            "Order ID": settlement.order_release_id,
            "Date": settlement.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            "Customer": settlement.customer_name,
            "Expected": f"₹{settlement.total_expected_settlement:,.2f}",
            "Actual": f"₹{settlement.total_actual_settlement:,.2f}",
            "Pending": f"₹{settlement.amount_pending_settlement:,.2f}",
            "Status": settlement.settlement_status,
            "Payment Type": settlement.payment_type
        }
        table_data.append(row)
    
    df = pd.DataFrame(table_data)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select for bulk actions",
                default=False
            ),
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["completed", "partial", "pending"],
                help="Settlement status"
            ),
            "Payment Type": st.column_config.SelectboxColumn(
                "Payment Type",
                options=["prepaid", "postpaid"],
                help="Payment type"
            )
        }
    )
    
    # Pagination
    total_pages = (total_count + 9) // 10
    if total_pages > 1:
        st.markdown("### Pagination")
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.button("Previous", disabled=st.session_state.current_page == 1):
                st.session_state.current_page -= 1
                st.experimental_rerun()
        
        with col2:
            st.markdown(f"Page {st.session_state.current_page} of {total_pages}")
        
        with col3:
            if st.button("Next", disabled=st.session_state.current_page == total_pages):
                st.session_state.current_page += 1
                st.experimental_rerun()

def get_monthly_reconciliation_summary(db: Session, year: int, month: int) -> Dict[str, Any]:
    """Get monthly reconciliation summary."""
    try:
        # Get start and end dates for the month
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        # Get orders summary
        orders = db.query(Order).filter(
            Order.created_at >= start_date,
            Order.created_at < end_date
        ).all()
        
        # Get returns summary
        returns = db.query(Return).filter(
            Return.created_at >= start_date,
            Return.created_at < end_date
        ).all()
        
        # Get settlements summary
        settlements = db.query(Settlement).filter(
            Settlement.created_at >= start_date,
            Settlement.created_at < end_date
        ).all()

        # Calculate metrics
        total_orders = len(orders)
        total_orders_amount = sum(order.total_amount for order in orders)
        total_returns = len(returns)
        total_returns_amount = sum(ret.amount for ret in returns)
        total_settlements = len(settlements)
        total_settlements_amount = sum(settlement.amount for settlement in settlements)
        
        # Calculate profit/loss
        gross_revenue = total_orders_amount
        return_costs = total_returns_amount
        net_profit = gross_revenue - return_costs
        
        # Calculate rates
        return_rate = (total_returns_amount / total_orders_amount * 100) if total_orders_amount > 0 else 0
        profit_margin = (net_profit / gross_revenue * 100) if gross_revenue > 0 else 0
        
        # Get status distributions
        order_status_dist = {}
        for order in orders:
            order_status_dist[order.status] = order_status_dist.get(order.status, 0) + 1
            
        return_status_dist = {}
        for ret in returns:
            return_status_dist[ret.status] = return_status_dist.get(ret.status, 0) + 1
            
        settlement_status_dist = {}
        for settlement in settlements:
            settlement_status_dist[settlement.status] = settlement_status_dist.get(settlement.status, 0) + 1

        return {
            'total_orders': total_orders,
            'total_orders_amount': total_orders_amount,
            'total_returns': total_returns,
            'total_returns_amount': total_returns_amount,
            'total_settlements': total_settlements,
            'total_settlements_amount': total_settlements_amount,
            'gross_revenue': gross_revenue,
            'return_costs': return_costs,
            'net_profit': net_profit,
            'return_rate': return_rate,
            'profit_margin': profit_margin,
            'order_status_dist': order_status_dist,
            'return_status_dist': return_status_dist,
            'settlement_status_dist': settlement_status_dist
        }
    except Exception as e:
        logger.error(f"Error getting monthly reconciliation summary: {str(e)}")
        raise

def get_monthly_trends(db: Session, year: int) -> Dict[str, List[Dict[str, Any]]]:
    """Get monthly trends for the specified year."""
    try:
        trends = {
            'orders': [],
            'returns': [],
            'settlements': [],
            'profit': []
        }
        
        for month in range(1, 13):
            summary = get_monthly_reconciliation_summary(db, year, month)
            
            trends['orders'].append({
                'month': month,
                'count': summary['total_orders'],
                'amount': summary['total_orders_amount']
            })
            
            trends['returns'].append({
                'month': month,
                'count': summary['total_returns'],
                'amount': summary['total_returns_amount']
            })
            
            trends['settlements'].append({
                'month': month,
                'count': summary['total_settlements'],
                'amount': summary['total_settlements_amount']
            })
            
            trends['profit'].append({
                'month': month,
                'amount': summary['net_profit'],
                'margin': summary['profit_margin']
            })
            
        return trends
    except Exception as e:
        logger.error(f"Error getting monthly trends: {str(e)}")
        raise

def get_custom_report(db: Session, start_date: datetime, end_date: datetime, metrics: List[str]) -> Dict[str, Any]:
    """Generate custom report based on selected metrics and date range."""
    try:
        # Get orders
        orders = db.query(Order).filter(
            Order.created_at >= start_date,
            Order.created_at <= end_date
        ).all()
        
        # Get returns
        returns = db.query(Return).filter(
            Return.created_at >= start_date,
            Return.created_at <= end_date
        ).all()
        
        # Get settlements
        settlements = db.query(Settlement).filter(
            Settlement.created_at >= start_date,
            Settlement.created_at <= end_date
        ).all()
        
        report_data = {}
        
        # Calculate selected metrics
        if 'orders' in metrics:
            report_data['orders'] = {
                'total_count': len(orders),
                'total_amount': sum(order.total_amount for order in orders),
                'status_distribution': {}
            }
            for order in orders:
                report_data['orders']['status_distribution'][order.status] = \
                    report_data['orders']['status_distribution'].get(order.status, 0) + 1
                
        if 'returns' in metrics:
            report_data['returns'] = {
                'total_count': len(returns),
                'total_amount': sum(ret.amount for ret in returns),
                'status_distribution': {}
            }
            for ret in returns:
                report_data['returns']['status_distribution'][ret.status] = \
                    report_data['returns']['status_distribution'].get(ret.status, 0) + 1
                
        if 'settlements' in metrics:
            report_data['settlements'] = {
                'total_count': len(settlements),
                'total_amount': sum(settlement.amount for settlement in settlements),
                'status_distribution': {}
            }
            for settlement in settlements:
                report_data['settlements']['status_distribution'][settlement.status] = \
                    report_data['settlements']['status_distribution'].get(settlement.status, 0) + 1
                
        if 'financial' in metrics:
            report_data['financial'] = {
                'gross_revenue': sum(order.total_amount for order in orders),
                'return_costs': sum(ret.amount for ret in returns),
                'net_profit': sum(order.total_amount for order in orders) - sum(ret.amount for ret in returns),
                'profit_margin': ((sum(order.total_amount for order in orders) - sum(ret.amount for ret in returns)) / 
                                sum(order.total_amount for order in orders) * 100) if orders else 0
            }
            
        return report_data
    except Exception as e:
        logger.error(f"Error generating custom report: {str(e)}")
        raise

def reconciliation_reports_tab():
    """Reconciliation Reports tab implementation."""
    st.title("Reconciliation Reports")
    
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'sort_by' not in st.session_state:
        st.session_state.sort_by = 'created_at'
    if 'sort_order' not in st.session_state:
        st.session_state.sort_order = 'DESC'
    if 'selected_orders' not in st.session_state:
        st.session_state.selected_orders = set()
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    
    # Add refresh controls
    refresh_col1, refresh_col2 = st.columns([1, 3])
    with refresh_col1:
        if st.button("🔄 Refresh Data"):
            st.session_state.current_page = 1
            st.experimental_rerun()
    with refresh_col2:
        st.session_state.auto_refresh = st.checkbox("Auto-refresh every 5 minutes", value=st.session_state.auto_refresh)
    
    # Get metrics with progress bar
    with st.spinner("Loading metrics..."):
        progress_bar = st.progress(0)
        metrics = get_daily_metrics()
        progress_bar.progress(100)
    
    # Export Options
    st.markdown("### Export Options")
    export_col1, export_col2 = st.columns(2)
    
    with export_col1:
        if st.button("Export CSV"):
            data = export_reconciliation_report(metrics, format='csv')
            if data is not None:
                st.download_button(
                    label="Download CSV Report",
                    data=data,
                    file_name=f"reconciliation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
    
    with export_col2:
        if st.button("Export PDF"):
            data = export_reconciliation_report(metrics, format='pdf')
            if data is not None:
                b64 = base64.b64encode(data).decode()
                href = f'<a href="data:application/pdf;base64,{b64}" download="reconciliation_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf">Click here to download the PDF report</a>'
                st.markdown(href, unsafe_allow_html=True)
    
    # Monthly Reports Section
    st.markdown("### Monthly Reports")
    
    # Year and Month Selection
    col1, col2 = st.columns(2)
    with col1:
        year = st.selectbox("Select Year", range(2020, datetime.now().year + 1), index=datetime.now().year - 2020)
    with col2:
        month = st.selectbox("Select Month", range(1, 13), format_func=lambda x: datetime(2000, x, 1).strftime('%B'))
    
    # Get monthly metrics
    monthly_metrics = get_monthly_reconciliation_summary(SessionLocal(), year, month)
    
    # Financial Overview
    st.markdown("#### Financial Overview")
    financial_col1, financial_col2, financial_col3, financial_col4 = st.columns(4)
    
    with financial_col1:
        st.metric(
            "Total Sales",
            f"₹{monthly_metrics['total_orders_amount']:,.2f}",
            help="Total sales amount for the selected month"
        )
    
    with financial_col2:
        st.metric(
            "Total Returns",
            f"₹{monthly_metrics['total_returns_amount']:,.2f}",
            help="Total returns amount for the selected month"
        )
    
    with financial_col3:
        st.metric(
            "Total Settlements",
            f"₹{monthly_metrics['total_settlements_amount']:,.2f}",
            help="Total settlements amount for the selected month"
        )
    
    with financial_col4:
        st.metric(
            "Net Profit",
            f"₹{monthly_metrics['net_profit']:,.2f}",
            help="Net profit for the selected month"
        )
    
    # Status Distributions
    st.markdown("#### Status Distributions")
    status_col1, status_col2, status_col3 = st.columns(3)
    
    with status_col1:
        st.markdown("##### Order Status")
        order_status_fig = px.pie(
            values=list(monthly_metrics['order_status_dist'].values()),
            names=list(monthly_metrics['order_status_dist'].keys()),
            title="Order Status Distribution"
        )
        st.plotly_chart(order_status_fig, use_container_width=True)
    
    with status_col2:
        st.markdown("##### Return Status")
        return_status_fig = px.pie(
            values=list(monthly_metrics['return_status_dist'].values()),
            names=list(monthly_metrics['return_status_dist'].keys()),
            title="Return Status Distribution"
        )
        st.plotly_chart(return_status_fig, use_container_width=True)
    
    with status_col3:
        st.markdown("##### Settlement Status")
        settlement_status_fig = px.pie(
            values=list(monthly_metrics['settlement_status_dist'].values()),
            names=list(monthly_metrics['settlement_status_dist'].keys()),
            title="Settlement Status Distribution"
        )
        st.plotly_chart(settlement_status_fig, use_container_width=True)
    
    # Monthly Trends
    st.markdown("#### Monthly Trends")
    trend_col1, trend_col2 = st.columns(2)
    
    with trend_col1:
        st.markdown("##### Order Trends")
        order_trend_fig = px.line(
            x=list(monthly_metrics['order_trends'].keys()),
            y=list(monthly_metrics['order_trends'].values()),
            title="Order Count Trend"
        )
        st.plotly_chart(order_trend_fig, use_container_width=True)
    
    with trend_col2:
        st.markdown("##### Return Trends")
        return_trend_fig = px.line(
            x=list(monthly_metrics['return_trends'].keys()),
            y=list(monthly_metrics['return_trends'].values()),
            title="Return Count Trend"
        )
        st.plotly_chart(return_trend_fig, use_container_width=True)
    
    # Custom Report Options
    st.markdown("### Custom Report")
    
    # Date Range Selection
    date_col1, date_col2 = st.columns(2)
    with date_col1:
        start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
    with date_col2:
        end_date = st.date_input("End Date", datetime.now())
    
    # Metrics Selection
    st.markdown("#### Select Metrics")
    metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
    
    with metrics_col1:
        include_orders = st.checkbox("Orders", value=True)
    with metrics_col2:
        include_returns = st.checkbox("Returns", value=True)
    with metrics_col3:
        include_settlements = st.checkbox("Settlements", value=True)
    with metrics_col4:
        include_financial = st.checkbox("Financial", value=True)
    
    # Generate Custom Report
    if st.button("Generate Custom Report"):
        custom_metrics = get_custom_report_metrics(start_date, end_date)
        
        # Display selected metrics
        if include_orders:
            st.markdown("#### Order Metrics")
            order_metrics_col1, order_metrics_col2, order_metrics_col3 = st.columns(3)
            
            with order_metrics_col1:
                st.metric(
                    "Total Orders",
                    f"{custom_metrics['orders']['total_orders']:,}",
                    help="Total number of orders in the selected period"
                )
            
            with order_metrics_col2:
                st.metric(
                    "Delivered Orders",
                    f"{custom_metrics['orders']['delivered_orders']:,}",
                    help="Number of delivered orders"
                )
            
            with order_metrics_col3:
                st.metric(
                    "Cancelled Orders",
                    f"{custom_metrics['orders']['cancelled_orders']:,}",
                    help="Number of cancelled orders"
                )
        
        if include_returns:
            st.markdown("#### Return Metrics")
            return_metrics_col1, return_metrics_col2, return_metrics_col3 = st.columns(3)
            
            with return_metrics_col1:
                st.metric(
                    "Total Returns",
                    f"{custom_metrics['returns']['total_returns']:,}",
                    help="Total number of returns"
                )
            
            with return_metrics_col2:
                st.metric(
                    "Refund Returns",
                    f"{custom_metrics['returns']['refund_returns']:,}",
                    help="Number of refund returns"
                )
            
            with return_metrics_col3:
                st.metric(
                    "Exchange Returns",
                    f"{custom_metrics['returns']['exchange_returns']:,}",
                    help="Number of exchange returns"
                )
        
        if include_settlements:
            st.markdown("#### Settlement Metrics")
            settlement_metrics_col1, settlement_metrics_col2, settlement_metrics_col3 = st.columns(3)
            
            with settlement_metrics_col1:
                st.metric(
                    "Completed Settlements",
                    f"{custom_metrics['settlements']['completed_settlements']:,}",
                    help="Number of completed settlements"
                )
            
            with settlement_metrics_col2:
                st.metric(
                    "Partial Settlements",
                    f"{custom_metrics['settlements']['partial_settlements']:,}",
                    help="Number of partial settlements"
                )
            
            with settlement_metrics_col3:
                st.metric(
                    "Pending Settlements",
                    f"{custom_metrics['settlements']['pending_settlements']:,}",
                    help="Number of pending settlements"
                )
        
        if include_financial:
            st.markdown("#### Financial Metrics")
            financial_metrics_col1, financial_metrics_col2, financial_metrics_col3 = st.columns(3)
            
            with financial_metrics_col1:
                st.metric(
                    "Total Sales",
                    f"₹{custom_metrics['financial']['total_sales']:,.2f}",
                    help="Total sales amount"
                )
            
            with financial_metrics_col2:
                st.metric(
                    "Total Returns",
                    f"₹{custom_metrics['financial']['total_returns']:,.2f}",
                    help="Total returns amount"
                )
            
            with financial_metrics_col3:
                st.metric(
                    "Net Profit",
                    f"₹{custom_metrics['financial']['net_profit']:,.2f}",
                    help="Net profit"
                )
        
        # Export Custom Report
        st.markdown("### Export Custom Report")
        custom_export_col1, custom_export_col2 = st.columns(2)
        
        with custom_export_col1:
            if st.button("Export Custom CSV"):
                data = export_reconciliation_report(custom_metrics, format='csv')
                if data is not None:
                    st.download_button(
                        label="Download Custom CSV Report",
                        data=data,
                        file_name=f"custom_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
        
        with custom_export_col2:
            if st.button("Export Custom PDF"):
                data = export_reconciliation_report(custom_metrics, format='pdf')
                if data is not None:
                    b64 = base64.b64encode(data).decode()
                    href = f'<a href="data:application/pdf;base64,{b64}" download="custom_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf">Click here to download the custom PDF report</a>'
                    st.markdown(href, unsafe_allow_html=True)

def export_reconciliation_report(metrics: Dict[str, Any], format: str = 'csv') -> Optional[bytes]:
    """Export reconciliation report data to CSV or PDF format."""
    try:
        if format.lower() == 'pdf':
            return generate_pdf_report(metrics)
        else:
            # Create DataFrame with all metrics
            data = []
            
            # Financial Overview
            financial_data = {
                'Total Sales': metrics.get('orders', {}).get('total_order_amount', 0),
                'Pending Settlements': metrics.get('returns', {}).get('pending_settlements', 0),
                'Net Profit': metrics.get('orders', {}).get('total_order_amount', 0) - metrics.get('returns', {}).get('pending_settlements', 0),
                'Return Rate': (metrics.get('returns', {}).get('total_returns', 0) / metrics.get('orders', {}).get('total_orders', 1)) * 100
            }
            data.append({**{'Section': 'Financial Overview'}, **financial_data})
            
            # Order Performance
            order_data = {
                'Total Orders': metrics.get('orders', {}).get('total_orders', 0),
                'Returns': metrics.get('returns', {}).get('total_returns', 0),
                'Exchange Rate': metrics.get('returns', {}).get('exchange_returns', 0)
            }
            data.append({**{'Section': 'Order Performance'}, **order_data})
            
            # Settlement Status
            settlement_data = {
                'Completed Settlements': metrics.get('settlements', {}).get('completed_settlements', 0),
                'Partial Settlements': metrics.get('settlements', {}).get('partial_settlements', 0),
                'Pending Settlements': metrics.get('settlements', {}).get('pending_settlements', 0)
            }
            data.append({**{'Section': 'Settlement Status'}, **settlement_data})
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Export to CSV
            return df.to_csv(index=False).encode('utf-8')
            
    except Exception as e:
        logger.error(f"Error exporting reconciliation report: {str(e)}")
        return None

def dashboard_tab():
    """Dashboard tab implementation."""
    st.title("Dashboard")
    
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'sort_by' not in st.session_state:
        st.session_state.sort_by = 'created_at'
    if 'sort_order' not in st.session_state:
        st.session_state.sort_order = 'DESC'
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    
    # Add refresh controls
    refresh_col1, refresh_col2 = st.columns([1, 3])
    with refresh_col1:
        if st.button("🔄 Refresh Data"):
            st.session_state.current_page = 1
            st.experimental_rerun()
    with refresh_col2:
        st.session_state.auto_refresh = st.checkbox("Auto-refresh every 5 minutes", value=st.session_state.auto_refresh)
    
    # Get metrics with progress bar
    with st.spinner("Loading metrics..."):
        progress_bar = st.progress(0)
        metrics = get_daily_metrics()
        progress_bar.progress(100)
    
    # Financial Analysis Section
    st.markdown("### Financial Analysis")
    financial_col1, financial_col2, financial_col3 = st.columns(3)
    
    with financial_col1:
        # Calculate revenue trend
        revenue_trend = metrics.get('revenue_trend', 0)
        trend_arrow = "↑" if revenue_trend > 0 else "↓" if revenue_trend < 0 else "→"
        trend_color = "green" if revenue_trend > 0 else "red" if revenue_trend < 0 else "gray"
        
        st.metric(
            "Monthly Revenue",
            f"₹{metrics.get('total_revenue', 0):,.2f}",
            f"{trend_arrow} {abs(revenue_trend):.1f}%",
            help="Total revenue for the current month"
        )
    
    with financial_col2:
        # Calculate returns trend
        returns_trend = metrics.get('returns_trend', 0)
        trend_arrow = "↑" if returns_trend > 0 else "↓" if returns_trend < 0 else "→"
        trend_color = "red" if returns_trend > 0 else "green" if returns_trend < 0 else "gray"
        
        st.metric(
            "Monthly Returns",
            f"₹{metrics.get('total_returns', 0):,.2f}",
            f"{trend_arrow} {abs(returns_trend):.1f}%",
            help="Total returns for the current month"
        )
    
    with financial_col3:
        # Calculate net profit trend
        profit_trend = metrics.get('profit_trend', 0)
        trend_arrow = "↑" if profit_trend > 0 else "↓" if profit_trend < 0 else "→"
        trend_color = "green" if profit_trend > 0 else "red" if profit_trend < 0 else "gray"
        
        st.metric(
            "Net Profit",
            f"₹{metrics.get('net_profit', 0):,.2f}",
            f"{trend_arrow} {abs(profit_trend):.1f}%",
            help="Net profit for the current month"
        )
    
    # Financial Trends Chart
    st.markdown("#### Financial Trends")
    trend_fig = go.Figure()
    
    # Add revenue line
    trend_fig.add_trace(go.Scatter(
        x=metrics.get('trend_dates', []),
        y=metrics.get('revenue_data', []),
        name='Revenue',
        line=dict(color='#4CAF50')
    ))
    
    # Add returns line
    trend_fig.add_trace(go.Scatter(
        x=metrics.get('trend_dates', []),
        y=metrics.get('returns_data', []),
        name='Returns',
        line=dict(color='#F44336')
    ))
    
    # Add net profit line
    trend_fig.add_trace(go.Scatter(
        x=metrics.get('trend_dates', []),
        y=metrics.get('profit_data', []),
        name='Net Profit',
        line=dict(color='#2196F3')
    ))
    
    trend_fig.update_layout(
        title="Monthly Financial Trends",
        xaxis_title="Date",
        yaxis_title="Amount (₹)",
        hovermode='x unified'
    )
    
    st.plotly_chart(trend_fig, use_container_width=True)
    
    # ... existing code ...

def data_quality_tab():
    """Data Quality tab implementation."""
    st.title("Data Quality")
    
    # Initialize session state
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    
    # Add refresh controls
    refresh_col1, refresh_col2 = st.columns([1, 3])
    with refresh_col1:
        if st.button("🔄 Refresh Data"):
            st.experimental_rerun()
    with refresh_col2:
        st.session_state.auto_refresh = st.checkbox("Auto-refresh every 5 minutes", value=st.session_state.auto_refresh)
    
    # Get data quality metrics
    with st.spinner("Checking data quality..."):
        quality_metrics = get_data_quality_metrics()
    
    # Data Health Overview
    st.markdown("### Data Health Overview")
    health_score = quality_metrics.get('health_score', 0)
    health_color = "green" if health_score >= 90 else "yellow" if health_score >= 70 else "red"
    
    st.metric(
        "Overall Data Health",
        f"{health_score}%",
        help="Percentage of data that meets quality standards",
        delta_color=health_color
    )
    
    # Critical Issues Section
    st.markdown("### Critical Issues")
    
    # Check for missing order IDs
    missing_order_ids = quality_metrics.get('missing_order_ids', 0)
    if missing_order_ids > 0:
        st.error(f"⚠️ {missing_order_ids} orders are missing order IDs")
        st.markdown("""
            **Impact:** This can cause problems with tracking orders and processing returns.
            **Action:** Please check the orders data and add missing order IDs.
        """)
    
    # Check for missing customer information
    missing_customer_info = quality_metrics.get('missing_customer_info', 0)
    if missing_customer_info > 0:
        st.error(f"⚠️ {missing_customer_info} orders are missing customer information")
        st.markdown("""
            **Impact:** This can affect customer communication and return processing.
            **Action:** Please update customer details in the orders data.
        """)
    
    # Check for missing payment information
    missing_payment_info = quality_metrics.get('missing_payment_info', 0)
    if missing_payment_info > 0:
        st.error(f"⚠️ {missing_payment_info} orders are missing payment information")
        st.markdown("""
            **Impact:** This can affect settlement processing and payment tracking.
            **Action:** Please add payment details to the affected orders.
        """)
    
    # Check for mismatched amounts
    mismatched_amounts = quality_metrics.get('mismatched_amounts', 0)
    if mismatched_amounts > 0:
        st.error(f"⚠️ {mismatched_amounts} orders have mismatched amounts")
        st.markdown("""
            **Impact:** This can cause reconciliation issues and incorrect financial reporting.
            **Action:** Please verify the amounts in orders, returns, and settlements.
        """)
    
    # If no critical issues found
    if all([missing_order_ids == 0, missing_customer_info == 0, 
            missing_payment_info == 0, mismatched_amounts == 0]):
        st.success("✅ No critical data quality issues found!")
        st.markdown("""
            Your data is in good shape! All critical information is present and accurate.
            Continue with your regular operations.
        """)
    
    # Data Quality History
    st.markdown("### Data Quality History")
    history_data = quality_metrics.get('history', [])
    if history_data:
        history_df = pd.DataFrame(history_data)
        history_fig = px.line(
            history_df,
            x='date',
            y='health_score',
            title="Data Health Score Trend"
        )
        history_fig.update_layout(
            yaxis_title="Health Score (%)",
            xaxis_title="Date"
        )
        st.plotly_chart(history_fig, use_container_width=True)
    else:
        st.info("No historical data available yet.")

def get_data_quality_metrics():
    """Get data quality metrics and check for critical issues."""
    try:
        # Initialize metrics
        metrics = {
            'health_score': 100,
            'missing_order_ids': 0,
            'missing_customer_info': 0,
            'missing_payment_info': 0,
            'mismatched_amounts': 0,
            'history': []
        }
        
        # Read master data files
        orders_df = read_file(ORDERS_MASTER) if os.path.exists(ORDERS_MASTER) else pd.DataFrame()
        returns_df = read_file(RETURNS_MASTER) if os.path.exists(RETURNS_MASTER) else pd.DataFrame()
        settlement_df = read_file(SETTLEMENT_MASTER) if os.path.exists(SETTLEMENT_MASTER) else pd.DataFrame()
        
        if not orders_df.empty:
            # Check for missing order IDs
            metrics['missing_order_ids'] = orders_df['order_release_id'].isna().sum()
            
            # Check for missing customer information
            required_customer_fields = ['customer_name', 'customer_email', 'customer_phone']
            missing_customer = orders_df[required_customer_fields].isna().any(axis=1)
            metrics['missing_customer_info'] = missing_customer.sum()
            
            # Check for missing payment information
            required_payment_fields = ['payment_type', 'payment_status']
            missing_payment = orders_df[required_payment_fields].isna().any(axis=1)
            metrics['missing_payment_info'] = missing_payment.sum()
            
            # Check for mismatched amounts
            if not returns_df.empty and not settlement_df.empty:
                # Merge data to check for mismatches
                merged_df = orders_df.merge(
                    returns_df[['order_release_id', 'return_amount']],
                    on='order_release_id',
                    how='left'
                ).merge(
                    settlement_df[['order_release_id', 'settlement_amount']],
                    on='order_release_id',
                    how='left'
                )
                
                # Check for mismatches between order amount and return/settlement amounts
                mismatched = merged_df[
                    (merged_df['final_amount'] != merged_df['return_amount'].fillna(0) + merged_df['settlement_amount'].fillna(0))
                ]
                metrics['mismatched_amounts'] = len(mismatched)
        
        # Calculate health score
        total_checks = 4  # Number of critical checks
        failed_checks = sum([
            metrics['missing_order_ids'] > 0,
            metrics['missing_customer_info'] > 0,
            metrics['missing_payment_info'] > 0,
            metrics['mismatched_amounts'] > 0
        ])
        metrics['health_score'] = max(0, 100 - (failed_checks / total_checks * 100))
        
        # Add to history
        metrics['history'].append({
            'date': datetime.now().strftime('%Y-%m-%d'),
            'health_score': metrics['health_score']
        })
        
        # Keep only last 30 days of history
        metrics['history'] = metrics['history'][-30:]
        
        return metrics
    
    except Exception as e:
        st.error(f"Error checking data quality: {str(e)}")
        return {
            'health_score': 0,
            'missing_order_ids': 0,
            'missing_customer_info': 0,
            'missing_payment_info': 0,
            'mismatched_amounts': 0,
            'history': []
        }

def settlement_tab():
    """Settlement Management Tab"""
    st.title("Settlement Management")
    
    # Initialize session state for pagination and sorting
    if 'settlement_page' not in st.session_state:
        st.session_state.settlements_page = 1
    if 'settlements_sort_by' not in st.session_state:
        st.session_state.settlements_sort_by = 'settlement_date'
    if 'settlements_sort_order' not in st.session_state:
        st.session_state.settlements_sort_order = 'desc'
    if 'selected_settlements' not in st.session_state:
        st.session_state.selected_settlements = []
    if 'auto_refresh_settlements' not in st.session_state:
        st.session_state.auto_refresh_settlements = True
    
    # Add refresh controls
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🔄 Refresh Settlements"):
            st.session_state.settlements_page = 1
            st.experimental_rerun()
    with col2:
        st.checkbox("Auto-refresh every 5 minutes", 
                   key="auto_refresh_settlements",
                   help="Automatically refresh the settlements data every 5 minutes")
    
    # Add progress bar for loading
    with st.spinner("Loading settlement data..."):
        progress_bar = st.progress(0)
        
        # Get current month
        current_month = datetime.now().strftime('%Y-%m')
        
        # Get settlement analysis
        settlement_analysis = analyze_settlements(db, current_month)
        
        # Update progress
        progress_bar.progress(50)
        
        # Settlement Overview Section
        st.subheader("Settlement Overview")
        
        # Create metrics columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Settlements",
                f"{settlement_analysis['total_settlements']:,}",
                f"{settlement_analysis['completion_rate']:.1f}% Complete"
            )
        
        with col2:
            st.metric(
                "Pending from Previous",
                f"{settlement_analysis['pending_from_previous']:,}",
                f"₹{settlement_analysis['total_amount_pending']:,.2f}"
            )
        
        with col3:
            st.metric(
                "Amount Settled",
                f"₹{settlement_analysis['total_amount_settled']:,.2f}",
                f"{settlement_analysis['amount_completion_rate']:.1f}% Complete"
            )
        
        with col4:
            st.metric(
                "Avg Settlement Time",
                f"{settlement_analysis['avg_settlement_time']:.1f} days",
                f"{settlement_analysis['partial_settlements']} Partial"
            )
        
        # Settlement Status Distribution
        st.subheader("Settlement Status Distribution")
        status_data = {
            'Completed': settlement_analysis['completed_settlements'],
            'Partial': settlement_analysis['partial_settlements'],
            'Pending': settlement_analysis['pending_settlements']
        }
        fig = px.pie(
            values=list(status_data.values()),
            names=list(status_data.keys()),
            title="Settlement Status Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Settlement History Section
        st.subheader("Settlement History")
        
        # Get settlement history
        history_query = db.query(SettlementHistory).filter(
            SettlementHistory.month == current_month
        ).order_by(
            SettlementHistory.settlement_date.desc()
        )
        
        # Add filters
        col1, col2 = st.columns(2)
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=['completed', 'partial', 'pending'],
                default=['completed', 'partial', 'pending']
            )
            if status_filter:
                history_query = history_query.filter(
                    SettlementHistory.settlement_status.in_(status_filter)
                )
        
        with col2:
            date_range = st.date_input(
                "Date Range",
                value=(
                    datetime.now().replace(day=1),
                    datetime.now()
                )
            )
            if len(date_range) == 2:
                history_query = history_query.filter(
                    SettlementHistory.settlement_date.between(
                        date_range[0],
                        date_range[1]
                    )
                )
        
        # Get history data
        history_data = history_query.all()
        
        # Create history DataFrame
        history_df = pd.DataFrame([{
            'Order ID': h.order_release_id,
            'Date': h.settlement_date,
            'Status': h.settlement_status.title(),
            'Amount Settled': h.amount_settled,
            'Amount Pending': h.amount_pending,
            'Month': h.month
        } for h in history_data])
        
        if not history_df.empty:
            # Add status change tracking
            history_df['Status Change'] = history_df.groupby('Order ID')['Status'].diff()
            history_df['Amount Change'] = history_df.groupby('Order ID')['Amount Settled'].diff()
            
            # Format the DataFrame
            history_df['Date'] = pd.to_datetime(history_df['Date']).dt.strftime('%Y-%m-%d %H:%M')
            history_df['Amount Settled'] = history_df['Amount Settled'].apply(lambda x: f"₹{x:,.2f}")
            history_df['Amount Pending'] = history_df['Amount Pending'].apply(lambda x: f"₹{x:,.2f}")
            history_df['Amount Change'] = history_df['Amount Change'].apply(
                lambda x: f"₹{x:+,.2f}" if pd.notna(x) else ""
            )
            
            # Display the history table
            st.dataframe(
                history_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Add settlement timeline visualization
            st.subheader("Settlement Timeline")
            
            # Create timeline data
            timeline_data = history_df.copy()
            timeline_data['Date'] = pd.to_datetime(timeline_data['Date'])
            
            fig = px.scatter(
                timeline_data,
                x='Date',
                y='Amount Settled',
                color='Status',
                hover_data=['Order ID', 'Amount Pending', 'Status Change'],
                title="Settlement Timeline"
            )
            fig.update_layout(
                yaxis_title="Amount Settled (₹)",
                xaxis_title="Date",
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No settlement history found for the selected filters.")
        
        # Update progress
        progress_bar.progress(100)
        
        # Add auto-refresh functionality
        if st.session_state.auto_refresh_settlements:
            time.sleep(300)  # 5 minutes
            st.experimental_rerun()

def settlement_reports_tab():
    """Settlement Reports tab implementation."""
    st.title("Settlement Reports")
    
    # Initialize session state
    if 'settlement_report_type' not in st.session_state:
        st.session_state.settlement_report_type = 'cross_month'
    if 'settlement_report_month' not in st.session_state:
        st.session_state.settlement_report_month = datetime.now().strftime('%Y-%m')
    
    # Report Type Selection
    report_type = st.radio(
        "Select Report Type",
        options=['cross_month', 'pending', 'history'],
        format_func=lambda x: x.replace('_', ' ').title(),
        key='settlement_report_type'
    )
    
    # Date Selection
    col1, col2 = st.columns(2)
    with col1:
        year = st.selectbox(
            "Select Year",
            range(2020, datetime.now().year + 1),
            index=datetime.now().year - 2020
        )
    with col2:
        month = st.selectbox(
            "Select Month",
            range(1, 13),
            format_func=lambda x: datetime(2000, x, 1).strftime('%B')
        )
    
    selected_month = f"{year}-{month:02d}"
    
    # Export Options
    export_col1, export_col2 = st.columns(2)
    with export_col1:
        if st.button("Export CSV"):
            if report_type == 'cross_month':
                data = export_cross_month_report(selected_month)
            elif report_type == 'pending':
                data = export_pending_settlements_report(selected_month)
            else:
                data = export_settlement_history_report(selected_month)
            
            if data:
                st.download_button(
                    "Download CSV",
                    data,
                    f"settlement_report_{report_type}_{selected_month}.csv",
                    "text/csv"
                )
    
    with export_col2:
        if st.button("Export PDF"):
            if report_type == 'cross_month':
                data = generate_cross_month_pdf(selected_month)
            elif report_type == 'pending':
                data = generate_pending_pdf(selected_month)
            else:
                data = generate_history_pdf(selected_month)
            
            if data:
                b64 = base64.b64encode(data).decode()
                href = f'<a href="data:application/pdf;base64,{b64}" download="settlement_report_{report_type}_{selected_month}.pdf">Click here to download the PDF report</a>'
                st.markdown(href, unsafe_allow_html=True)
    
    # Report Content
    if report_type == 'cross_month':
        show_cross_month_report(selected_month)
    elif report_type == 'pending':
        show_pending_settlements_report(selected_month)
    else:
        show_settlement_history_report(selected_month)

def show_cross_month_report(month: str):
    """Display cross-month settlement analysis report."""
    try:
        # Get current month analysis
        current_analysis = analyze_settlements(db, month)
        
        # Get previous month analysis
        prev_month = (datetime.strptime(month, '%Y-%m') - pd.DateOffset(months=1)).strftime('%Y-%m')
        prev_analysis = analyze_settlements(db, prev_month)
        
        # Cross-month metrics
        st.subheader("Cross-Month Settlement Analysis")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Pending from Previous Month",
                f"{prev_analysis['pending_settlements']:,}",
                f"₹{prev_analysis['total_amount_pending']:,.2f}"
            )
        
        with col2:
            completed_from_prev = prev_analysis['pending_settlements'] - current_analysis['pending_from_previous']
            st.metric(
                "Completed from Previous",
                f"{completed_from_prev:,}",
                f"{completed_from_prev/prev_analysis['pending_settlements']*100:.1f}% Completion"
            )
        
        with col3:
            st.metric(
                "Carried Forward",
                f"{current_analysis['pending_from_previous']:,}",
                f"₹{current_analysis['total_amount_pending']:,.2f}"
            )
        
        # Settlement Trends
        st.subheader("Settlement Trends")
        trends_data = current_analysis['trends']
        
        if trends_data:
            df = pd.DataFrame([{
                'Month': t.month,
                'Total': t.total_settlements,
                'Completed': t.completed_settlements,
                'Partial': t.partial_settlements,
                'Pending': t.pending_settlements,
                'Amount Settled': t.total_settled,
                'Amount Pending': t.total_pending
            } for t in trends_data])
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df['Month'],
                y=df['Completed'],
                name='Completed',
                marker_color='green'
            ))
            fig.add_trace(go.Bar(
                x=df['Month'],
                y=df['Partial'],
                name='Partial',
                marker_color='orange'
            ))
            fig.add_trace(go.Bar(
                x=df['Month'],
                y=df['Pending'],
                name='Pending',
                marker_color='red'
            ))
            
            fig.update_layout(
                title="Settlement Status Distribution Over Time",
                barmode='stack',
                xaxis_title="Month",
                yaxis_title="Number of Settlements"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Amount trends
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df['Month'],
                y=df['Amount Settled'],
                name='Amount Settled',
                line=dict(color='green')
            ))
            fig.add_trace(go.Scatter(
                x=df['Month'],
                y=df['Amount Pending'],
                name='Amount Pending',
                line=dict(color='red')
            ))
            
            fig.update_layout(
                title="Settlement Amount Trends",
                xaxis_title="Month",
                yaxis_title="Amount (₹)"
            )
            st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error generating cross-month report: {str(e)}")

def show_pending_settlements_report(month: str):
    """Display pending settlements report."""
    try:
        # Get pending settlements
        pending_settlements = Settlement.get_pending_settlements(db, month)
        
        st.subheader("Pending Settlements Summary")
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Total Pending",
                f"{len(pending_settlements):,}",
                f"₹{sum(s.amount_pending for s in pending_settlements):,.2f}"
            )
        
        with col2:
            avg_pending = sum(s.amount_pending for s in pending_settlements) / len(pending_settlements) if pending_settlements else 0
            st.metric(
                "Average Pending Amount",
                f"₹{avg_pending:,.2f}",
                f"{len([s for s in pending_settlements if s.amount_pending > avg_pending])} Above Average"
            )
        
        with col3:
            days_pending = sum(
                (datetime.now().date() - s.settlement_date).days 
                for s in pending_settlements
            ) / len(pending_settlements) if pending_settlements else 0
            st.metric(
                "Average Days Pending",
                f"{days_pending:.1f} days",
                f"{len([s for s in pending_settlements if (datetime.now().date() - s.settlement_date).days > days_pending])} Above Average"
            )
        
        # Pending settlements table
        st.subheader("Pending Settlements Details")
        
        if pending_settlements:
            df = pd.DataFrame([{
                'Order ID': s.order_release_id,
                'Date': s.settlement_date.strftime('%Y-%m-%d'),
                'Expected Amount': s.order.final_amount,
                'Amount Settled': s.amount_settled,
                'Amount Pending': s.amount_pending,
                'Days Pending': (datetime.now().date() - s.settlement_date).days,
                'Status': s.settlement_status
            } for s in pending_settlements])
            
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True
            )
            
            # Distribution of pending amounts
            fig = px.histogram(
                df,
                x='Amount Pending',
                title="Distribution of Pending Amounts",
                labels={'Amount Pending': 'Amount (₹)'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pending settlements found for the selected month.")
        
    except Exception as e:
        st.error(f"Error generating pending settlements report: {str(e)}")

def show_settlement_history_report(month: str):
    """Display settlement history report."""
    try:
        # Get settlement history
        history = SettlementHistory.get_settlement_trends(db, month, month)
        
        st.subheader("Settlement History")
        
        if history:
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Total History Records",
                    f"{len(history):,}",
                    f"₹{sum(h.total_settled for h in history):,.2f} Total Settled"
                )
            
            with col2:
                st.metric(
                    "Status Changes",
                    f"{sum(1 for h in history if h.total_settlements > 0):,}",
                    f"{sum(h.completed_settlements for h in history):,} Completed"
                )
            
            with col3:
                st.metric(
                    "Average Settlement Time",
                    f"{sum((datetime.now().date() - h.settlement_date).days for h in history) / len(history):.1f} days",
                    f"{sum(h.partial_settlements for h in history):,} Partial"
                )
            
            # History timeline
            st.subheader("Settlement Timeline")
            
            df = pd.DataFrame([{
                'Date': h.settlement_date.strftime('%Y-%m-%d'),
                'Total': h.total_settlements,
                'Completed': h.completed_settlements,
                'Partial': h.partial_settlements,
                'Pending': h.pending_settlements,
                'Amount Settled': h.total_settled,
                'Amount Pending': h.total_pending
            } for h in history])
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df['Amount Settled'],
                name='Amount Settled',
                line=dict(color='green')
            ))
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df['Amount Pending'],
                name='Amount Pending',
                line=dict(color='red')
            ))
            
            fig.update_layout(
                title="Daily Settlement Amounts",
                xaxis_title="Date",
                yaxis_title="Amount (₹)"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Status changes
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df['Completed'],
                name='Completed',
                line=dict(color='green')
            ))
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df['Partial'],
                name='Partial',
                line=dict(color='orange')
            ))
            fig.add_trace(go.Scatter(
                x=df['Date'],
                y=df['Pending'],
                name='Pending',
                line=dict(color='red')
            ))
            
            fig.update_layout(
                title="Daily Settlement Status Changes",
                xaxis_title="Date",
                yaxis_title="Number of Settlements"
            )
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.info("No settlement history found for the selected month.")
        
    except Exception as e:
        st.error(f"Error generating settlement history report: {str(e)}")

def export_cross_month_report(month: str) -> Optional[str]:
    """Export cross-month settlement analysis to CSV."""
    try:
        # Get analysis data
        current_analysis = analyze_settlements(db, month)
        prev_month = (datetime.strptime(month, '%Y-%m') - pd.DateOffset(months=1)).strftime('%Y-%m')
        prev_analysis = analyze_settlements(db, prev_month)
        
        # Create DataFrame
        data = {
            'Metric': [
                'Current Month Total Settlements',
                'Current Month Completed',
                'Current Month Partial',
                'Current Month Pending',
                'Current Month Amount Settled',
                'Current Month Amount Pending',
                'Previous Month Pending',
                'Completed from Previous',
                'Carried Forward',
                'Average Settlement Time'
            ],
            'Value': [
                current_analysis['total_settlements'],
                current_analysis['completed_settlements'],
                current_analysis['partial_settlements'],
                current_analysis['pending_settlements'],
                current_analysis['total_amount_settled'],
                current_analysis['total_amount_pending'],
                prev_analysis['pending_settlements'],
                prev_analysis['pending_settlements'] - current_analysis['pending_from_previous'],
                current_analysis['pending_from_previous'],
                current_analysis['avg_settlement_time']
            ]
        }
        
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
        
    except Exception as e:
        logger.error(f"Error exporting cross-month report: {str(e)}")
        return None

def export_pending_settlements_report(month: str) -> Optional[str]:
    """Export pending settlements report to CSV."""
    try:
        # Get pending settlements
        pending_settlements = Settlement.get_pending_settlements(db, month)
        
        # Create DataFrame
        data = []
        for s in pending_settlements:
            data.append({
                'Order ID': s.order_release_id,
                'Date': s.settlement_date.strftime('%Y-%m-%d'),
                'Expected Amount': s.order.final_amount,
                'Amount Settled': s.amount_settled,
                'Amount Pending': s.amount_pending,
                'Days Pending': (datetime.now().date() - s.settlement_date).days,
                'Status': s.settlement_status
            })
        
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
        
    except Exception as e:
        logger.error(f"Error exporting pending settlements report: {str(e)}")
        return None

def export_settlement_history_report(month: str) -> Optional[str]:
    """Export settlement history report to CSV."""
    try:
        # Get settlement history
        history = SettlementHistory.get_settlement_trends(db, month, month)
        
        # Create DataFrame
        data = []
        for h in history:
            data.append({
                'Date': h.settlement_date.strftime('%Y-%m-%d'),
                'Total Settlements': h.total_settlements,
                'Completed': h.completed_settlements,
                'Partial': h.partial_settlements,
                'Pending': h.pending_settlements,
                'Amount Settled': h.total_settled,
                'Amount Pending': h.total_pending
            })
        
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
        
    except Exception as e:
        logger.error(f"Error exporting settlement history report: {str(e)}")
        return None

def generate_cross_month_pdf(month: str) -> Optional[bytes]:
    """Generate PDF report for cross-month analysis."""
    try:
        # Get analysis data
        current_analysis = analyze_settlements(db, month)
        prev_month = (datetime.strptime(month, '%Y-%m') - pd.DateOffset(months=1)).strftime('%Y-%m')
        prev_analysis = analyze_settlements(db, prev_month)
        
        # Create PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer)
        elements = []
        
        # Title
        elements.append(Paragraph(f"Cross-Month Settlement Analysis Report - {month}", styles['Heading1']))
        elements.append(Spacer(1, 20))
        
        # Summary
        elements.append(Paragraph("Summary", styles['Heading2']))
        elements.append(Spacer(1, 10))
        
        summary_data = [
            ["Metric", "Value"],
            ["Current Month Total Settlements", str(current_analysis['total_settlements'])],
            ["Current Month Completed", str(current_analysis['completed_settlements'])],
            ["Current Month Partial", str(current_analysis['partial_settlements'])],
            ["Current Month Pending", str(current_analysis['pending_settlements'])],
            ["Current Month Amount Settled", f"₹{current_analysis['total_amount_settled']:,.2f}"],
            ["Current Month Amount Pending", f"₹{current_analysis['total_amount_pending']:,.2f}"],
            ["Previous Month Pending", str(prev_analysis['pending_settlements'])],
            ["Completed from Previous", str(prev_analysis['pending_settlements'] - current_analysis['pending_from_previous'])],
            ["Carried Forward", str(current_analysis['pending_from_previous'])],
            ["Average Settlement Time", f"{current_analysis['avg_settlement_time']:.1f} days"]
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(summary_table)
        
        # Build PDF
        doc.build(elements)
        return buffer.getvalue()
        
    except Exception as e:
        logger.error(f"Error generating cross-month PDF: {str(e)}")
        return None

def generate_pending_pdf(month: str) -> Optional[bytes]:
    """Generate PDF report for pending settlements."""
    try:
        # Get pending settlements
        pending_settlements = Settlement.get_pending_settlements(db, month)
        
        # Create PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer)
        elements = []
        
        # Title
        elements.append(Paragraph(f"Pending Settlements Report - {month}", styles['Heading1']))
        elements.append(Spacer(1, 20))
        
        # Summary
        elements.append(Paragraph("Summary", styles['Heading2']))
        elements.append(Spacer(1, 10))
        
        total_pending = sum(s.amount_pending for s in pending_settlements)
        avg_pending = total_pending / len(pending_settlements) if pending_settlements else 0
        avg_days = sum(
            (datetime.now().date() - s.settlement_date).days 
            for s in pending_settlements
        ) / len(pending_settlements) if pending_settlements else 0
        
        summary_data = [
            ["Metric", "Value"],
            ["Total Pending Settlements", str(len(pending_settlements))],
            ["Total Pending Amount", f"₹{total_pending:,.2f}"],
            ["Average Pending Amount", f"₹{avg_pending:,.2f}"],
            ["Average Days Pending", f"{avg_days:.1f} days"]
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(summary_table)
        
        # Details
        if pending_settlements:
            elements.append(Paragraph("Pending Settlements Details", styles['Heading2']))
            elements.append(Spacer(1, 10))
            
            details_data = [["Order ID", "Date", "Expected", "Settled", "Pending", "Days"]]
            for s in pending_settlements:
                details_data.append([
                    s.order_release_id,
                    s.settlement_date.strftime('%Y-%m-%d'),
                    f"₹{s.order.final_amount:,.2f}",
                    f"₹{s.amount_settled:,.2f}",
                    f"₹{s.amount_pending:,.2f}",
                    str((datetime.now().date() - s.settlement_date).days)
                ])
            
            details_table = Table(details_data, colWidths=[1.5*inch, 1*inch, 1.5*inch, 1.5*inch, 1.5*inch, 1*inch])
            details_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(details_table)
        
        # Build PDF
        doc.build(elements)
        return buffer.getvalue()
        
    except Exception as e:
        logger.error(f"Error generating pending PDF: {str(e)}")
        return None

def generate_history_pdf(month: str) -> Optional[bytes]:
    """Generate PDF report for settlement history."""
    try:
        # Get settlement history
        history = SettlementHistory.get_settlement_trends(db, month, month)
        
        # Create PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer)
        elements = []
        
        # Title
        elements.append(Paragraph(f"Settlement History Report - {month}", styles['Heading1']))
        elements.append(Spacer(1, 20))
        
        # Summary
        elements.append(Paragraph("Summary", styles['Heading2']))
        elements.append(Spacer(1, 10))
        
        total_records = len(history)
        total_settled = sum(h.total_settled for h in history)
        total_completed = sum(h.completed_settlements for h in history)
        total_partial = sum(h.partial_settlements for h in history)
        avg_days = sum(
            (datetime.now().date() - h.settlement_date).days 
            for h in history
        ) / len(history) if history else 0
        
        summary_data = [
            ["Metric", "Value"],
            ["Total History Records", str(total_records)],
            ["Total Amount Settled", f"₹{total_settled:,.2f}"],
            ["Total Completed", str(total_completed)],
            ["Total Partial", str(total_partial)],
            ["Average Settlement Time", f"{avg_days:.1f} days"]
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 12),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(summary_table)
        
        # Details
        if history:
            elements.append(Paragraph("Daily Settlement Details", styles['Heading2']))
            elements.append(Spacer(1, 10))
            
            details_data = [["Date", "Total", "Completed", "Partial", "Pending", "Amount Settled", "Amount Pending"]]
            for h in history:
                details_data.append([
                    h.settlement_date.strftime('%Y-%m-%d'),
                    str(h.total_settlements),
                    str(h.completed_settlements),
                    str(h.partial_settlements),
                    str(h.pending_settlements),
                    f"₹{h.total_settled:,.2f}",
                    f"₹{h.total_pending:,.2f}"
                ])
            
            details_table = Table(details_data, colWidths=[1*inch, 1*inch, 1*inch, 1*inch, 1*inch, 1.5*inch, 1.5*inch])
            details_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(details_table)
        
        # Build PDF
        doc.build(elements)
        return buffer.getvalue()
        
    except Exception as e:
        logger.error(f"Error generating history PDF: {str(e)}")
        return None

def main():
    """Main application entry point."""
    st.set_page_config(
        page_title="Reconciliation Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    tab = st.sidebar.radio(
        "Select Tab",
        options=["Dashboard", "Orders", "Returns", "Settlements", "Settlement Reports", "Data Quality"]
    )
    
    # Main content
    if tab == "Dashboard":
        dashboard_tab()
    elif tab == "Orders":
        orders_management_tab()
    elif tab == "Returns":
        returns_analysis_tab()
    elif tab == "Settlements":
        settlement_tab()
    elif tab == "Settlement Reports":
        settlement_reports_tab()
    elif tab == "Data Quality":
        data_quality_tab()

if __name__ == "__main__":
    main() 