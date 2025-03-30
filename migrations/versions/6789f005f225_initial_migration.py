"""initial migration

Revision ID: 6789f005f225
Revises: 
Create Date: 2025-03-30 21:47:55.650128

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime


# revision identifiers, used by Alembic.
revision = '6789f005f225'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create orders table
    op.create_table(
        'orders',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('order_release_id', sa.String(), nullable=False),
        sa.Column('order_line_id', sa.String(), nullable=False),
        sa.Column('seller_order_id', sa.String(), nullable=True),
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('delivered_on', sa.DateTime(), nullable=True),
        sa.Column('cancelled_on', sa.DateTime(), nullable=True),
        sa.Column('final_amount', sa.Float(), nullable=False),
        sa.Column('total_mrp', sa.Float(), nullable=False),
        sa.Column('discount', sa.Float(), nullable=False),
        sa.Column('shipping_charge', sa.Float(), nullable=False),
        sa.Column('order_status', sa.String(), nullable=False),
        sa.Column('payment_type', sa.String(), nullable=False),  # prepaid/postpaid
        sa.Column('city', sa.String(), nullable=True),
        sa.Column('state', sa.String(), nullable=True),
        sa.Column('zipcode', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), default=datetime.utcnow),
        sa.Column('updated_at', sa.DateTime(), default=datetime.utcnow, onupdate=datetime.utcnow),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('order_release_id')
    )

    # Create returns table
    op.create_table(
        'returns',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('order_release_id', sa.String(), nullable=False),
        sa.Column('order_line_id', sa.String(), nullable=False),
        sa.Column('return_type', sa.String(), nullable=False),  # return_refund/exchange
        sa.Column('return_date', sa.DateTime(), nullable=False),
        sa.Column('packing_date', sa.DateTime(), nullable=True),
        sa.Column('delivery_date', sa.DateTime(), nullable=True),
        sa.Column('customer_paid_amount', sa.Float(), nullable=False),
        sa.Column('prepaid_amount', sa.Float(), nullable=False),
        sa.Column('postpaid_amount', sa.Float(), nullable=False),
        sa.Column('mrp', sa.Float(), nullable=False),
        sa.Column('total_discount_amount', sa.Float(), nullable=False),
        sa.Column('total_settlement', sa.Float(), nullable=False),
        sa.Column('total_actual_settlement', sa.Float(), nullable=False),
        sa.Column('amount_pending_settlement', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(), default=datetime.utcnow),
        sa.Column('updated_at', sa.DateTime(), default=datetime.utcnow, onupdate=datetime.utcnow),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['order_release_id'], ['orders.order_release_id'])
    )

    # Create settlements table
    op.create_table(
        'settlements',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('order_release_id', sa.String(), nullable=False),
        sa.Column('order_line_id', sa.String(), nullable=False),
        sa.Column('total_expected_settlement', sa.Float(), nullable=False),
        sa.Column('total_actual_settlement', sa.Float(), nullable=False),
        sa.Column('amount_pending_settlement', sa.Float(), nullable=False),
        sa.Column('prepaid_commission_deduction', sa.Float(), nullable=False),
        sa.Column('prepaid_logistics_deduction', sa.Float(), nullable=False),
        sa.Column('prepaid_payment', sa.Float(), nullable=False),
        sa.Column('postpaid_commission_deduction', sa.Float(), nullable=False),
        sa.Column('postpaid_logistics_deduction', sa.Float(), nullable=False),
        sa.Column('postpaid_payment', sa.Float(), nullable=False),
        sa.Column('settlement_status', sa.String(), nullable=False),  # completed/partial/pending
        sa.Column('created_at', sa.DateTime(), default=datetime.utcnow),
        sa.Column('updated_at', sa.DateTime(), default=datetime.utcnow, onupdate=datetime.utcnow),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['order_release_id'], ['orders.order_release_id'])
    )

    # Create settlement_dates table for tracking daily settlements
    op.create_table(
        'settlement_dates',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('settlement_id', sa.Integer(), nullable=False),
        sa.Column('settlement_date', sa.DateTime(), nullable=False),
        sa.Column('settlement_amount', sa.Float(), nullable=False),
        sa.Column('bank_utr_no', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), default=datetime.utcnow),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['settlement_id'], ['settlements.id'])
    )

    # Create monthly_reconciliation table
    op.create_table(
        'monthly_reconciliation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('month', sa.Date(), nullable=False),
        sa.Column('total_orders', sa.Integer(), nullable=False),
        sa.Column('total_returns', sa.Integer(), nullable=False),
        sa.Column('total_settlements', sa.Float(), nullable=False),
        sa.Column('pending_settlements', sa.Float(), nullable=False),
        sa.Column('completed_settlements', sa.Float(), nullable=False),
        sa.Column('return_losses', sa.Float(), nullable=False),
        sa.Column('net_profit', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(), default=datetime.utcnow),
        sa.Column('updated_at', sa.DateTime(), default=datetime.utcnow, onupdate=datetime.utcnow),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('month')
    )


def downgrade() -> None:
    op.drop_table('monthly_reconciliation')
    op.drop_table('settlement_dates')
    op.drop_table('settlements')
    op.drop_table('returns')
    op.drop_table('orders') 