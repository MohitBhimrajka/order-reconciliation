from pydantic import BaseModel, validator, Field
from datetime import datetime
from typing import Optional, List
from decimal import Decimal

class OrderValidator(BaseModel):
    order_release_id: str
    order_line_id: str
    seller_order_id: Optional[str] = None
    created_on: datetime
    delivered_on: Optional[datetime] = None
    cancelled_on: Optional[datetime] = None
    final_amount: Decimal = Field(ge=0)
    total_mrp: Decimal = Field(ge=0)
    discount: Decimal = Field(ge=0)
    shipping_charge: Decimal = Field(ge=0)
    order_status: str
    payment_type: str
    city: Optional[str] = None
    state: Optional[str] = None
    zipcode: Optional[str] = None

    @validator('payment_type')
    def validate_payment_type(cls, v):
        if v not in ['prepaid', 'postpaid']:
            raise ValueError('payment_type must be either prepaid or postpaid')
        return v

    @validator('order_status')
    def validate_order_status(cls, v):
        valid_statuses = ['C', 'P', 'X', 'D', 'R']  # Add all valid statuses
        if v not in valid_statuses:
            raise ValueError(f'order_status must be one of {valid_statuses}')
        return v

class ReturnValidator(BaseModel):
    order_release_id: str
    order_line_id: str
    return_type: str
    return_date: datetime
    packing_date: Optional[datetime] = None
    delivery_date: Optional[datetime] = None
    customer_paid_amount: Decimal = Field(ge=0)
    prepaid_amount: Decimal = Field(ge=0)
    postpaid_amount: Decimal = Field(ge=0)
    mrp: Decimal = Field(ge=0)
    total_discount_amount: Decimal = Field(ge=0)
    total_settlement: Decimal
    total_actual_settlement: Decimal
    amount_pending_settlement: Decimal = Field(ge=0)

    @validator('return_type')
    def validate_return_type(cls, v):
        if v not in ['return_refund', 'exchange']:
            raise ValueError('return_type must be either return_refund or exchange')
        return v

    @validator('total_actual_settlement')
    def validate_settlement_amount(cls, v, values):
        if 'return_type' in values and values['return_type'] == 'return_refund':
            if v > 0:
                raise ValueError('return_refund settlements should be negative')
        return v

class SettlementValidator(BaseModel):
    order_release_id: str
    order_line_id: str
    total_expected_settlement: Decimal
    total_actual_settlement: Decimal
    amount_pending_settlement: Decimal = Field(ge=0)
    prepaid_commission_deduction: Decimal = Field(ge=0)
    prepaid_logistics_deduction: Decimal = Field(ge=0)
    prepaid_payment: Decimal
    postpaid_commission_deduction: Decimal = Field(ge=0)
    postpaid_logistics_deduction: Decimal = Field(ge=0)
    postpaid_payment: Decimal
    settlement_status: str

    @validator('settlement_status')
    def validate_settlement_status(cls, v):
        if v not in ['completed', 'partial', 'pending']:
            raise ValueError('settlement_status must be one of: completed, partial, pending')
        return v

    @validator('amount_pending_settlement')
    def validate_pending_amount(cls, v, values):
        if 'total_expected_settlement' in values:
            if v > values['total_expected_settlement']:
                raise ValueError('pending_amount cannot be greater than expected_settlement')
        return v

class SettlementDateValidator(BaseModel):
    settlement_id: int
    settlement_date: datetime
    settlement_amount: Decimal
    bank_utr_no: Optional[str] = None

    @validator('settlement_amount')
    def validate_settlement_amount(cls, v):
        if v <= 0:
            raise ValueError('settlement_amount must be positive')
        return v

class MonthlyReconciliationValidator(BaseModel):
    month: datetime
    total_orders: int = Field(ge=0)
    total_returns: int = Field(ge=0)
    total_settlements: Decimal = Field(ge=0)
    pending_settlements: Decimal = Field(ge=0)
    completed_settlements: Decimal = Field(ge=0)
    return_losses: Decimal = Field(ge=0)
    net_profit: Decimal

    @validator('net_profit')
    def validate_net_profit(cls, v, values):
        if 'total_settlements' in values and 'return_losses' in values:
            expected_profit = values['total_settlements'] - values['return_losses']
            if v != expected_profit:
                raise ValueError('net_profit must equal total_settlements - return_losses')
        return v

    @validator('total_returns')
    def validate_returns(cls, v, values):
        if 'total_orders' in values and v > values['total_orders']:
            raise ValueError('total_returns cannot be greater than total_orders')
        return v 