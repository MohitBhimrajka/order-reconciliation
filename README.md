# Reconciliation Application

This application helps track and reconcile orders, returns, and settlements for an e-commerce business. It processes CSV files containing order, return, and settlement data, stores the information in a database, and provides an API to query and analyze the data.

## Features

- Process orders, returns, and settlement data from CSV files
- Track monthly reconciliation metrics
- Calculate return losses and net profit
- Monitor pending settlements
- Provide REST API endpoints for data access

## Setup

1. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with the following content:
```
DATABASE_URL=sqlite:///./reconciliation.db
```

4. Place your CSV files in the `data` directory with the following naming convention:
- `orders-MM-YYYY.csv`
- `returns-MM-YYYY.csv`
- `settlement-MM-YYYY.csv`

## Usage

1. Process data files and populate the database:
```bash
python scripts/process_data.py
```

2. Start the API server:
```bash
python scripts/run_api.py
```

3. Access the API documentation at:
```
http://localhost:8000/docs
```

## API Endpoints

### Orders
- `GET /orders/` - Get all orders with optional filters
  - Query parameters:
    - `start_date`: Filter orders from this date
    - `end_date`: Filter orders until this date
    - `payment_type`: Filter by payment type (prepaid/postpaid)

### Returns
- `GET /returns/` - Get all returns with optional filters
  - Query parameters:
    - `start_date`: Filter returns from this date
    - `end_date`: Filter returns until this date
    - `return_type`: Filter by return type (return_refund/exchange)

### Settlements
- `GET /settlements/` - Get all settlements with optional filters
  - Query parameters:
    - `start_date`: Filter settlements from this date
    - `end_date`: Filter settlements until this date
    - `status`: Filter by settlement status (completed/partial/pending)

### Monthly Reconciliation
- `GET /monthly-reconciliation/` - Get monthly reconciliation data
  - Query parameters:
    - `start_month`: Filter from this month
    - `end_month`: Filter until this month

### Reconciliation Summary
- `GET /reconciliation-summary/` - Get detailed reconciliation summary for a specific month
  - Query parameters:
    - `month`: The month to get summary for (defaults to current month)

## Data Structure

### Orders
- Tracks order details including:
  - Order ID and line items
  - Creation and delivery dates
  - Amounts (final, MRP, discount, shipping)
  - Payment type
  - Location information

### Returns
- Tracks return details including:
  - Return type (refund/exchange)
  - Dates (return, packing, delivery)
  - Amounts (customer paid, settlement)
  - Payment split (prepaid/postpaid)

### Settlements
- Tracks settlement details including:
  - Expected and actual settlement amounts
  - Pending amounts
  - Commission and logistics deductions
  - Payment splits
  - Settlement status

### Monthly Reconciliation
- Aggregates monthly metrics including:
  - Total orders and returns
  - Settlement totals
  - Return losses
  - Net profit

## Error Handling

The application includes comprehensive error handling:
- Input validation
- Database transaction management
- Logging of errors and operations
- Graceful error responses in the API

## Logging

Logs are written to `reconciliation.log` in the project root directory. The log level is set to INFO by default.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 