# Reconciliation System

A comprehensive system for managing and reconciling orders, returns, and settlements.

## Architecture

The system follows a modern, DB-centric architecture with the following components:

### Backend (Python/FastAPI)
- **API Layer**: FastAPI-based REST API
- **Database Layer**: PostgreSQL with SQLAlchemy ORM
- **Processing Layer**: Efficient data processing with SQL queries
- **Reporting Layer**: Real-time reporting and analytics

### Frontend (React)
- **UI Layer**: Modern React components
- **State Management**: Redux for global state
- **API Integration**: Axios for API communication
- **Visualization**: Plotly for interactive charts

### Database Schema
- **Orders**: Core order information
- **Returns**: Return processing and tracking
- **Settlements**: Settlement management and reconciliation
- **Status History**: Order status tracking
- **Audit Logs**: System activity logging

## Prerequisites

- Python 3.8 or higher
- Node.js 14 or higher
- PostgreSQL 12 or higher
- Git

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd reconciliation
```

2. Run the setup script:
```bash
./run.sh
```

The script will:
- Check prerequisites
- Create and activate virtual environment
- Install Python dependencies
- Install Node.js dependencies
- Set up environment variables
- Initialize database
- Start backend and frontend servers

## Data Requirements

### Order Data
- Order ID (unique identifier)
- Customer information
- Order details (items, quantities, prices)
- Status information
- Timestamps

### Return Data
- Return ID (unique identifier)
- Associated Order ID
- Return reason
- Return status
- Settlement information

### Settlement Data
- Settlement ID (unique identifier)
- Associated Order/Return ID
- Settlement amount
- Settlement status
- Payment information

## Usage

1. **Starting the System**
```bash
./run.sh
```

2. **Accessing the Application**
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

3. **Data Processing**
- Upload data files through the web interface
- Monitor processing status in real-time
- View processing results and reports

4. **Reporting**
- Generate custom reports
- View analytics and visualizations
- Export data in various formats

## Testing

### Running Tests
```bash
# Activate virtual environment
source venv/bin/activate

# Run Python tests
pytest

# Run frontend tests
cd frontend
npm test
```

### Test Coverage
- Backend: 90%+ coverage
- Frontend: 80%+ coverage
- Critical paths: 100% coverage

## Development

### Backend Development
1. Activate virtual environment:
```bash
source venv/bin/activate
```

2. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

3. Run development server:
```bash
uvicorn src.api:app --reload
```

### Frontend Development
1. Navigate to frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

3. Run development server:
```bash
npm start
```

## Deployment

### Production Setup
1. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with production values
```

2. Build frontend:
```bash
cd frontend
npm run build
```

3. Start production servers:
```bash
# Backend
gunicorn src.api:app -w 4 -k uvicorn.workers.UvicornWorker

# Frontend (using nginx)
sudo systemctl start nginx
```

## Troubleshooting

### Common Issues
1. **Database Connection**
   - Check PostgreSQL service status
   - Verify database credentials in .env
   - Ensure database exists

2. **API Errors**
   - Check API logs
   - Verify environment variables
   - Check network connectivity

3. **Frontend Issues**
   - Clear browser cache
   - Check console for errors
   - Verify API endpoints

### Logs
- Backend logs: `logs/backend.log`
- Frontend logs: `logs/frontend.log`
- Database logs: Check PostgreSQL logs

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 