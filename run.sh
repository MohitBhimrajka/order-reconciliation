#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_message() {
    echo -e "${2}${1}${NC}"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Python version
check_python_version() {
    if command_exists python3; then
        PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
        if (( $(echo "$PYTHON_VERSION < 3.8" | bc -l) )); then
            print_message "Python 3.8 or higher is required. Found version $PYTHON_VERSION" "$RED"
            exit 1
        fi
    else
        print_message "Python 3 is not installed" "$RED"
        exit 1
    fi
}

# Function to check if PostgreSQL is installed
check_postgres() {
    if ! command_exists psql; then
        print_message "PostgreSQL is not installed" "$RED"
        exit 1
    fi
}

# Function to check if Node.js is installed
check_nodejs() {
    if ! command_exists node; then
        print_message "Node.js is not installed" "$RED"
        exit 1
    fi
}

# Function to create and activate virtual environment
setup_venv() {
    if [ ! -d "venv" ]; then
        print_message "Creating virtual environment..." "$YELLOW"
        python3 -m venv venv
    fi
    source venv/bin/activate
}

# Function to install Python dependencies
install_python_deps() {
    print_message "Installing Python dependencies..." "$YELLOW"
    pip install --upgrade pip
    pip install -r requirements.txt
}

# Function to install Node.js dependencies
install_node_deps() {
    print_message "Installing Node.js dependencies..." "$YELLOW"
    cd frontend
    npm install
    cd ..
}

# Function to setup environment variables
setup_env() {
    print_message "Setting up environment variables..." "$YELLOW"
    
    # Create .env file if it doesn't exist
    if [ ! -f ".env" ]; then
        # Generate random password
        DB_PASSWORD=$(openssl rand -base64 12)
        
        # Create .env file
        cat > .env << EOL
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=reconciliation_db
DB_USER=reconciliation_user
DB_PASSWORD=$DB_PASSWORD

# Application Configuration
APP_ENV=development
APP_DEBUG=True
APP_SECRET_KEY=$(openssl rand -base64 32)

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Frontend Configuration
FRONTEND_PORT=3000
EOL
        
        print_message "Created .env file with generated credentials" "$GREEN"
    fi
}

# Function to setup database
setup_database() {
    print_message "Setting up database..." "$YELLOW"
    
    # Source environment variables
    source .env
    
    # Create database if it doesn't exist
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -c "CREATE DATABASE $DB_NAME"
    
    # Run database migrations
    python3 src/database/migrations.py
}

# Function to start the backend
start_backend() {
    print_message "Starting backend server..." "$YELLOW"
    cd src
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload &
    BACKEND_PID=$!
    cd ..
}

# Function to start the frontend
start_frontend() {
    print_message "Starting frontend development server..." "$YELLOW"
    cd frontend
    npm start &
    FRONTEND_PID=$!
    cd ..
}

# Function to handle cleanup on exit
cleanup() {
    print_message "\nShutting down servers..." "$YELLOW"
    kill $BACKEND_PID 2>/dev/null
    kill $FRONTEND_PID 2>/dev/null
    deactivate
    print_message "Cleanup complete" "$GREEN"
}

# Set up trap for cleanup
trap cleanup EXIT

# Main execution
print_message "Starting Reconciliation System Setup..." "$GREEN"

# Check prerequisites
check_python_version
check_postgres
check_nodejs

# Setup environment
setup_venv
install_python_deps
install_node_deps
setup_env
setup_database

# Start servers
start_backend
start_frontend

print_message "\nSetup complete! The application is now running:" "$GREEN"
print_message "Backend: http://localhost:8000" "$GREEN"
print_message "Frontend: http://localhost:3000" "$GREEN"
print_message "\nPress Ctrl+C to stop the servers" "$YELLOW"

# Wait for user input
wait 