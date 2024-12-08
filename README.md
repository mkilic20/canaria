# Job Data Scraping Pipeline

A robust data pipeline that extracts job listings from JSON files, processes them through multiple stages, and stores them in PostgreSQL and MongoDB databases with Redis caching. The entire system is containerized using Docker for easy deployment and scalability.

## Project Overview

This pipeline processes job listing data through several stages:
- Extracts data from JSON files using Scrapy
- Processes and validates job information including salary calculations
- Stores data in PostgreSQL (primary database)
- Uses Redis for caching to prevent duplicate processing
- Optional storage in MongoDB (secondary database)
- Exports processed data to CSV files

## Project Structure
├── docker-compose.yaml # Docker services configuration
├── dockerfile # Scrapy service container definition
├── infra/ # Infrastructure management
│ ├── postgresql_connector.py
│ └── redis_connector.py
├── jobs_project/ # Main Scrapy project
│ ├── jobs_project/
│ │ ├── items.py # Data structure definitions
│ │ ├── pipelines.py # Data processing pipelines
│ │ ├── settings.py # Project configuration
│ │ └── spiders/
│ │ └── json_spider.py
│ └── scrapy.cfg
├── query.py # Database query utilities
├── requirements.txt # Project dependencies
└── .env # Environment variables
```

## Prerequisites

- Docker
- Docker Compose

## Setup Instructions

1. Clone the repository:

```bash
git clone <repository-url>
cd job-scraping-pipeline
```

2. Build and start the services:

```bash
docker-compose build
docker-compose up -d
```

3. Run the scraper:

```bash
docker-compose exec scraper scrapy crawl json_spider
```

4. Export data to CSV (optional):

```bash
docker-compose exec scraper python /app/query.py
```

## Database Configuration

### PostgreSQL
- Database: jobs_db
- User: user
- Password: password
- Port: 5432

### MongoDB
- Database: jobs_db
- Username: root
- Password: example
- Port: 27017

### Redis
- Port: 6379
- No authentication required

## Data Pipeline Process

1. **Data Extraction**
   - Spider reads JSON files from the data directory
   - Parses job listings and creates structured items
   - Generates unique IDs for each job listing

2. **Data Processing**
   - Validates and transforms job data
   - Processes salary information (converts hourly rates to annual)
   - Normalizes location data (city and zipcode)

3. **Storage Layer**
   - Primary storage in PostgreSQL
   - Redis caching for deduplication
   - Optional MongoDB storage
   - CSV export functionality

## Monitoring and Logging

- All logs are stored in `/app/logs/spider.log`
- Detailed logging of:
  - Pipeline operations
  - Data processing steps
  - Success/failure tracking
  - Salary calculations
  - Database operations

## Database Schema

### PostgreSQL Table Structure

```sql
CREATE TABLE jobs (
    _id VARCHAR(36) PRIMARY KEY,
    companyName VARCHAR(255),
    correctDate TIMESTAMP,
    jobKey VARCHAR(255),
    jobPageUrl TEXT,
    annualSalaryAvg DECIMAL(12,2),
    city VARCHAR(100),
    zipcode INTEGER
);
```

## Troubleshooting

1. **Connection Issues**
   - Check container status: `docker-compose ps`
   - View service logs: `docker-compose logs [service_name]`
   - Verify network connectivity between containers

2. **Data Processing Issues**
   - Check spider logs in `/app/logs/spider.log`
   - Verify JSON file placement in `/app/data/`
   - Ensure database connections are properly configured

## Technologies Used

- Python 3.11
- Scrapy 2.12.0
- PostgreSQL 15
- MongoDB 6
- Redis 7
- Docker & Docker Compose

## Performance

Based on the logs, the system demonstrates:
- Processing speed: ~200 items in under 1 second
- Successful salary parsing for both hourly and annual rates
- Efficient data deduplication using Redis
- Reliable data storage across multiple databases

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request
