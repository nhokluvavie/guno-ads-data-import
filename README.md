# Guna Ads Data Import

Meta Ads Data Import Pipeline for PostgreSQL - A robust, scalable Java application for fetching and processing Meta Ads API data.

## Overview

This application automatically fetches advertising data from Meta Ads API and stores it in PostgreSQL database following a normalized schema. It supports hierarchical data (Account → Campaign → AdSet → Ad → Placement) and performance metrics with demographic breakdowns.

## Features

- **Meta Ads Integration**: Complete Meta Marketing API v19.0 integration
- **Automated Scheduling**: Daily performance data and weekly hierarchy sync
- **Batch Processing**: Optimized bulk data operations
- **Multi-Environment**: Test and Production profiles
- **Rate Limiting**: Meta API rate limit compliance
- **Error Handling**: Retry mechanisms and comprehensive logging
- **Health Monitoring**: Built-in health checks and metrics

## Tech Stack

- **Java 17**
- **Spring Boot 3.1.5**
- **PostgreSQL** with HikariCP connection pooling
- **Quartz Scheduler** for job management
- **Meta Business SDK 19.0.0**
- **Maven** for build management

## Database Schema

The application works with 7 main tables:
- `tbl_account` - Meta Ad Account information
- `tbl_campaign` - Campaign data
- `tbl_adset` - AdSet configuration
- `tbl_advertisement` - Individual ads
- `tbl_placement` - Ad placements
- `tbl_ads_reporting` - Performance metrics (main data table)
- `tbl_ads_processing_date` - Date dimension

## Quick Start

### Prerequisites

1. **Java 17+** installed
2. **PostgreSQL 13+** with schema from `gunoads.ddl`
3. **Meta Developer Account** with app credentials

### Setup

1. **Clone and configure**:
   ```bash
   git clone <repository-url>
   cd guna-ads-data-import
   cp .env.example .env
   ```

2. **Configure environment variables in `.env`**:
   ```bash
   # Database
   DB_URL=jdbc:postgresql://localhost:5432/gunads
   DB_USERNAME=your_username
   DB_PASSWORD=your_password
   
   # Meta API
   META_APP_ID=your_app_id
   META_APP_SECRET=your_app_secret
   META_ACCESS_TOKEN=your_access_token
   ```

3. **Run application**:
   ```bash
   # Test environment (default)
   mvn spring-boot:run
   
   # Production environment
   mvn spring-boot:run -Dspring.profiles.active=prod
   ```

## Configuration

### Profiles

- **test** (default): Development/testing with debug logging and smaller batch sizes
- **prod**: Production optimized with larger connection pools and performance logging

### Key Settings

| Setting | Description | Default |
|---------|-------------|---------|
| `meta.ads.rate-limit.requests-per-hour` | API rate limit | 200 |
| `scheduler.daily-job-cron` | Daily data fetch schedule | `0 0 2 * * ?` (2 AM) |
| `batch.size` | Database batch size | 1000 |
| `spring.datasource.hikari.maximum-pool-size` | DB connection pool | 10 (test), 20 (prod) |

## Data Pipeline

### Daily Operations
1. **Performance Data Sync** (2:00 AM daily)
    - Fetch previous day's insights data
    - Process with demographic breakdowns
    - Store in `tbl_ads_reporting`

### Weekly Operations
2. **Hierarchy Sync** (1:00 AM Sunday)
    - Update account structure
    - Sync campaigns, adsets, ads
    - Extract placement information

## Monitoring

### Health Checks
- Application health: `http://localhost:8080/actuator/health`
- Metrics: `http://localhost:8080/actuator/metrics`

### Logging
- **Test**: Console + file (`logs/guna-ads-test.log`)
- **Production**: File only (`logs/guna-ads-prod.log` + `logs/guna-ads-error.log`)

## Development

### Project Structure
```
src/main/java/com/gunads/
├── config/          # Configuration classes
├── connector/       # Meta API integration
├── model/          
│   ├── entity/     # Database entities
│   ├── dto/        # API response DTOs
│   └── enums/      # Enums and constants
├── dao/            # Data access layer
├── service/        # Business logic
├── scheduler/      # Job scheduling
├── processor/      # Data transformation
├── util/           # Utilities
└── exception/      # Custom exceptions
```

### Building
```bash
# Run tests
mvn test

# Build JAR
mvn clean package

# Run with specific profile
mvn spring-boot:run -Dspring.profiles.active=prod
```

## Performance

- **Throughput**: 1M+ records/hour
- **Memory**: < 2GB normal operation
- **Uptime Target**: 99.9%
- **API Compliance**: < 200 requests/hour

## Error Handling

- **Retry Logic**: 3 attempts with exponential backoff
- **Circuit Breaker**: API failure protection
- **Dead Letter**: Failed records logging
- **Alerts**: Critical error notifications

## Security

- **Environment Variables**: Sensitive data in `.env`
- **Connection Pooling**: Secure DB connections
- **API Token**: Secure Meta API authentication

## License

Internal Use Only

---

**Version**: 1.0.0  
**Last Updated**: September 2025