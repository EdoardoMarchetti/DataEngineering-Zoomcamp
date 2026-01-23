# NYC Taxi Data Homework - Setup Guide

This guide explains how to set up the Docker environment, upload data to PostgreSQL, and connect using pgAdmin.

## Prerequisites

- Docker and Docker Compose installed
- Internet connection (scripts download data from URLs)

## Step 1: Start Docker Containers

Start the PostgreSQL and pgAdmin containers using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- **PostgreSQL** on port `5433` (mapped from container port 5432)
- **pgAdmin** on port `8080`

To verify containers are running:

```bash
docker-compose ps
```

To view logs:

```bash
docker-compose logs -f
```

To stop containers:

```bash
docker-compose down
```

## Step 2: Build Docker Image for Data Ingestion

Build the Docker image that contains the Python ingestion scripts:

```bash
docker build -t trip-ingestion .
```

## Step 3: Upload Data to PostgreSQL

### Upload Zone Data

First, upload the taxi zone lookup data:

```bash
docker run --rm \
  --network homework_default \
  trip-ingestion zone_ingestion.py \
  --target-table zones \
  --pg-host db \
  --pg-port 5432 \
  --pg-user postgres \
  --pg-pass postgres \
  --pg-db ny_taxi
```

**Note:** Replace `homework_default` with your actual Docker network name. You can find it with:
```bash
docker network ls
```

### Upload Trip Data

Upload the green taxi trips data:

```bash
docker run --rm \
  --network homework_default \
  trip-ingestion trip_ingestion.py \
  --taxi-type green \
  --year 2025 \
  --month 11 \
  --target-table green_taxi_data \
  --pg-host db \
  --pg-port 5432 \
  --pg-user postgres \
  --pg-pass postgres \
  --pg-db ny_taxi
```

**Note:** The scripts automatically download data from URLs, so no manual download is required.

## Step 4: Connect to pgAdmin

1. Open your web browser and navigate to:
   ```
   http://localhost:8080
   ```

2. Login with:
   - **Email:** `pgadmin@pgadmin.com`
   - **Password:** `pgadmin`

3. Add a new PostgreSQL server:
   - Right-click on "Servers" → "Register" → "Server"
   - In the **General** tab:
     - Name: `NYC Taxi Database` (or any name you prefer)
   - In the **Connection** tab:
     - **Host name/address:** `postgres` (or `db` - the service name from docker-compose)
     - **Port:** `5432` (internal container port, not 5433)
     - **Maintenance database:** `ny_taxi`
     - **Username:** `postgres`
     - **Password:** `postgres`
     - Check "Save password" if desired
   - Click "Save"

4. You should now see the `ny_taxi` database with your tables:
   - `zones` - Taxi zone lookup data
   - `green_taxi_data` - Green taxi trip data

## Verify Data

You can verify the data was loaded correctly by connecting to PostgreSQL:

```bash
# Using psql (if installed)
psql -h localhost -p 5433 -U postgres -d ny_taxi

# Or using pgcli (if installed)
pgcli -h localhost -p 5433 -U postgres -d ny_taxi
```

Then run:

```sql
-- Check zone data
SELECT COUNT(*) FROM zones;

-- Check trip data
SELECT COUNT(*) FROM green_taxi_data;

-- Sample queries
SELECT * FROM zones LIMIT 5;
SELECT * FROM green_taxi_data LIMIT 5;
```

## Troubleshooting

### Containers not starting
- Check if ports 5433 and 8080 are already in use
- View logs: `docker-compose logs`

### Connection issues in pgAdmin
- Make sure you're using `postgres` or `db` as hostname (not `localhost`)
- Use port `5432` (internal container port), not `5433`
- Verify containers are on the same network: `docker network inspect homework_default`

### Data ingestion fails
- Verify containers are running: `docker-compose ps`
- Check network connectivity: `docker network ls`
- Review container logs: `docker-compose logs db`

## Database Credentials Summary

- **Host (from host machine):** `localhost`
- **Host (from Docker containers):** `postgres` or `db`
- **Port (from host):** `5433`
- **Port (from containers):** `5432`
- **Database:** `ny_taxi`
- **Username:** `postgres`
- **Password:** `postgres`
