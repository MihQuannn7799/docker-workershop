# Homework: Questions and Solutions

## Question 1
Run docker with the `python:3.13` image. Use an entrypoint bash to interact with the container.
```bash
docker run -it -- rm --entrypoint=bash python:3.13

#  pip version
pip --version
```

## Question 2
Given the following docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

```bash
docker compose up
```

```
host = db
port = 5433
```

Note: Script for ingesting green taxi and zone data set is in the `ingest_data.py` file.
Run the following command in the terminal to run the script:
```bash
# TO ingest green taxi data set (It has default value)
 uv run ingest.py --target_table 

 # To ingest taxi zone data set
 uv run ingest.py --target_table taxi_zones --data_set taxi_zones
```

## Question 3
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

```sql
SELECT COUNT(*)
FROM yellow_tripdata
WHERE 
	trip_distance <= 1
WHERE tpep_pickup_datetime >= '2019-01-01'
  AND tpep_pickup_datetime <  '2019-02-01';
``` 

## Question 4
Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

```sql
SELECT
    DATE(pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_trip_distance
FROM trips
WHERE trip_distance < 100
GROUP BY DATE(pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;
```

## Question 5 
Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

```sql
SELECT
    z_drop."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS max_tip
FROM green_taxi_trips g
JOIN zones z_pick
    ON g."PULocationID" = z_pick."LocationID"
JOIN zones z_drop
    ON g."DOLocationID" = z_drop."LocationID"
WHERE z_pick."Zone" = 'East Harlem North'
  AND g.lpep_pickup_datetime >= '2025-11-01'
  AND g.lpep_pickup_datetime <  '2025-12-01'
GROUP BY z_drop."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

## Question 6
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

```sql
SELECT
    z_drop."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS max_tip
FROM green_taxi_trips g
JOIN zones z_pick
    ON g."PULocationID" = z_pick."LocationID"
JOIN zones z_drop
    ON g."DOLocationID" = z_drop."LocationID"
WHERE z_pick."Zone" = 'East Harlem North'
  AND g.lpep_pickup_datetime >= '2025-11-01'
  AND g.lpep_pickup_datetime <  '2025-12-01'
GROUP BY z_drop."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

## Question 7 
Which of the following sequences, respectively, describes the workflow for:

1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

```bash
terraform init
terraform plan
terraform apply --auto-approve
terraform destroy
```
