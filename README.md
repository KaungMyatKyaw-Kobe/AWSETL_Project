# AWS ETL Pipeline with Apache Airflow 3

> This project is from **Ansh Lamba JSR's _Apache Airflow 3 - The complete bootcamp_**.

An end-to-end ETL pipeline orchestrated by Apache Airflow 3. It follows the Medallion Architecture (Bronze → Silver → Gold). It pulls flight data (airports, bookings, passengers) from CSV files hosted on GitHub, stores the raw files in Amazon S3, transforms them with AWS Glue (Spark), catalogs them with a Glue Crawler, and builds the Gold layer with a Databricks job.

## Architecture

```
GitHub CSVs (airports, bookings, passengers)
        │
        ▼
[extract_load_s3]              Bronze: raw CSVs → s3://<bucket>/bronze/<YYYY-MM-DD>/
        │
        ├──► [transform_load_s3]            Silver: Glue Spark job "sliver_layer"
        └──► [transform_load_s3_parquet]    Silver: Glue Spark job "sliver_layer_athena" (Parquet for Athena)
                        │
                        ▼
              [trigger_crawler]             Glue Crawler "sliver_crawler" updates the Glue Data Catalog
                        │
                        ▼
              [trigger_databricks_job]      Gold: Databricks job builds the Gold layer
```

The DAG `aws_etl` runs `@daily`. The load date of the Bronze folder is passed to the Glue jobs through XCom as the `--load_date` argument. Each task retries up to 3 times.

## Technologies

| Area | Technology |
|---|---|
| Orchestration | Apache Airflow 3.2.0 (TaskFlow API, CeleryExecutor) |
| Containerization | Docker and Docker Compose |
| Airflow metadata DB / broker | PostgreSQL 16, Redis 7.2 |
| Language | Python (pandas, requests, python-dotenv) |
| Cloud storage | Amazon S3 |
| Transformation | AWS Glue (PySpark jobs, Delta Lake) |
| Catalog / querying | AWS Glue Crawler, Glue Data Catalog, Amazon Athena |
| Gold layer | Databricks (Databricks SDK for Python, Unity Catalog notebooks) |
| AWS SDK | boto3 |
| Access control | AWS IAM (users, roles, inline policies) |

## Project Structure

```
AWSETL_Project/
├── dags/
│   └── aws_etl.py            # Airflow DAG definition
├── utils/
│   ├── bronze_layer.py       # Fetch CSVs from the source and upload them to S3
│   ├── sliver_layer.py       # Trigger and poll the Glue Spark jobs and the Glue Crawler
│   └── gold_layer.py         # Trigger the Databricks job
├── note/
│   ├── aws_iam_inline_policies/   # IAM policies used for the user, Glue and the Spark role
│   ├── aws_spark_scripts/         # Glue Spark job scripts and Spark config
│   └── databricks notebook/       # Databricks catalog setup and Gold layer notebooks
├── config/airflow.cfg
├── plugins/
├── data/
├── Dockerfile                # Extends apache/airflow:3.2.0 with requirements.txt
├── docker-compose.yaml
└── requirements.txt
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose. Give Docker at least 4 GB of RAM.
- An **AWS account** with:
  - An S3 bucket. The code uses `kobe-airflow-bucket`; change it to your own bucket name in `dags/aws_etl.py`.
  - An IAM user with an access key and the permissions in `note/aws_iam_inline_policies/`.
  - An IAM role for Glue with S3 access (see `Permission For Role sparkaccess.txt`).
  - Two Glue Spark jobs named `sliver_layer` and `sliver_layer_athena`. Create them from the scripts in `note/aws_spark_scripts/` and apply the job parameters in `spark_config.txt`.
  - A Glue Crawler named `sliver_crawler` that points at the Silver layer output.
- A **Databricks** workspace (the free edition works) with:
  - The catalog created from `note/databricks notebook/Create Catalog.ipynb`.
  - A job that runs `Gold_Layer.ipynb`. Copy its Job ID into `dags/aws_etl.py`.
  - A Personal Access Token (PAT).

> **Important:** The AWS services and Databricks services must be in the same region.
>
> The AWS region is hard-coded as `ap-southeast-2` in `utils/`. If your resources are in another region, change it there.

## How to Run

### 1. Clone the project

```bash
git clone <your-repo-url>
cd AWSETL_Project
```

### 2. Create the `.env` file

Create a `.env` file in the project root:

```env
AIRFLOW_UID=50000

# AWS
aws_access_key_id=<your-aws-access-key-id>
aws_secret_access_key=<your-aws-secret-access-key>

# Databricks
Databricks_HOST=https://<your-workspace>.cloud.databricks.com
Databricks_PAT=<your-databricks-personal-access-token>
```

> `.env` is listed in `.gitignore`. Never commit your credentials.

On Linux, set `AIRFLOW_UID` to your user ID (`id -u`) so that files in `logs/` get the right owner.

### 3. Create the folders Airflow mounts

```bash
mkdir -p dags logs plugins config data
```

### 4. Build the image and initialize Airflow

```bash
docker compose build
docker compose up airflow-init
```

This builds the custom image (Airflow 3.2.0 plus `requirements.txt`), migrates the metadata database and creates the admin user.

### 5. Start all services

```bash
docker compose up -d
```

Check that every container is `healthy`:

```bash
docker compose ps
```

### 6. Open the Airflow UI

Go to **http://localhost:8080** and log in:

- Username: `airflow`
- Password: `airflow`

### 7. Run the pipeline

1. Find the **`aws_etl`** DAG. New DAGs start paused.
2. Unpause it with the toggle.
3. Click **Trigger** to run it now, or wait for the daily schedule.
4. Watch the task progress in the Grid or Graph view. Each task's logs show the Glue job and crawler status updates.

### Optional: Celery Flower

```bash
docker compose --profile flower up -d
```

Flower is available at **http://localhost:5555**.

## Stopping and Cleaning Up

```bash
# Stop the containers
docker compose down

# Stop the containers and delete the database volume (full reset)
docker compose down --volumes --remove-orphans
```

## Testing Layers Individually

Each module in `utils/` has a `__main__` block, so you can test a layer outside Airflow:

```bash
pip install -r requirements.txt apache-airflow-providers-amazon
python -m utils.bronze_layer
python -m utils.sliver_layer
python -m utils.gold_layer
```

## Credits

This project is from **Ansh Lamba JSR's _Apache Airflow 3 - The complete bootcamp_**.
