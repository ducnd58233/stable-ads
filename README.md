# Stable Ads

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Commands](#commands)
- [Access Services & Dashboards](#access-services--dashboards)
- [Folder Structure](#folder-structure)

## Prerequisites

Before setting up the project, ensure you have the following installed:

- **Conda** (Miniconda or Anaconda) - For Python environment management
- **Docker** and **Docker Compose** - For running infrastructure services (PostgreSQL, MinIO, Kafka, Ollama, Airflow)
- **Python 3.13** - Required Python version
- **CUDA Toolkit** - For GPU acceleration (required for PyTorch with CUDA support)
- **uv** - Python package manager (installed via conda environment)

## Setup

### Create Environment

conda env create -f environment.yml### Activate Environment

conda activate stable-ads### Configure Environment Variables

cp .env.example .env

### Start Infrastructure Services
h
docker compose -f deployments/docker/local/docker-compose.yml --env-file .env up -dThis will start the following services:
- PostgreSQL (database)
- MinIO (object storage)
- Kafka cluster (3 nodes for message queue)
- Kafka UI (dashboard for Kafka)
- Airflow (webserver & scheduler)
- Ollama (LLM service)

## Commands

### Start All Services
```
docker compose -f deployments/docker/local/docker-compose.yml --env-file .env up -d
```

### Start Individual Service Groups

- Infrastructure only (PostgreSQL, MinIO, Kafka, Kafka UI, Redis, Ollama)
```
docker compose -f deployments/docker/local/docker-compose.infra.yml --env-file .env up -d
```

- Ads services
```
docker compose -f deployments/docker/local/docker-compose.ads.yml --env-file .env up -d
```

- Data Ingestion & Airflow (Airflow webserver, scheduler, DAGs)
```
docker compose -f deployments/docker/local/docker-compose.data-ingestion-dags.yml --env-file .env up -d### Run Applications Locally
```

- API Server
```
uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload
```

- Worker Service
```
uvicorn apps.worker.main:app --host 0.0.0.0 --port 8001 --reload
```

## Access Services & Dashboards

### Application Services

- **API Server**: http://localhost:8000
  - API Docs: http://localhost:8000/docs
  - Health Check: http://localhost:8000/healthz
- **Worker Service**: http://localhost:8001
  - Health Check: http://localhost:8001/healthz

### Infrastructure Dashboards

- **MinIO Console**: http://localhost:9001
  - Default credentials: `minioadmin` / `minioadmin`
  - Access Key: Set via `STORAGE_ACCESS_KEY` in `.env`
  - Secret Key: Set via `STORAGE_SECRET_KEY` in `.env`

- **Kafka UI**: http://localhost:8082
  - View topics, messages, consumer groups, and cluster information
  - No authentication required (development only)

- **Airflow UI**: http://localhost:8080
  - Default credentials: `airflow` / `airflow` (or set via `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` in `.env`)
  - View and manage DAGs, monitor task execution, view logs

- **PostgreSQL**: `localhost:5432`
  - Database: `stable-ads` (or set via `DB_DB` in `.env`)
  - User: `postgres` (or set via `DB_USER` in `.env`)
  - Password: Set via `DB_PASSWORD` in `.env`


## Folder Structure

```
stable-ads/
|
|- apps/                     # Application entry points
|  |
|  |- api/                   # API server application
|  |
|  |- dags/                  # Airflow DAGs
|  |
|  |- worker/                # Worker service application
|
|- core/                     # Core infrastructure and utilities
|  |
|  |- infra/                 # Infrastructure components
|  |  |
|  |  |- blob/               # Object storage abstraction (MinIO)
|  |  |
|  |  |- db/                 # Database components
|  |  |
|  |  |- mq/                 # Message queue components
|  |     |
|  |     |- adapters/        # Message queue adapters
|  |     |
|  |     |- bus/             # Message bus
|  |
|  |- settings/              # Configuration management
|  |
|  |- utils/                 # Utility functions
|
|- deployments/              # Deployment configurations
|  |
|  |- docker/                # Docker configurations
|     |
|     |- local/              # Local development Docker Compose files
|
|- modules/                  # Business logic modules
|  |
|  |- data_ingestion/        # Data ingestion module
|  |  |
|  |  |- parsers/            # File parsers (CSV, etc.)
|  |
|  |- jobs/                  # Job management module
|  |
|  |- llm/                   # Large Language Model integration
|  |  |
|  |  |- tools/               # LLM tools
|  |
|  |- orchestrator/          # Workflow orchestration
|  |
|  |- render/                # Video rendering module
|     |
|     |- backend/            # Rendering backends
|
|- notebooks/                # Jupyter notebooks

```
