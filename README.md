# Stable Ads

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Commands](#commands)
- [Folder Structure](#folder-structure)

## Prerequisites

Before setting up the project, ensure you have the following installed:

- **Conda** (Miniconda or Anaconda) - For Python environment management
- **Docker** and **Docker Compose** - For running infrastructure services (PostgreSQL, MinIO, Kafka, Ollama)
- **Python 3.13** - Required Python version
- **CUDA Toolkit** - For GPU acceleration (required for PyTorch with CUDA support)
- **uv** - Python package manager (installed via conda environment)

## Setup

### Create Environment

```bash
conda env create -f environment.yml
```

### Activate Environment

```bash
conda activate stable-ads
```

### Configure Environment Variables

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration settings.

### Start Infrastructure Services

```bash
docker compose -f deployments/docker/local/docker-compose.yml --env-file .env up -d
```

This will start the following services:
- PostgreSQL (database)
- MinIO (object storage)
- Kafka cluster (3 nodes for message queue)
- Ollama (LLM service)

## Commands

### Start All Services

```bash
docker compose -f deployments/docker/local/docker-compose.yml --env-file .env up -d
```

### Start Individual Service Groups

```bash
# Infrastructure only
docker compose -f deployments/docker/local/docker-compose.infra.yml --env-file .env up -d
# Ads services
docker compose -f deployments/docker/local/docker-compose.ads.yml --env-file .env up -d
```

### Run Applications Locally

```bash
# API Server
uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload

# Worker Service
uvicorn apps.worker.main:app --host 0.0.0.0 --port 8001 --reload

# Data Ingestion Service
uvicorn apps.data-ingestion.main:app --host 0.0.0.0 --port 8002 --reload

# Prediction API
uvicorn apps.prediction-api.main:app --host 0.0.0.0 --port 8003 --reload
```

### Access Services

- **API Server**: http://localhost:8000
- **Worker Service**: http://localhost:8001
- **Data Ingestion**: http://localhost:8002
- **Prediction API**: http://localhost:8003
- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **MLflow UI**: http://localhost:5000
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Spark Master**: http://localhost:8081
- **SigNoz Observability**: http://localhost:3301


## Folder Structure

```
stable-ads/
├── apps/                    # Application entry points
│   ├── api/                 # API server application
│   │   └── main.py          # FastAPI application entry point
│   └── worker/              # Worker service application
│       └── main.py          # Worker service entry point
│
├── core/                    # Core infrastructure and utilities
│   ├── infra/               # Infrastructure components
│   │   ├── blob/            # Object storage abstraction (MinIO)
│   │   │   ├── buckets.py   # Bucket management
│   │   │   ├── interface.py # Storage interface
│   │   │   ├── minio_client.py  # MinIO client implementation
│   │   │   └── registry.py  # Service registry
│   │   ├── container.py     # Dependency injection container
│   │   ├── db/              # Database components
│   │   │   ├── async_db.py  # Async database connection
│   │   │   └── model.py     # Database models
│   │   └── mq/              # Message queue components
│   │       ├── adapters/    # Message queue adapters
│   │       │   └── aiokafka_pubsub.py  # Kafka pub/sub adapter
│   │       └── bus/         # Message bus
│   │           ├── interfaces.py   # Bus interfaces
│   │           ├── marshalers.py   # Message serialization
│   │           ├── router.py       # Message routing
│   │           └── topics.py       # Topic definitions
│   ├── settings/            # Configuration management
│   │   └── config.py        # Application settings
│   └── utils/               # Utility functions
│       └── run_in_thread.py # Thread execution utilities
│
├── deployments/             # Deployment configurations
│   └── docker/
│       └── local/
│           └── docker-compose.yml  # Local development Docker Compose
│
├── generated/               # Generated output files
│   └── videos/              # Generated video files
│
├── modules/                 # Business logic modules
│   ├── jobs/               # Job management module
│   │   ├── api.py          # Job API endpoints
│   │   ├── domain.py       # Job domain logic
│   │   ├── dto.py          # Data transfer objects
│   │   ├── handler.py      # Job handlers
│   │   ├── model.py        # Job models
│   │   ├── repository.py   # Data access layer
│   │   └── service.py      # Business services
│   ├── llm/                # Large Language Model integration
│   │   ├── provider.py     # LLM provider abstraction
│   │   └── tools/          # LLM tools
│   │       └── common.py   # Common LLM utilities
│   ├── orchestrator/       # Workflow orchestration
│   │   ├── domain.py       # Orchestration domain logic
│   │   ├── dto.py          # Orchestration DTOs
│   │   ├── graph.py        # Workflow graph definition
│   │   └── mapper.py       # Data mapping utilities
│   └── render/             # Video rendering module
│       ├── backend/        # Rendering backends
│       │   ├── animateddiff.py  # AnimateDiff backend
│       │   ├── base.py     # Base renderer interface
│       │   ├── dto.py      # Render DTOs
│       │   └── svd.py      # Stable Video Diffusion backend
│       └── video_renderer.py  # Video renderer service
│
├── notebooks/              # Jupyter notebooks
│   └── test-svd.ipynb      # SVD testing notebook
│
├── runs/                   # Runtime data and models
│   └── models/             # Downloaded ML models
│       └── video-render/   # Video rendering models
│           ├── animateddiff/    # AnimateDiff models
│           ├── svd/            # Stable Video Diffusion models
│           └── text-to-image/  # Text-to-image models
│
├── environment.yml         # Conda environment definition
├── pyproject.toml          # Python project configuration
├── uv.lock                 # Dependency lock file

```
