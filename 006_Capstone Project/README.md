#  Electric Scooter Demand Forecasting in Chicago

## 📌 Project Overview

This project forecasts daily demand for electric scooters across Chicago’s 77 neighborhoods, helping a logistics company optimize distribution from local depots. Accurate 3-day-ahead forecasts enable:
- Minimizing scooter shortages during high demand
- Reducing surplus inventory and associated idle time

The system uses a production-grade ML pipeline with modern MLOps practices deployed entirely in the cloud.

---

## 🎯 Objective

Develop and deploy a cloud-based machine learning system that predicts the number of scooter rides per neighborhood per day. The output helps logistics centers maintain an optimal scooter stock level for the next 3 days.

---

## 🧱 Architecture Overview

### ☁️ Cloud Infrastructure

- **EC2**: Hosts MLflow, prediction service, and monitoring tools
- **S3**: Stores data, model artifacts, and monitoring references
- **ECR**: Stores Docker images for the training and inference services
- **RDS (PostgreSQL)**: Stores experiment metadata (MLflow backend)
- **Prefect Cloud**: Orchestrates automated ML pipeline runs

---

## 🧪 Experiment Tracking & Model Registry

- **MLflow** tracks all experiments, stores models in **S3**, and logs metadata in **RDS**.
- Final models are saved to MLflow's model registry.

**Links:**
- MLflow UI: [http://16.171.140.74:5000](http://16.171.140.74:5000)
- Registered Model: [escooter-demand-model](http://16.171.140.74:5000/#/models/escooter-demand-model)

---

## ⚙️ Workflow Orchestration with Prefect

Scheduled daily training jobs using Prefect Cloud. Key commands:

```bash
prefect cloud login -k {PREFECT_KEY}
prefect deployment build -n escooters-demand-train -p default-agent-pool -q escooters-demand-training src/pipeline/train_pipeline.py:main_flow --cron "0 5 * * *"
prefect deployment apply main_flow-deployment.yaml
prefect agent start --pool default-agent-pool --work-queue escooters-demand-training


# E-Scooter Demand Forecasting

A machine learning project for predicting e-scooter trip demand using containerized microservices, monitoring, and CI/CD best practices.

## Model Serving

A REST API for forecasts is containerized with Docker and deployed on EC2.

### API Endpoint

```
POST http://16.171.140.74:9696/predict
```

### Sample Request

```python
import requests

input_data = {
    "community": "LAKE VIEW", 
    "date": "2020-09-11"
}

r = requests.post('http://16.171.140.74:9696/predict', json=input_data)
print(r.json())
```

### Sample Response

```json
{
    "model_version": "71e8d2efac7a4433991aba1a699108e3",
    "trips": 1113
}
```

## Model Monitoring

- **Evidently** computes monitoring metrics
- **Grafana** dashboards visualize real-time model health  
- Data stored in **PostgreSQL**

* **Links:**
  - Grafana Dashboard: [http://16.171.140.74:3000](http://16.171.140.74:3000)
  - Login: `admin` | Password: `admin`

---

## Model Tracking

Model experiments and versions tracked with MLflow.

* **Links:**
  - MLflow UI: [http://16.171.140.74:5000](http://16.171.140.74:5000)
  - Registered Model: [escooter-demand-model](http://16.171.140.74:5000/#/models/escooter-demand-model)

---

## Workflow Orchestration

Scheduled daily training jobs using Prefect Cloud. Key commands:

```bash
prefect cloud login -k {PREFECT_KEY}

prefect deployment build -n escooters-demand-train \
  -p default-agent-pool \
  -q escooters-demand-training \
  src/pipeline/train_pipeline.py:main_flow \
  --cron "0 5 * * *"

prefect deployment apply main_flow-deployment.yaml

prefect agent start \
  --pool default-agent-pool \
  --work-queue escooters-demand-training
```

---

## Reproducibility

Dependency management with **Poetry** and environment isolation via Docker.

### Run API Locally

```bash
docker build -t escooters-trips-api \
  --build-arg AWS_ACCESS_KEY_ID="*****" \
  --build-arg AWS_SECRET_ACCESS_KEY="*****" \
  -f .docker/Dockerfile-api .

docker run -p 9696:9696 escooters-trips-api
```

Or using a public image:

```bash
docker pull public.ecr.aws/h6l8h0t3/escooters-trips/escooters-trips-api:latest
docker run -v ~/.aws:/root/.aws -p 9696:9696 public.ecr.aws/h6l8h0t3/escooters-trips/escooters-trips-api:latest
```

### Run Training Locally

```bash
docker build -f .docker/Dockerfile-train -t escooters-trips-train .
docker run --env TRACKING_URI="http://16.171.140.74:5000" escooters-trips-train
```

Or using prebuilt image:

```bash
docker pull public.ecr.aws/h6l8h0t3/escooters-trips/escooters-trips-train:latest
docker run --env TRACKING_URI="http://16.171.140.74:5000" public.ecr.aws/h6l8h0t3/escooters-trips/escooters-trips-train:latest
```

## Engineering Best Practices

### Testing
- Unit tests with `pytest`
- Integration tests included

### Code Quality
- **Linting:** `pylint`
- **Formatting:** `black`, `isort`

### Makefile Commands

```bash
make test              # Run unit tests
make quality_checks    # Run linting and formatting
make build_api         # Build Docker image for API
make build_train       # Build Docker image for training
```

### Pre-Commit Hooks
- Trailing whitespace cleanup
- End-of-file fixes
- YAML syntax validation
- `isort`, `black`, `pylint` enforcement

## CI/CD Pipeline

- **Stage 1:** Code formatting, linting, and testing
- **Stage 2:** Build and push prediction API image to ECR
- **Stage 3:** Build and push training pipeline image to ECR

## Getting Started

1. Clone the repository
2. Install dependencies with Poetry: `poetry install`
3. Run tests: `make test`
4. Start the API locally using Docker commands above
5. Send prediction requests to the API endpoint

## Project Structure

```
├── .docker/                 # Docker configuration files
├── src/                     # Source code
├── tests/                   # Test files
├── Makefile                 # Build and test commands
├── pyproject.toml          # Poetry dependencies
└── README.md               # This file
```
