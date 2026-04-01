# Electricity Exchange Pipeline

This Dagster pipeline fetches Finland's electricity day-ahead price data from ENTSO-E Transparency Platform and stores it in a PostgreSQL database.

## Architecture
- **Software-defined assets:** Price data is fetched and parsed to the asset `parsed_electricity_prices`) and then stored to database in `db_electricity_prices`) asset. A dbt project then builds views for quarter hour, daily and monthly price data.
- **Automation sensor:** Uses an `AutomationConditionSensor` to monitor assets. It triggers a materialization when data is missing and ENTSO-E publishes the next day-ahead prices.

## Running with Docker Compose

1.  **Set env vars:**
Create a `.env` file in the project root, required variables are listed in `.env.example`.

2.  **Start the services:**
    ```bash
    docker compose up --build
    ```
    ```bash
    # Alternatively for testing production environment
    docker compose -f compose.prod.yaml up --build
    ```
3.  **Access the Dagster UI:** [http://localhost:3010](http://localhost:3010)

## Production Deployment

The pipeline is designed to run on a cluster using the official Dagster Helm chart.

### 1. Build and push Docker image
The provided GHA workflow `build-push-image.yml` builds a production image and pushes it to GHCR.

### 2. Configure Helm
Update `dagster-values.yaml` with your own repository and database configs.

### 3. Deploy
```bash
# Create namespace and secrets
kubectl create namespace electricity-pipeline
kubectl create secret generic entsoe-secrets --from-literal=api-token="YOUR_TOKEN" -n electricity-pipeline
kubectl create secret generic postgres-secrets --from-literal=connection-string="postgresql://..." -n electricity-pipeline

# Install via Helm
helm repo add dagster https://dagster-io.github.io/helm
helm install dagster-release dagster/dagster -n electricity-pipeline -f values.yaml
```
