# Electricity Exchange Pipeline

This Dagster pipeline fetches Finland's electricity day-ahead price data from ENTSO-E Transparency Platform and stores it in a PostgreSQL database.

## Architecture
- **Software-defined assets:** Uses Dagster's asset-based approach for fetching (`raw_entsoe_xml`), parsing (`parsed_electricity_prices`), and storing (`db_electricity_prices`) data.
- **Automation sensor:** Uses an `AutomationConditionSensor` to monitor assets. It triggers a run when data is missing OR once ENTSO-E publishes the day-ahead prices.

## Running with Docker Compose

For local development and testing

1.  **Start the services:**
    ```bash
    docker compose up --build
    ```
2.  **Access the UI:** [http://localhost:3000](http://localhost:3000)

## Production Deployment

The pipeline is designed to run on a cluster using the official Dagster Helm chart.

### 1. Build and push Docker image
The provided GHA workflow (`deploy.yml`) builds a production image and pushes it to GHCR.

### 2. Configure Helm
Update `dagster-values.yaml` with your own repository and database configs.

### 3. Deploy
```bash
# Create namespace and secrets
kubectl create namespace electricity-pipeline
kubectl create secret generic entsoe-secrets --from-literal=api-token="YOUR_TOKEN" -n electricity-pipeline
# kubectl create secret generic postgres-secrets --from-literal=connection-string="postgresql://..." -n electricity-pipeline

# Install via Helm
helm repo add dagster https://dagster-io.github.io/helm
helm install dagster-release dagster/dagster -n electricity-pipeline -f dagster-values.yaml
```

## Database Schema
Prices are stored in the `electricity_prices` table:
- `timestamp`: UTC datetime (Primary Key)
- `price_eur_mwh`: Raw price from API
- `price_cent_kwh_vat`: Derived price in cents/kWh including VAT 25.5%
- `created_at`: Row insertion UTC timestamp
