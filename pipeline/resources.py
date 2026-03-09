import requests
from dagster import ConfigurableResource
from sqlalchemy import create_engine

class ApiResource(ConfigurableResource):
    api_key: str
    base_url: str = "https://web-api.tp.entsoe.eu/api"

    def fetch_day_ahead_prices(self, domain_eic: str, start_date: str, end_date: str):
        params = {
            "securityToken": self.api_key,
            "documentType": "A44",
            "in_Domain": domain_eic,
            "out_Domain": domain_eic,
            "periodStart": start_date,
            "periodEnd": end_date,
        }
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.text

class PostgresResource(ConfigurableResource):
    connection_string: str

    def get_engine(self):
        return create_engine(self.connection_string)
