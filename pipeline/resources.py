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
    user: str
    password: str
    host: str
    port: str
    db_name: str

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    def get_engine(self, pool_size: int = 5, max_overflow: int = 10):
        return create_engine(
            self.connection_string,
            pool_size=pool_size,
            max_overflow=max_overflow,
        )
