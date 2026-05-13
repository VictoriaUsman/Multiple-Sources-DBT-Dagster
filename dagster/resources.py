from dagster import ConfigurableResource
import requests

class WeatherAPIResource(ConfigurableResource):
    api_key: str
    def fetch(self, city: str):
        url = f"http://api.weatherapi.com/v1/current.json?key={self.api_key}&q={city}"
        try:
            return requests.get(url, timeout=10)
        except requests.exceptions.Timeout:
            raise RuntimeError(f"Weather API timed out for city '{city}'") from None
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(f"Weather API unreachable for city '{city}': {e}") from e


class DiscordResource(ConfigurableResource):
    webhook_url: str

    def notify(self, message: str) -> None:
        response = requests.post(
            self.webhook_url,
            json={"content": message},
            timeout=10,
        )
        if response.status_code not in (200, 204):
            raise RuntimeError(f"Discord notification failed ({response.status_code}): {response.text}")