from dagster import ConfigurableResource
import requests

class WeatherAPIResource(ConfigurableResource):
    api_key: str
    def fetch(self, city: str):
        url = f"http://api.weatherapi.com/v1/current.json?key={self.api_key}&q={city}"
        return requests.get(url)