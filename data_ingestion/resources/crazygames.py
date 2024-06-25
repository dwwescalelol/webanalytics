from datetime import datetime, timezone
from pydantic import PrivateAttr
import requests
from dagster import ConfigurableResource, get_dagster_logger


class CrazyGamesResource(ConfigurableResource):

    base_url: str = "https://api.crazygames.com/v3/en_US/"
    _session: requests.sessions.Session = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._session = requests.Session()

    def get_game_rating(self, game: str) -> dict:
        endpoint = f"{self.base_url}/game/{game}/rating"
        responce = self._session.get(endpoint)
        data = responce.json()
        data["currentTime"] = datetime.now(timezone.utc).isoformat()
        return data

    def get_games(self, paginationPage: int = 1, paginationSize: int = 100) -> dict:
        endpoint = (
            f"{self.base_url}/page/games?"
            f"paginationPage={paginationPage}"
            f"&paginationSize={paginationSize}"
        )

        responce = self._session.get(endpoint)
        return responce.json()

    def get_tags(self) -> dict:
        endpoint = f"{self.base_url}/page/tags"
        responce = self._session.get(endpoint)
        return responce.json()

    def get_game(self, game: str) -> dict:
        endpoint = f"{self.base_url}/page/game/{game}"
        responce = self._session.get(endpoint)
        return responce.json()
