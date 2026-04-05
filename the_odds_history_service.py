from datetime import datetime
import requests
from settings import THE_ODDS_API_KEY, THE_ODDS_BASE_URL

class TheOddsHistoryService:
    def __init__(self):
        self.base_url = THE_ODDS_BASE_URL.rstrip("/")
        self.api_key = THE_ODDS_API_KEY

    def _get(self, path, params=None):
        if not self.api_key:
            raise RuntimeError("THE_ODDS_API_KEY eksik. .env içine ekle.")
        request_params = dict(params or {})
        request_params["apiKey"] = self.api_key

        response = requests.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params=request_params,
            timeout=45,
        )
        response.raise_for_status()
        return response.json()

    def get_historical_odds(self, sport_key: str, date_iso: str, regions="eu", markets="h2h,totals"):
        # Example path:
        # /sports/{sport}/odds-history
        return self._get(
            f"sports/{sport_key}/odds-history",
            {
                "regions": regions,
                "markets": markets,
                "date": date_iso,
                "oddsFormat": "decimal",
            },
        )

    def get_historical_event_odds(self, sport_key: str, event_id: str, date_iso: str, regions="eu", markets="h2h,totals"):
        # Optional event-specific historical endpoint, if your plan supports it
        return self._get(
            f"sports/{sport_key}/events/{event_id}/odds-history",
            {
                "regions": regions,
                "markets": markets,
                "date": date_iso,
                "oddsFormat": "decimal",
            },
        )
