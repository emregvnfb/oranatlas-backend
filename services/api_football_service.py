from datetime import datetime, timedelta, timezone
import re
import requests

from settings import API_FOOTBALL_KEY, API_FOOTBALL_BASE_URL, APP_TIMEZONE


class APIFootballService:
    def __init__(self):
        self.base_url = API_FOOTBALL_BASE_URL.rstrip("/")
        self.headers = {"x-apisports-key": API_FOOTBALL_KEY} if API_FOOTBALL_KEY else {}

    def _get(self, path, params=None):
        if not API_FOOTBALL_KEY:
            raise RuntimeError("API_FOOTBALL_KEY eksik. .env içine ekle.")

        response = requests.get(
            f"{self.base_url}/{path.lstrip('/')}",
            headers=self.headers,
            params=params or {},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            raise RuntimeError(str(data["errors"]))
        return data

    def _sanitize_search(self, text):
        clean = re.sub(r"[^A-Za-z0-9 ]+", " ", str(text or ""))
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean

    def get_fixtures_for_date(self, date_str):
        data = self._get("fixtures", {"date": date_str, "timezone": APP_TIMEZONE})
        return data.get("response", [])

    def get_today_and_tomorrow_fixtures(self):
        now = datetime.now(timezone.utc)
        today = now.astimezone().strftime("%Y-%m-%d")
        tomorrow = (now.astimezone() + timedelta(days=1)).strftime("%Y-%m-%d")
        return self.get_fixtures_for_date(today) + self.get_fixtures_for_date(tomorrow)

    def get_fixture_by_id(self, fixture_id):
        data = self._get("fixtures", {"id": fixture_id, "timezone": APP_TIMEZONE})
        items = data.get("response", [])
        return items[0] if items else None

    def get_fixture_odds(self, fixture_id):
        data = self._get("odds", {"fixture": fixture_id, "timezone": APP_TIMEZONE})
        return data.get("response", [])

    def get_odds_by_date(self, date_str, bookmaker_id=None, bet_id=None, page=1):
        params = {
            "date": date_str,
            "timezone": APP_TIMEZONE,
            "page": page,
        }
        if bookmaker_id is not None:
            params["bookmaker"] = int(bookmaker_id)
        if bet_id is not None:
            params["bet"] = int(bet_id)

        data = self._get("odds", params)
        return data.get("response", [])

    def get_odds_bookmakers(self, search=None):
        params = {}
        if search:
            params["search"] = self._sanitize_search(search)
        data = self._get("odds/bookmakers", params)
        return data.get("response", [])

    def get_odds_bets(self, search=None):
        params = {}
        if search:
            params["search"] = self._sanitize_search(search)
        data = self._get("odds/bets", params)
        return data.get("response", [])

    def search_bookmaker(self, name):
        items = self.get_odds_bookmakers(search=name)
        wanted = self._sanitize_search(name).lower()
        for item in items:
            current = self._sanitize_search(item.get("name")).lower()
            if current == wanted:
                return item
        return items[0] if items else None

    def search_prematch_bet(self, name):
        items = self.get_odds_bets(search=name)
        wanted = self._sanitize_search(name).lower()
        for item in items:
            current = self._sanitize_search(item.get("name")).lower()
            if current == wanted:
                return item
        return items[0] if items else None
