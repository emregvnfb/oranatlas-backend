from dataclasses import dataclass
from typing import Optional

@dataclass
class Fixture:
    provider_fixture_id_primary: str
    league_name: str
    country_name: str
    home_team: str
    away_team: str
    starting_at_utc: str
    status: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None

@dataclass
class OddsSnapshot:
    fixture_id: int
    provider_name: str
    bookmaker_name: str
    market_key: str
    selection_key: str
    odd_decimal: float
    captured_at_utc: str
    is_inplay: bool = False
