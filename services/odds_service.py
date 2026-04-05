from datetime import datetime, timezone
from typing import List, Dict, Any

def implied_probability(odd_decimal):
    try:
        odd_decimal = float(odd_decimal)
        if odd_decimal <= 1:
            return None
        return round(1.0 / odd_decimal, 6)
    except Exception:
        return None

def normalize_api_football_odds(provider_payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    captured_at = datetime.now(timezone.utc).isoformat()
    for item in provider_payload:
        fixture_id = item.get("fixture", {}).get("id")
        for bookmaker in item.get("bookmakers", []):
            bookmaker_name = bookmaker.get("name") or "unknown"
            for bet in bookmaker.get("bets", []):
                market_name = bet.get("name") or "unknown_market"
                for value in bet.get("values", []):
                    odd = value.get("odd")
                    selection = value.get("value") or "unknown_selection"
                    try:
                        odd_decimal = float(str(odd).replace(",", "."))
                    except Exception:
                        continue
                    rows.append({
                        "provider_fixture_id": str(fixture_id),
                        "provider_name": "api_football",
                        "bookmaker_name": bookmaker_name,
                        "market_key": market_name,
                        "selection_key": selection,
                        "odd_decimal": odd_decimal,
                        "implied_probability": implied_probability(odd_decimal),
                        "captured_at_utc": captured_at,
                        "is_inplay": False,
                        "raw_payload": {
                            "bookmaker": bookmaker_name,
                            "market": market_name,
                            "selection": selection,
                        },
                    })
    return rows
