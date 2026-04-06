# prediction_service_v2.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any


@dataclass
class MarketPrediction:
    market_key: str
    market_name: str
    selection: str
    probability: float
    implied_probability: float
    odds: float
    ev: float
    confidence: float
    league_weight: float
    score: float
    reason: str


@dataclass
class BestPredictionResult:
    match_id: int
    league_name: str
    home_team: str
    away_team: str
    kickoff: str
    best_market: Optional[MarketPrediction]
    all_predictions: List[MarketPrediction]
    summary: Dict[str, Any]


class PredictionServiceV2:
    def __init__(self, db_path: str = "matches.db"):
        self.db_path = db_path

        self.default_league_weights = {
            "Turkey Super Lig": 1.02,
            "England Premier League": 1.08,
            "Spain La Liga": 1.08,
            "Italy Serie A": 1.07,
            "Germany Bundesliga": 1.07,
            "France Ligue 1": 1.06,
            "UEFA Champions League": 1.10,
            "UEFA Europa League": 1.07,
        }

        self.market_stability_weights = {
            "HOME_WIN": 1.00,
            "DRAW": 0.92,
            "AWAY_WIN": 1.00,
            "DOUBLE_CHANCE_1X": 1.10,
            "DOUBLE_CHANCE_X2": 1.08,
            "DOUBLE_CHANCE_12": 1.04,
            "OVER_1_5": 1.10,
            "UNDER_3_5": 1.06,
            "BTTS_YES": 1.00,
            "BTTS_NO": 0.98,
        }

        # Relaxed thresholds so V2 actually returns markets instead of empty lists
        self.min_odds = 1.18
        self.max_reasonable_odds = 5.50
        self.min_probability = 0.40
        self.min_ev = -0.03
        self.min_confidence = 0.42

    def analyze_match(self, match_row: Dict[str, Any], odds_data: Dict[str, float]) -> BestPredictionResult:
        league_name = str(match_row.get("league_name", "") or "")
        home_team = str(match_row.get("home_team", "") or "")
        away_team = str(match_row.get("away_team", "") or "")
        kickoff = str(match_row.get("match_date", "") or "")
        match_id = int(match_row.get("match_id", 0) or 0)

        raw_probs = self._predict_base_probabilities(match_row)
        market_probs = self._build_market_probabilities(raw_probs)
        league_weight = self._get_league_weight(league_name)

        all_scored: List[MarketPrediction] = []

        for market_key, market_info in market_probs.items():
            odds_key = market_info["odds_key"]
            odds = self._safe_float(odds_data.get(odds_key))

            if odds is None:
                continue
            if odds < self.min_odds or odds > self.max_reasonable_odds:
                continue

            probability = self._clamp(market_info["probability"], 0.01, 0.99)
            implied_probability = 1.0 / odds
            ev = (probability * odds) - 1.0

            base_conf = self._estimate_confidence(match_row, market_key, probability)
            stability_weight = self.market_stability_weights.get(market_key, 1.0)
            confidence = self._clamp(base_conf * league_weight * stability_weight, 0.01, 0.99)

            score = (
                (ev * 100.0) * 0.45 +
                (confidence * 100.0) * 0.30 +
                (probability * 100.0) * 0.25
            )

            if odds >= 4.50:
                score -= 9
            elif odds >= 3.20:
                score -= 5
            elif odds >= 2.50:
                score -= 2

            reason = self._build_reason_text(
                match_row=match_row,
                market_key=market_key,
                probability=probability,
                odds=odds,
                ev=ev,
                confidence=confidence,
                league_weight=league_weight
            )

            pred = MarketPrediction(
                market_key=market_key,
                market_name=market_info["market_name"],
                selection=market_info["selection"],
                probability=round(probability, 4),
                implied_probability=round(implied_probability, 4),
                odds=round(odds, 2),
                ev=round(ev, 4),
                confidence=round(confidence, 4),
                league_weight=round(league_weight, 4),
                score=round(score, 2),
                reason=reason
            )
            all_scored.append(pred)

        filtered_predictions = self._filter_predictions(all_scored)
        filtered_predictions.sort(key=lambda x: x.score, reverse=True)
        all_scored.sort(key=lambda x: x.score, reverse=True)

        # Important: if relaxed filters still produce nothing, keep top raw candidates
        final_predictions = filtered_predictions if filtered_predictions else all_scored[:3]
        best_market = final_predictions[0] if final_predictions else None

        return BestPredictionResult(
            match_id=match_id,
            league_name=league_name,
            home_team=home_team,
            away_team=away_team,
            kickoff=kickoff,
            best_market=best_market,
            all_predictions=final_predictions,
            summary={
                "match_score_home": raw_probs["home_strength"],
                "match_score_draw": raw_probs["draw_strength"],
                "match_score_away": raw_probs["away_strength"],
                "goals_expectancy": raw_probs["goals_expectancy"],
                "btts_expectancy": raw_probs["btts_expectancy"],
                "league_weight": league_weight,
                "prediction_count": len(final_predictions),
                "raw_prediction_count": len(all_scored),
            }
        )

    def analyze_matches_bulk(self, matches: List[Dict[str, Any]]) -> List[BestPredictionResult]:
        results: List[BestPredictionResult] = []
        for row in matches:
            odds_data = row.get("odds_data", {}) or {}
            result = self.analyze_match(row, odds_data)
            results.append(result)
        return results

    def choose_coupon_candidates(
        self,
        matches: List[Dict[str, Any]],
        min_score: float = 45.0,
        min_confidence: float = 0.44,
        min_ev: float = -0.03,
        prefer_odds_min: float = 1.20,
        prefer_odds_max: float = 1.65
    ) -> List[Dict[str, Any]]:
        analyzed = self.analyze_matches_bulk(matches)
        candidates: List[Dict[str, Any]] = []
        used_pairs = set()

        for res in analyzed:
            bm = res.best_market
            if not bm:
                continue

            pair_key = (res.home_team, res.away_team, bm.market_key)
            if pair_key in used_pairs:
                continue

            if bm.score >= min_score and bm.confidence >= min_confidence and bm.ev >= min_ev:
                candidates.append(self._coupon_row(res))
                used_pairs.add(pair_key)

        candidates.sort(key=lambda x: (x["score"], x["confidence"], x["ev"]), reverse=True)
        return candidates

    def _predict_base_probabilities(self, row: Dict[str, Any]) -> Dict[str, float]:
        home_form = self._safe_float(row.get("home_form"), 0.50)
        away_form = self._safe_float(row.get("away_form"), 0.50)
        home_attack = self._safe_float(row.get("home_attack"), 1.20)
        away_attack = self._safe_float(row.get("away_attack"), 1.10)
        home_defense = self._safe_float(row.get("home_defense"), 1.00)
        away_defense = self._safe_float(row.get("away_defense"), 1.00)
        home_points = self._safe_float(row.get("home_points_per_match"), 1.40)
        away_points = self._safe_float(row.get("away_points_per_match"), 1.20)
        home_goals_for = self._safe_float(row.get("home_goals_for"), 1.40)
        away_goals_for = self._safe_float(row.get("away_goals_for"), 1.20)
        home_goals_against = self._safe_float(row.get("home_goals_against"), 1.10)
        away_goals_against = self._safe_float(row.get("away_goals_against"), 1.10)
        home_win_rate = self._safe_float(row.get("home_win_rate"), 0.45)
        away_win_rate = self._safe_float(row.get("away_win_rate"), 0.30)
        draw_rate = self._safe_float(row.get("draw_rate"), 0.26)

        home_adv = 0.18

        home_strength = (
            (home_form - away_form) * 0.24 +
            (home_attack - away_defense) * 0.18 +
            (home_points - away_points) * 0.12 +
            (home_goals_for - away_goals_against) * 0.16 +
            (home_win_rate - away_win_rate) * 0.20 +
            home_adv
        )

        away_strength = (
            (away_form - home_form) * 0.22 +
            (away_attack - home_defense) * 0.18 +
            (away_points - home_points) * 0.12 +
            (away_goals_for - home_goals_against) * 0.16 +
            (away_win_rate - home_win_rate) * 0.20
        )

        draw_strength = (
            draw_rate * 1.20 -
            abs(home_form - away_form) * 0.35 -
            abs(home_points - away_points) * 0.15
        )

        goals_expectancy = (
            home_goals_for * 0.33 +
            away_goals_for * 0.27 +
            away_goals_against * 0.20 +
            home_goals_against * 0.20
        )

        btts_expectancy = (
            ((home_goals_for + away_goals_for) / 2.0) * 0.55 +
            ((home_goals_against + away_goals_against) / 2.0) * 0.45
        )

        raw_home = math.exp(home_strength)
        raw_draw = math.exp(draw_strength)
        raw_away = math.exp(away_strength)
        denom = raw_home + raw_draw + raw_away

        p_home = raw_home / denom
        p_draw = raw_draw / denom
        p_away = raw_away / denom

        return {
            "p_home": p_home,
            "p_draw": p_draw,
            "p_away": p_away,
            "home_strength": round(home_strength, 4),
            "draw_strength": round(draw_strength, 4),
            "away_strength": round(away_strength, 4),
            "goals_expectancy": round(goals_expectancy, 4),
            "btts_expectancy": round(btts_expectancy, 4),
        }

    def _build_market_probabilities(self, raw_probs: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
        p_home = raw_probs["p_home"]
        p_draw = raw_probs["p_draw"]
        p_away = raw_probs["p_away"]
        goals_exp = raw_probs["goals_expectancy"]
        btts_exp = raw_probs["btts_expectancy"]

        p_1x = self._clamp(p_home + p_draw, 0.01, 0.99)
        p_x2 = self._clamp(p_draw + p_away, 0.01, 0.99)
        p_12 = self._clamp(p_home + p_away, 0.01, 0.99)

        p_over_15 = self._clamp(0.42 + (goals_exp * 0.18), 0.40, 0.92)
        p_under_35 = self._clamp(0.88 - (max(goals_exp - 2.2, 0) * 0.16), 0.35, 0.90)
        p_btts_yes = self._clamp(0.35 + (btts_exp * 0.16), 0.25, 0.85)
        p_btts_no = self._clamp(1.0 - p_btts_yes, 0.15, 0.75)

        return {
            "HOME_WIN": {"market_name": "Maç Sonucu", "selection": "MS 1", "probability": p_home, "odds_key": "home_win"},
            "DRAW": {"market_name": "Maç Sonucu", "selection": "MS X", "probability": p_draw, "odds_key": "draw"},
            "AWAY_WIN": {"market_name": "Maç Sonucu", "selection": "MS 2", "probability": p_away, "odds_key": "away_win"},
            "DOUBLE_CHANCE_1X": {"market_name": "Çifte Şans", "selection": "1X", "probability": p_1x, "odds_key": "double_chance_1x"},
            "DOUBLE_CHANCE_X2": {"market_name": "Çifte Şans", "selection": "X2", "probability": p_x2, "odds_key": "double_chance_x2"},
            "DOUBLE_CHANCE_12": {"market_name": "Çifte Şans", "selection": "12", "probability": p_12, "odds_key": "double_chance_12"},
            "OVER_1_5": {"market_name": "Toplam Gol", "selection": "1.5 Üst", "probability": p_over_15, "odds_key": "over_1_5"},
            "UNDER_3_5": {"market_name": "Toplam Gol", "selection": "3.5 Alt", "probability": p_under_35, "odds_key": "under_3_5"},
            "BTTS_YES": {"market_name": "Karşılıklı Gol", "selection": "KG Var", "probability": p_btts_yes, "odds_key": "btts_yes"},
            "BTTS_NO": {"market_name": "Karşılıklı Gol", "selection": "KG Yok", "probability": p_btts_no, "odds_key": "btts_no"},
        }

    def _estimate_confidence(self, row: Dict[str, Any], market_key: str, probability: float) -> float:
        fields = [
            row.get("home_form"),
            row.get("away_form"),
            row.get("home_attack"),
            row.get("away_attack"),
            row.get("home_defense"),
            row.get("away_defense"),
            row.get("home_goals_for"),
            row.get("away_goals_for"),
        ]

        non_null_count = sum(1 for x in fields if x is not None)
        data_quality = non_null_count / len(fields)

        home_form = self._safe_float(row.get("home_form"), 0.50)
        away_form = self._safe_float(row.get("away_form"), 0.50)
        form_gap = abs(home_form - away_form)

        home_points = self._safe_float(row.get("home_points_per_match"), 1.30)
        away_points = self._safe_float(row.get("away_points_per_match"), 1.20)
        points_gap = abs(home_points - away_points)

        certainty_from_prob = abs(probability - 0.50) * 2.0

        conf = (
            data_quality * 0.30 +
            min(form_gap, 1.0) * 0.22 +
            min(points_gap / 2.0, 1.0) * 0.18 +
            certainty_from_prob * 0.30
        )

        if market_key in {"DRAW", "BTTS_YES", "BTTS_NO"}:
            conf *= 0.95

        return self._clamp(conf, 0.01, 0.99)

    def _get_league_weight(self, league_name: str) -> float:
        db_weight = self._read_league_weight_from_db(league_name)
        if db_weight is not None:
            return db_weight
        return self.default_league_weights.get(league_name, 1.0)

    def _read_league_weight_from_db(self, league_name: str) -> Optional[float]:
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT AVG(success_rate) AS avg_success FROM league_market_performance WHERE league_name = ?",
                (league_name,),
            )
            row = cur.fetchone()
            conn.close()
            if not row or row["avg_success"] is None:
                return None
            weight = 1.0 + ((float(row["avg_success"]) - 0.50) * 0.8)
            return self._clamp(weight, 0.88, 1.12)
        except Exception:
            return None

    def _filter_predictions(self, predictions: List[MarketPrediction]) -> List[MarketPrediction]:
        out = []
        for p in predictions:
            if p.probability < self.min_probability:
                continue
            if p.ev < self.min_ev:
                continue
            if p.confidence < self.min_confidence:
                continue
            out.append(p)
        return out

    def _coupon_row(self, res: BestPredictionResult) -> Dict[str, Any]:
        bm = res.best_market
        assert bm is not None
        return {
            "match_id": res.match_id,
            "league_name": res.league_name,
            "home_team": res.home_team,
            "away_team": res.away_team,
            "kickoff": res.kickoff,
            "market_key": bm.market_key,
            "market_name": bm.market_name,
            "selection": bm.selection,
            "odds": bm.odds,
            "probability": bm.probability,
            "implied_probability": bm.implied_probability,
            "ev": bm.ev,
            "confidence": bm.confidence,
            "league_weight": bm.league_weight,
            "score": bm.score,
            "reason": bm.reason,
        }

    def _build_reason_text(
        self,
        match_row: Dict[str, Any],
        market_key: str,
        probability: float,
        odds: float,
        ev: float,
        confidence: float,
        league_weight: float
    ) -> str:
        home_form = self._safe_float(match_row.get("home_form"), 0.50)
        away_form = self._safe_float(match_row.get("away_form"), 0.50)
        home_points = self._safe_float(match_row.get("home_points_per_match"), 1.30)
        away_points = self._safe_float(match_row.get("away_points_per_match"), 1.20)

        parts = []

        if home_form > away_form:
            parts.append("ev sahibi form avantajlı")
        elif away_form > home_form:
            parts.append("deplasman form avantajlı")

        if home_points > away_points:
            parts.append("ev sahibi puan ortalaması üstün")
        elif away_points > home_points:
            parts.append("deplasman puan ortalaması üstün")

        if probability >= 0.70:
            parts.append("olasılık güçlü")
        elif probability >= 0.55:
            parts.append("olasılık tatmin edici")

        if ev >= 0.08:
            parts.append("yüksek value")
        elif ev >= 0.00:
            parts.append("nötr-pozitif value")
        elif ev >= -0.03:
            parts.append("sınırda value")

        if confidence >= 0.72:
            parts.append("güven yüksek")
        elif confidence >= 0.50:
            parts.append("güven yeterli")

        if league_weight > 1.03:
            parts.append("lig güven katsayısı pozitif")

        if not parts:
            parts.append("istatistiksel denge olumlu")

        return ", ".join(parts)

    def _safe_float(self, value: Any, default: Optional[float] = None) -> Optional[float]:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _clamp(self, x: float, low: float, high: float) -> float:
        return max(low, min(high, x))


if __name__ == "__main__":
    service = PredictionServiceV2(db_path="matches.db")

    sample_match = {
        "match_id": 101,
        "league_name": "Turkey Super Lig",
        "home_team": "Galatasaray",
        "away_team": "Kasımpaşa",
        "match_date": "2026-04-06 20:00:00",
        "home_form": 0.82,
        "away_form": 0.48,
        "home_attack": 1.95,
        "away_attack": 1.12,
        "home_defense": 0.86,
        "away_defense": 1.38,
        "home_points_per_match": 2.35,
        "away_points_per_match": 1.05,
        "home_goals_for": 2.10,
        "away_goals_for": 1.05,
        "home_goals_against": 0.80,
        "away_goals_against": 1.55,
        "home_win_rate": 0.74,
        "away_win_rate": 0.26,
        "draw_rate": 0.22,
    }

    sample_odds = {
        "home_win": 1.42,
        "draw": 4.20,
        "away_win": 6.40,
        "double_chance_1x": 1.10,
        "double_chance_x2": 2.70,
        "double_chance_12": 1.20,
        "over_1_5": 1.22,
        "under_3_5": 1.36,
        "btts_yes": 1.72,
        "btts_no": 2.02,
    }

    result = service.analyze_match(sample_match, sample_odds)

    print("=== EN İYİ TAHMİN ===")
    if result.best_market:
        print(asdict(result.best_market))
    else:
        print("Uygun market bulunamadı.")

    print("\n=== TÜM TAHMİNLER ===")
    for item in result.all_predictions:
        print(asdict(item))
