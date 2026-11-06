"""Tests du producteur de rejeu (sans Kafka)."""
from __future__ import annotations

from ingestion.replay import enrich, game_seconds, paced


def test_game_seconds_uses_cumulative_minutes():
    assert game_seconds({"minute": 45, "second": 30}) == 2730


def test_paced_sleeps_by_game_time_over_speed():
    events = [{"minute": 0, "second": 0}, {"minute": 0, "second": 30}, {"minute": 1, "second": 30},
              {"minute": 45, "second": 0}, {"minute": 0, "second": 10}]
    sleeps: list[float] = []
    assert list(paced(events, speed=30, sleep=sleeps.append)) == events
    assert sleeps == [1.0, 2.0, 87.0]  # pas d'attente quand le temps recule


def test_paced_without_speed_never_sleeps():
    sleeps: list[float] = []
    list(paced([{"minute": 0, "second": 0}, {"minute": 5, "second": 0}], speed=0, sleep=sleeps.append))
    assert sleeps == []


def test_enrich_adds_match_context():
    match = {"match_id": 7, "match_date": "2024-07-14", "competition": {"competition_id": 55, "competition_name": "UEFA Euro"},
             "season": {"season_id": 282}, "home_team": {"home_team_name": "Spain"}, "away_team": {"away_team_name": "England"}}
    out = enrich({"id": "x", "minute": 3}, match)
    assert out["match_id"] == 7 and out["season_id"] == 282 and out["home_team"] == "Spain" and out["id"] == "x"
