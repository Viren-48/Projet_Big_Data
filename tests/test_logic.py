"""Tests de la logique du dashboard (pandas seulement)."""
from __future__ import annotations

from dashboard.logic import match_index, shots_frame, team_summary, to_display_coords, xg_race

SHOTS = [
    {"id": "a", "match_id": 1, "competition_name": "UEFA Euro", "match_date": "2024-06-14", "home_team": "H",
     "away_team": "A", "period": 1, "minute": 10, "second": 0, "team": "H", "player": "p1", "shot_x": 108.0,
     "shot_y": 30.0, "shot_type": "Open Play", "body_part": "Head", "is_penalty": False, "is_goal": 1.0,
     "xg": 0.30, "statsbomb_xg": 0.28},
    {"id": "b", "match_id": 1, "competition_name": "UEFA Euro", "match_date": "2024-06-14", "home_team": "H",
     "away_team": "A", "period": 1, "minute": 30, "second": 5, "team": "A", "player": "p2", "shot_x": 100.0,
     "shot_y": 50.0, "shot_type": "Open Play", "body_part": "Right Foot", "is_penalty": False, "is_goal": 0.0,
     "xg": 0.10, "statsbomb_xg": 0.12},
    {"id": "c", "match_id": 1, "competition_name": "UEFA Euro", "match_date": "2024-06-14", "home_team": "H",
     "away_team": "A", "period": 2, "minute": 70, "second": 0, "team": "H", "player": "p1", "shot_x": 110.0,
     "shot_y": 40.0, "shot_type": "Open Play", "body_part": "Left Foot", "is_penalty": False, "is_goal": 0.0,
     "xg": 0.20, "statsbomb_xg": 0.22},
]


def test_shots_frame_deduplicates_by_id():
    df = shots_frame(SHOTS + [dict(SHOTS[0], xg=0.31)])
    assert len(df) == 3
    assert df.loc[df["id"] == "a", "xg"].item() == 0.31


def test_match_index_label():
    idx = match_index(shots_frame(SHOTS))
    assert idx["label"].tolist() == ["H – A (2024-06-14)"]
    assert idx["last_minute"].item() == 70


def test_team_summary_prefers_streaming_totals():
    df = shots_frame(SHOTS)
    assert team_summary(df, "H") == {"goals": 1, "xg": 0.5, "shots": 2}
    assert team_summary(df, "H", {"goals": 2, "xg": 1.25, "shots": 5}) == {"goals": 2, "xg": 1.25, "shots": 5}


def test_xg_race_is_cumulative_and_spans_the_match():
    race = xg_race(shots_frame(SHOTS), ("H", "A"))
    home = race[race["team"] == "H"]
    assert home["minute"].tolist() == [0, 10, 70, 70]
    assert home["cum_xg"].round(2).tolist() == [0.0, 0.3, 0.5, 0.5]
    away = race[race["team"] == "A"]
    assert away["minute"].iloc[-1] == 70 and away["cum_xg"].iloc[-1] == 0.10


def test_home_shots_are_mirrored():
    out = to_display_coords(shots_frame(SHOTS), "H")
    home = out[out["id"] == "a"].iloc[0]
    away = out[out["id"] == "b"].iloc[0]
    assert (home["plot_x"], home["plot_y"]) == (12.0, 50.0)
    assert (away["plot_x"], away["plot_y"]) == (100.0, 50.0)
