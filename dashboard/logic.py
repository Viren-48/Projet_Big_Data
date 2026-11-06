"""Logique du dashboard, sans Streamlit ni Kafka : uniquement pandas, testable unitairement."""
from __future__ import annotations

import pandas as pd

PITCH_LENGTH, PITCH_WIDTH = 120.0, 80.0

SHOT_COLUMNS = [
    "id", "match_id", "competition_name", "match_date", "home_team", "away_team",
    "period", "minute", "second", "team", "player", "shot_x", "shot_y",
    "shot_type", "body_part", "is_penalty", "is_goal", "xg", "statsbomb_xg",
]


def shots_frame(records: list[dict]) -> pd.DataFrame:
    """Messages du topic des tirs -> DataFrame dédupliqué (un tir = un id) et trié."""
    if not records:
        return pd.DataFrame(columns=SHOT_COLUMNS)
    df = pd.DataFrame(records).drop_duplicates("id", keep="last")
    return df.sort_values(["match_id", "period", "minute", "second"]).reset_index(drop=True)


def match_index(shots: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par match, le plus récemment actif en premier (ordre d'arrivée des tirs)."""
    if shots.empty:
        return pd.DataFrame(columns=["match_id", "label", "competition_name", "match_date", "last_minute"])
    shots = shots.assign(_arrival=range(len(shots)))
    out = (
        shots.groupby("match_id")
        .agg(
            home_team=("home_team", "first"),
            away_team=("away_team", "first"),
            competition_name=("competition_name", "first"),
            match_date=("match_date", "first"),
            last_minute=("minute", "max"),
            _arrival=("_arrival", "max"),
        )
        .reset_index()
        .sort_values("_arrival", ascending=False)
    )
    out["label"] = out["home_team"] + " – " + out["away_team"] + " (" + out["match_date"].astype(str) + ")"
    return out.drop(columns="_arrival").reset_index(drop=True)


def team_summary(shots: pd.DataFrame, team: str, totals: dict | None = None) -> dict:
    """Buts, xG et tirs d'une équipe. Priorité à l'agrégat calculé par Spark (topic des cumuls)."""
    if totals:
        return {"goals": int(totals["goals"]), "xg": float(totals["xg"]), "shots": int(totals["shots"])}
    own = shots[shots["team"] == team]
    return {"goals": int(own["is_goal"].sum()), "xg": float(own["xg"].sum()), "shots": int(len(own))}


def xg_race(shots: pd.DataFrame, teams: tuple[str, str]) -> pd.DataFrame:
    """xG cumulé par équipe et par minute, prêt pour une courbe en escalier.
    Chaque équipe part de 0 à la minute 0 et se prolonge jusqu'à la dernière minute connue."""
    end = int(shots["minute"].max()) if not shots.empty else 0
    frames = []
    for team in teams:
        own = shots[shots["team"] == team][["minute", "xg", "is_goal", "player"]].copy()
        own["cum_xg"] = own["xg"].cumsum()
        start = pd.DataFrame({"minute": [0], "xg": [0.0], "is_goal": [0.0], "player": [None], "cum_xg": [0.0]})
        last = own["cum_xg"].iloc[-1] if not own.empty else 0.0
        stop = pd.DataFrame({"minute": [end], "xg": [0.0], "is_goal": [0.0], "player": [None], "cum_xg": [last]})
        frames.append(pd.concat([start, own, stop], ignore_index=True).assign(team=team))
    return pd.concat(frames, ignore_index=True)


def to_display_coords(shots: pd.DataFrame, home_team: str) -> pd.DataFrame:
    """StatsBomb oriente chaque équipe vers x = 120. Pour une carte unique, on retourne les tirs
    de l'équipe à domicile vers le but de gauche (x = 0)."""
    out = shots.copy()
    is_home = out["team"] == home_team
    out["plot_x"] = out["shot_x"].where(~is_home, PITCH_LENGTH - out["shot_x"])
    out["plot_y"] = out["shot_y"].where(~is_home, PITCH_WIDTH - out["shot_y"])
    return out
