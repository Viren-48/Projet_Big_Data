"""Couche restitution : dashboard Streamlit.

- « Match en direct » : lit les topics Kafka produits par le streaming (tirs + cumuls par équipe).
- « Modèle » : lit les résultats du batch (metrics.json, test_predictions.parquet) sur le volume /serving.
"""
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.logic import (
    PITCH_LENGTH,
    PITCH_WIDTH,
    match_index,
    shots_frame,
    team_summary,
    to_display_coords,
    xg_race,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SHOTS = os.environ.get("TOPIC_SHOTS", "xg-shots")
TOPIC_TOTALS = os.environ.get("TOPIC_TOTALS", "xg-team-totals")
SERVING_DIR = Path(os.environ.get("SERVING_DIR", "/serving"))

INK, MUTED, TURF, PITCH, LINE = "#17241D", "#5B6B62", "#EEF2EC", "#2F6B3F", "#F3F6F1"
HOME, AWAY = "#2B59C3", "#D98E04"
FONT = "Barlow, system-ui, sans-serif"

st.set_page_config(page_title="xG en direct", layout="wide")
st.markdown(
    f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600&family=Barlow+Condensed:wght@500;700&display=swap');
html, body, [class*="css"], .stMarkdown, .stDataFrame {{ font-family: {FONT}; }}
h1, h2, h3 {{ font-family: 'Barlow Condensed', {FONT}; font-weight: 700; letter-spacing: .01em; color: {INK}; }}
.board {{ background: {PITCH}; color: {LINE}; border-radius: 6px; padding: 18px 28px 14px; margin: 4px 0 18px; }}
.board-grid {{ display: grid; grid-template-columns: 1fr auto 1fr; align-items: center; gap: 24px; }}
.board-team {{ font-family: 'Barlow Condensed', {FONT}; font-size: 1.9rem; font-weight: 500; line-height: 1.1; }}
.board-team.away {{ text-align: right; }}
.board-team .swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin: 0 8px 3px 0; }}
.board-team.away .swatch {{ margin: 0 0 3px 8px; }}
.board-score {{ font-family: 'Barlow Condensed', {FONT}; font-size: 4.2rem; font-weight: 700; line-height: 1; text-align: center; font-variant-numeric: tabular-nums; }}
.board-xg {{ font-size: 1.05rem; text-align: center; opacity: .88; font-variant-numeric: tabular-nums; }}
.board-meta {{ font-size: .9rem; opacity: .75; margin-top: 10px; display: flex; justify-content: space-between; }}
@media (max-width: 700px) {{ .board-score {{ font-size: 3rem; }} .board-team {{ font-size: 1.3rem; }} }}
</style>
""",
    unsafe_allow_html=True,
)


# --- Kafka ---------------------------------------------------------------------------------------
def _consumer():
    """Un consommateur par session navigateur, relu depuis le début : quelques milliers de messages."""
    if "consumer" not in st.session_state:
        from confluent_kafka import Consumer

        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "group.id": f"dashboard-{uuid.uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([TOPIC_SHOTS, TOPIC_TOTALS])
        st.session_state.update(consumer=consumer, shots=[], totals={}, kafka_error=None)
    return st.session_state.consumer


def pull(max_messages: int = 5000, timeout: float = 0.3) -> None:
    consumer = _consumer()
    for _ in range(max_messages):
        msg = consumer.poll(timeout)
        if msg is None:
            break
        if msg.error():
            st.session_state.kafka_error = str(msg.error())
            continue
        payload = json.loads(msg.value())
        if msg.topic() == TOPIC_SHOTS:
            st.session_state.shots.append(payload)
        else:
            st.session_state.totals[(payload["match_id"], payload["team"])] = payload


# --- Graphiques ----------------------------------------------------------------------------------
def _pitch_shapes() -> list[dict]:
    line = {"color": LINE, "width": 2}
    rect = lambda x0, y0, x1, y1: {"type": "rect", "x0": x0, "y0": y0, "x1": x1, "y1": y1, "line": line, "layer": "below"}
    shapes = [
        rect(0, 0, PITCH_LENGTH, PITCH_WIDTH),
        {"type": "line", "x0": 60, "y0": 0, "x1": 60, "y1": PITCH_WIDTH, "line": line, "layer": "below"},
        {"type": "circle", "x0": 50, "y0": 30, "x1": 70, "y1": 50, "line": line, "layer": "below"},
    ]
    for x0, x1, xs0, xs1, gx in ((0, 18, 0, 6, -2), (102, 120, 114, 120, 120)):
        shapes += [
            rect(x0, 18, x1, 62),        # surface de réparation
            rect(xs0, 30, xs1, 50),      # surface de but
            rect(gx, 36, gx + 2, 44),    # but
        ]
    return shapes


def shot_map(shots: pd.DataFrame, home: str, away: str) -> go.Figure:
    df = to_display_coords(shots, home)
    fig = go.Figure()
    for team, color in ((home, HOME), (away, AWAY)):
        own = df[df["team"] == team]
        for is_goal, symbol in ((0.0, "circle-open"), (1.0, "circle")):
            sub = own[own["is_goal"] == is_goal]
            fig.add_trace(
                go.Scatter(
                    x=sub["plot_x"], y=sub["plot_y"], mode="markers",
                    name=f"{team} — {'buts' if is_goal else 'tirs'}",
                    marker={"size": 8 + 42 * sub["xg"].clip(0, 1), "color": color, "symbol": symbol,
                            "line": {"width": 2, "color": color}},
                    customdata=sub[["player", "minute", "xg", "statsbomb_xg"]],
                    hovertemplate="%{customdata[0]}<br>%{customdata[1]}'<br>xG %{customdata[2]:.2f}, "
                                  "StatsBomb %{customdata[3]:.2f}<extra></extra>",
                )
            )
    fig.update_layout(
        shapes=_pitch_shapes(), plot_bgcolor=PITCH, paper_bgcolor=TURF, height=430,
        margin={"l": 8, "r": 8, "t": 8, "b": 8}, showlegend=False, font={"family": FONT, "color": INK},
        xaxis={"range": [-4, PITCH_LENGTH + 4], "visible": False},
        yaxis={"range": [PITCH_WIDTH + 2, -2], "visible": False, "scaleanchor": "x"},
    )
    return fig


def race_chart(shots: pd.DataFrame, home: str, away: str) -> go.Figure:
    race = xg_race(shots, (home, away))
    fig = go.Figure()
    for team, color in ((home, HOME), (away, AWAY)):
        own = race[race["team"] == team]
        fig.add_trace(go.Scatter(x=own["minute"], y=own["cum_xg"], mode="lines", line_shape="hv",
                                 line={"color": color, "width": 3}, name=team, hoverinfo="skip"))
        goals = own[own["is_goal"] == 1.0]
        fig.add_trace(go.Scatter(x=goals["minute"], y=goals["cum_xg"], mode="markers", showlegend=False,
                                 marker={"size": 11, "color": color, "line": {"width": 2, "color": TURF}},
                                 customdata=goals[["player"]],
                                 hovertemplate="But : %{customdata[0]} (%{x}')<extra></extra>"))
    fig.update_layout(
        height=430, paper_bgcolor=TURF, plot_bgcolor=TURF, margin={"l": 8, "r": 8, "t": 8, "b": 8},
        font={"family": FONT, "color": INK}, legend={"orientation": "h", "y": 1.08, "x": 0},
        xaxis={"title": "Minute", "gridcolor": "#D6DED3", "zeroline": False},
        yaxis={"title": "xG cumulé", "gridcolor": "#D6DED3", "zeroline": False, "rangemode": "tozero"},
    )
    return fig


def scoreboard(shots: pd.DataFrame, match: pd.Series) -> None:
    home, away = match["home_team"], match["away_team"]
    totals = st.session_state.totals
    h = team_summary(shots, home, totals.get((match["match_id"], home)))
    a = team_summary(shots, away, totals.get((match["match_id"], away)))
    st.markdown(
        f"""
<div class="board">
  <div class="board-grid">
    <div class="board-team"><span class="swatch" style="background:{HOME}"></span>{home}</div>
    <div>
      <div class="board-score">{h['goals']} – {a['goals']}</div>
      <div class="board-xg">xG {h['xg']:.2f} – {a['xg']:.2f}</div>
    </div>
    <div class="board-team away">{away}<span class="swatch" style="background:{AWAY}"></span></div>
  </div>
  <div class="board-meta"><span>{match['competition_name']}, {match['match_date']}</span>
  <span>{int(match['last_minute'])}e minute, tirs {h['shots']} – {a['shots']}</span></div>
</div>
""",
        unsafe_allow_html=True,
    )


# --- Onglets -------------------------------------------------------------------------------------
@st.fragment(run_every="2s")
def live_tab() -> None:
    try:
        pull()
    except Exception as exc:  # broker indisponible au démarrage : on le dit, on réessaie au prochain tick
        st.error(f"Kafka injoignable sur {KAFKA_BOOTSTRAP} : {exc}")
        return
    if st.session_state.get("kafka_error"):
        st.caption(f"Dernière erreur Kafka : {st.session_state.kafka_error}")

    shots = shots_frame(st.session_state.shots)
    if shots.empty:
        st.info("Aucun tir reçu pour l'instant. Démarrez le streaming (`make stream`), puis le rejeu (`make replay`).")
        return

    matches = match_index(shots)
    choice = st.selectbox("Match", matches["label"], index=0, key="match_label")
    match = matches[matches["label"] == choice].iloc[0]
    current = shots[shots["match_id"] == match["match_id"]]

    scoreboard(current, match)
    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Course aux xG")
        st.plotly_chart(race_chart(current, match["home_team"], match["away_team"]), use_container_width=True)
    with right:
        st.subheader("Carte des tirs")
        st.plotly_chart(shot_map(current, match["home_team"], match["away_team"]), use_container_width=True)
        st.caption(f"Taille du point = xG. Point plein = but. {match['home_team']} attaque vers la gauche.")

    st.subheader("Derniers tirs")
    recent = current.sort_values(["period", "minute", "second"], ascending=False).head(12)
    st.dataframe(
        recent[["minute", "team", "player", "shot_type", "body_part", "xg", "statsbomb_xg", "is_goal"]]
        .rename(columns={"minute": "Minute", "team": "Équipe", "player": "Joueur", "shot_type": "Type",
                         "body_part": "Surface", "xg": "Notre xG", "statsbomb_xg": "xG StatsBomb",
                         "is_goal": "But"})
        .assign(But=lambda d: d["But"].map({1.0: "oui", 0.0: ""})),
        hide_index=True, use_container_width=True,
        column_config={"Notre xG": st.column_config.NumberColumn(format="%.2f"),
                       "xG StatsBomb": st.column_config.NumberColumn(format="%.2f")},
    )


def model_tab() -> None:
    path = SERVING_DIR / "metrics.json"
    if not path.exists():
        st.info("Aucun modèle entraîné pour l'instant. Lancez `make train`.")
        return
    report = json.loads(path.read_text(encoding="utf-8"))
    best = report["best_model"]

    st.subheader("Comparaison sur le jeu de test")
    st.caption(
        f"{report['n_test_shots']:,} tirs hors penalties (Bundesliga 2023/2024, Copa América 2024). "
        f"Modèle retenu par validation croisée : {best}. Entraîné le {report['trained_at']}."
    )
    rows = [
        {"Modèle": name + (" (retenu)" if name == best else ""), "Log loss CV": m["cv_log_loss"], **m["test"]}
        for name, m in report["models"].items()
    ] + [{"Modèle": "xG StatsBomb (référence)", "Log loss CV": None, **report["statsbomb_test"]}]
    table = pd.DataFrame(rows).rename(columns={
        "log_loss": "Log loss", "brier": "Brier", "auc_roc": "AUC ROC",
        "goals": "Buts", "xg_total": "xG total", "n_shots": "Tirs",
    })
    st.dataframe(table, hide_index=True, use_container_width=True,
                 column_config={c: st.column_config.NumberColumn(format="%.4f")
                                for c in ("Log loss CV", "Log loss", "Brier", "AUC ROC")})

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Calibration")
        fig = go.Figure(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", line={"color": MUTED, "dash": "dot"},
                                   name="Calibration parfaite", hoverinfo="skip"))
        for key, label, color in (("model", f"Notre modèle ({best})", HOME), ("statsbomb", "StatsBomb", AWAY)):
            cal = pd.DataFrame(report["calibration"][key])
            fig.add_trace(go.Scatter(x=cal["mean_predicted"], y=cal["observed_rate"], mode="lines+markers",
                                     name=label, line={"color": color, "width": 3},
                                     customdata=cal[["n"]],
                                     hovertemplate="prédit %{x:.2f}, observé %{y:.2f}, %{customdata[0]} tirs<extra></extra>"))
        fig.update_layout(height=400, paper_bgcolor=TURF, plot_bgcolor=TURF, font={"family": FONT, "color": INK},
                          margin={"l": 8, "r": 8, "t": 8, "b": 8}, legend={"orientation": "h", "y": 1.1, "x": 0},
                          xaxis={"title": "xG moyen prédit", "range": [0, 1], "gridcolor": "#D6DED3"},
                          yaxis={"title": "Taux de buts observé", "range": [0, 1], "gridcolor": "#D6DED3"})
        st.plotly_chart(fig, use_container_width=True)
    with right:
        weights = pd.DataFrame(report["models"][best]["feature_weights"])
        if not weights.empty:
            kind = "importance" if "importance" in weights else "coefficient"
            st.subheader("Variables les plus influentes")
            weights = weights.iloc[::-1]
            fig = go.Figure(go.Bar(x=weights[kind], y=weights["feature"], orientation="h", marker_color=PITCH))
            fig.update_layout(height=400, paper_bgcolor=TURF, plot_bgcolor=TURF,
                              font={"family": FONT, "color": INK}, margin={"l": 8, "r": 8, "t": 8, "b": 8},
                              xaxis={"title": kind.capitalize(), "gridcolor": "#D6DED3"})
            st.plotly_chart(fig, use_container_width=True)

    st.caption(f"xG attribué à chaque penalty : {report['penalty_xg']:.3f} (taux de réussite du jeu d'entraînement). "
               "Données : StatsBomb Open Data.")


st.title("xG en direct")
live, model = st.tabs(["Match en direct", "Modèle"])
with live:
    live_tab()
with model:
    model_tab()
