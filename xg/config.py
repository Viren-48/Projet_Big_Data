"""Configuration centrale : chemins HDFS, topics Kafka, découpage train/test/rejeu, géométrie du terrain."""
from __future__ import annotations

import os

# --- Infrastructure -----------------------------------------------------------------------------
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://namenode:8020")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
STATSBOMB_DIR = os.environ.get("STATSBOMB_DIR", "/data/statsbomb/data")  # clone local du dépôt
SERVING_DIR = os.environ.get("SERVING_DIR", "/serving")  # volume partagé Spark -> dashboard

TOPIC_EVENTS = os.environ.get("TOPIC_EVENTS", "match-events")
TOPIC_SHOTS = os.environ.get("TOPIC_SHOTS", "xg-shots")
TOPIC_TOTALS = os.environ.get("TOPIC_TOTALS", "xg-team-totals")

# --- Zones HDFS ----------------------------------------------------------------------------------
RAW = f"{HDFS_URI}/raw/statsbomb"            # JSON brut, copié tel quel
CURATED_EVENTS = f"{HDFS_URI}/curated/events"  # Parquet, 1 ligne = 1 événement
CURATED_MATCHES = f"{HDFS_URI}/curated/matches"
FEATURES_SHOTS = f"{HDFS_URI}/features/shots"  # Parquet, 1 ligne = 1 tir + variables
MODEL_DIR = f"{HDFS_URI}/models/xg_best"
STREAM_SHOTS = f"{HDFS_URI}/serving/stream_shots"
CHECKPOINTS = f"{HDFS_URI}/checkpoints"

# --- Découpage par compétition-saison (competition_id, season_id) ------------------------------
# IDs relevés dans data/competitions.json du dépôt hudl/open-data.
TEST_SEASONS = {
    (9, 281),    # 1. Bundesliga 2023/2024
    (223, 282),  # Copa America 2024
}
REPLAY_SEASONS = {
    (55, 282),   # UEFA Euro 2024
    (1267, 107), # African Cup of Nations 2023
}
MODEL_GENDER = "male"  # le modèle principal n'utilise que les compétitions masculines

# --- Terrain StatsBomb (yards) : 120 x 80, but attaqué en x = 120 --------------------------------
PITCH_LENGTH, PITCH_WIDTH = 120.0, 80.0
GOAL_X, GOAL_Y = 120.0, 40.0
POST_LOW_Y, POST_HIGH_Y = 36.0, 44.0
