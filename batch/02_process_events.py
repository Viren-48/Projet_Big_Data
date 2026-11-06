"""Couche traitement (Spark Core) : JSON brut HDFS -> Parquet curated.

- Événements : RDD `wholeTextFiles` (un fichier = un match = un tableau JSON), parsing et ajout du
  match_id tiré du nom de fichier, puis DataFrame au schéma explicite `EVENT`.
- Matchs : jointure avec competitions.json (genre de la compétition) et attribution du split.

Usage : spark-submit batch/02_process_events.py
"""
from __future__ import annotations

import json
import os

from pyspark.sql import functions as F

from xg.config import CURATED_EVENTS, CURATED_MATCHES, RAW, REPLAY_SEASONS, TEST_SEASONS
from xg.schemas import EVENT
from xg.spark import get_spark


def explode_match_file(item: tuple[str, str]):
    """(chemin, contenu) -> une ligne JSON par événement, avec match_id."""
    path, content = item
    match_id = int(os.path.basename(path).removesuffix(".json"))
    for event in json.loads(content):
        event["match_id"] = match_id
        yield json.dumps(event)


def season_key(competition_col: str = "competition_id", season_col: str = "season_id"):
    return F.concat_ws("_", F.col(competition_col).cast("string"), F.col(season_col).cast("string"))


def main() -> None:
    spark = get_spark("02_process_events")
    sc = spark.sparkContext

    # --- Spark Core : RDD de fichiers entiers ------------------------------------------------------
    raw_files = sc.wholeTextFiles(f"{RAW}/events/*.json", minPartitions=64)
    event_lines = raw_files.flatMap(explode_match_file)
    events = spark.read.schema(EVENT).json(event_lines)
    events.write.mode("overwrite").parquet(CURATED_EVENTS)

    # --- Matchs + compétitions --------------------------------------------------------------------
    competitions = (
        spark.read.option("multiLine", True)
        .json(f"{RAW}/competitions.json")
        .select("competition_id", "season_id", "competition_gender")
    )
    matches = (
        spark.read.option("multiLine", True)
        .json(f"{RAW}/matches/*/*.json")
        .select(
            F.col("match_id").cast("int"),
            F.col("match_date"),
            F.col("kick_off"),
            F.col("competition.competition_id").cast("int").alias("competition_id"),
            F.col("competition.competition_name").alias("competition_name"),
            F.col("season.season_id").cast("int").alias("season_id"),
            F.col("season.season_name").alias("season_name"),
            F.col("home_team.home_team_name").alias("home_team"),
            F.col("away_team.away_team_name").alias("away_team"),
            F.col("home_score").cast("int"),
            F.col("away_score").cast("int"),
        )
        .join(competitions, ["competition_id", "season_id"], "left")
    )
    test_keys = [f"{c}_{s}" for c, s in TEST_SEASONS]
    replay_keys = [f"{c}_{s}" for c, s in REPLAY_SEASONS]
    matches = matches.withColumn(
        "split",
        F.when(season_key().isin(replay_keys), "replay")
        .when(season_key().isin(test_keys), "test")
        .otherwise("train"),
    )
    matches.write.mode("overwrite").parquet(CURATED_MATCHES)

    n_events = spark.read.parquet(CURATED_EVENTS).count()
    print(f"Traitement terminé : {n_events:,} événements -> {CURATED_EVENTS}")
    spark.read.parquet(CURATED_MATCHES).groupBy("split").count().show()
    spark.stop()


if __name__ == "__main__":
    main()
