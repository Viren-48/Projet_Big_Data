"""Couche analytique (Spark SQL) : table des tirs avec variables, cible et split.

Usage : spark-submit batch/03_build_shots.py
"""
from __future__ import annotations

from pyspark.sql import functions as F

from xg import db
from xg.config import CURATED_EVENTS, CURATED_MATCHES, FEATURES_SHOTS, MODEL_GENDER
from xg.features import CATEGORICAL_FEATURES, LABEL, NUMERIC_FEATURES, shot_features
from xg.spark import get_spark

CONTEXT_COLUMNS = [
    "id", "match_id", "period", "minute", "second", "team", "player",
    "competition_id", "season_id", "competition_name", "season_name", "match_date",
    "home_team", "away_team", "split",
    "is_penalty", "statsbomb_xg", "shot_x", "shot_y", "end_x", "end_y",
]


def main() -> None:
    spark = get_spark("03_build_shots")
    spark.read.parquet(CURATED_EVENTS).createOrReplaceTempView("events")
    spark.read.parquet(CURATED_MATCHES).createOrReplaceTempView("matches")

    shot_events = spark.sql(
        f"""
        SELECT e.*, m.competition_id, m.season_id, m.competition_name, m.season_name,
               m.match_date, m.home_team, m.away_team, m.split
        FROM events e
        JOIN matches m ON e.match_id = m.match_id
        WHERE e.type.name = 'Shot'
          AND m.competition_gender = '{MODEL_GENDER}'
        """
    )

    shots = shot_features(shot_events).select(
        *[c for c in CONTEXT_COLUMNS if c not in ("team", "player")],
        F.col("team.name").alias("team"),
        F.col("player.name").alias("player"),
        *NUMERIC_FEATURES,
        *CATEGORICAL_FEATURES,
        LABEL,
    )
    shots.write.mode("overwrite").partitionBy("split").parquet(FEATURES_SHOTS)

    # --- Indicateurs batch : affichés en console ET stockés dans PostgreSQL ------------------------
    spark.read.parquet(FEATURES_SHOTS).createOrReplaceTempView("shots")
    by_split = spark.sql(
        """
        SELECT split,
               COUNT(*)                                   AS shots,
               COUNT(DISTINCT match_id)                   AS matches,
               ROUND(AVG(is_goal), 4)                     AS goal_rate,
               ROUND(AVG(CASE WHEN is_penalty THEN is_goal END), 4) AS penalty_goal_rate,
               ROUND(AVG(has_freeze_frame), 4)            AS freeze_frame_share
        FROM shots
        GROUP BY split
        ORDER BY split DESC
        """
    )
    by_competition = spark.sql(
        """
        SELECT competition_name, season_name,
               COUNT(*)                       AS shots,
               CAST(SUM(is_goal) AS BIGINT)   AS goals,
               ROUND(AVG(is_goal), 4)         AS goal_rate,
               ROUND(AVG(distance), 2)        AS avg_distance
        FROM shots
        GROUP BY competition_name, season_name
        ORDER BY shots DESC
        """
    )
    by_distance = spark.sql(
        """
        SELECT CASE WHEN distance < 6  THEN '1. < 6 yd'
                    WHEN distance < 12 THEN '2. 6-12 yd'
                    WHEN distance < 18 THEN '3. 12-18 yd'
                    WHEN distance < 25 THEN '4. 18-25 yd'
                    ELSE '5. > 25 yd' END   AS distance_bucket,
               COUNT(*)                     AS shots,
               CAST(SUM(is_goal) AS BIGINT) AS goals,
               ROUND(AVG(is_goal), 4)       AS goal_rate
        FROM shots
        WHERE NOT is_penalty
        GROUP BY 1
        ORDER BY 1
        """
    )
    by_split.show(truncate=False)
    by_distance.show(truncate=False)

    db.ensure_schema()
    for table, df in (
        ("batch_split_summary", by_split),
        ("batch_competition_summary", by_competition),
        ("batch_goal_rate_by_distance", by_distance),
    ):
        n = db.replace(table, [r.asDict() for r in df.collect()])
        print(f"PostgreSQL : {n} lignes -> {table}")
    spark.stop()


if __name__ == "__main__":
    main()
