"""Couche ingestion streaming + déploiement ML : Kafka -> Spark Structured Streaming -> xG en direct.

Quatre requêtes continues sur le même flux d'événements :
  1. chaque tir avec son xG           -> topic Kafka TOPIC_SHOTS   (lu par le dashboard)
  2. le même flux                     -> Parquet sur HDFS          (archive)
  3. xG et buts cumulés par équipe    -> topic Kafka TOPIC_TOTALS  (agrégation avec état, mode update)
  4. chaque tir avec son xG           -> PostgreSQL, table shot_predictions (restitution, idempotent)

Le modèle est le PipelineModel exporté par 04_train.py, rechargé tel quel : mêmes variables
(`xg.features.shot_features`), même code qu'à l'entraînement.

Usage :
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/05_stream_xg.py
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from xg import db
from xg.config import (
    CHECKPOINTS,
    KAFKA_BOOTSTRAP,
    MODEL_DIR,
    SERVING_DIR,
    STREAM_SHOTS,
    TOPIC_EVENTS,
    TOPIC_SHOTS,
    TOPIC_TOTALS,
)
from xg.features import shot_features
from xg.schemas import STREAM_MESSAGE
from xg.spark import get_spark

SHOT_OUTPUT = [
    "id", "match_id", "competition_name", "match_date", "home_team", "away_team",
    "period", "minute", "second", "team", "player",
    "shot_x", "shot_y", "end_x", "end_y",
    "shot_type", "body_part", "is_penalty", "is_goal", "xg", "statsbomb_xg",
]


def main() -> None:
    db.ensure_schema()
    meta = json.loads((Path(SERVING_DIR) / "metrics.json").read_text(encoding="utf-8"))
    penalty_xg = float(meta["penalty_xg"])

    spark = get_spark("05_stream_xg")
    model = PipelineModel.load(MODEL_DIR)

    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_EVENTS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("timestamp").alias("kafka_ts"),  # instant de publication : sert à mesurer le délai
            F.from_json(F.col("value").cast("string"), STREAM_MESSAGE).alias("e"),
        )
        .select("kafka_ts", "e.*")
    )

    shots = shot_features(events)
    scored_all = (
        model.transform(shots)
        .withColumn("model_xg", vector_to_array("probability")[1])
        .withColumn("xg", F.when(F.col("is_penalty"), F.lit(penalty_xg)).otherwise(F.col("model_xg")))
        .withColumn("team", F.col("team.name"))
        .withColumn("player", F.col("player.name"))
        .select(*SHOT_OUTPUT, "kafka_ts")
    )
    scored = scored_all.select(*SHOT_OUTPUT)

    # 1. Tirs -> Kafka
    (
        scored.select(
            F.col("match_id").cast("string").alias("key"),
            F.to_json(F.struct(*SHOT_OUTPUT)).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_SHOTS)
        .option("checkpointLocation", f"{CHECKPOINTS}/shots_kafka")
        .outputMode("append")
        .queryName("shots_to_kafka")
        .start()
    )

    # 2. Tirs -> Parquet HDFS
    (
        scored.writeStream.format("parquet")
        .option("path", STREAM_SHOTS)
        .option("checkpointLocation", f"{CHECKPOINTS}/shots_parquet")
        .partitionBy("match_id")
        .outputMode("append")
        .queryName("shots_to_hdfs")
        .start()
    )

    # 4. Tirs -> PostgreSQL (upsert : un micro-batch rejoué après reprise ne crée pas de doublon)
    def write_batch_to_postgres(batch_df, batch_id: int) -> None:
        rows = [r.asDict() for r in batch_df.collect()]  # un micro-batch ne contient que quelques tirs
        db.upsert("shot_predictions", rows, key=["id"])

    (
        scored_all.writeStream.foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", f"{CHECKPOINTS}/shots_postgres")
        .outputMode("append")
        .queryName("shots_to_postgres")
        .start()
    )

    # 3. Cumul par équipe -> Kafka (état conservé par Spark entre micro-batchs)
    totals = scored.groupBy("match_id", "team").agg(
        F.round(F.sum("xg"), 4).alias("xg"),
        F.sum("is_goal").cast("int").alias("goals"),
        F.count("*").alias("shots"),
        F.max("minute").alias("last_minute"),
    )
    (
        totals.select(
            F.concat_ws("|", F.col("match_id").cast("string"), "team").alias("key"),
            F.to_json(F.struct("match_id", "team", "xg", "goals", "shots", "last_minute")).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_TOTALS)
        .option("checkpointLocation", f"{CHECKPOINTS}/totals_kafka")
        .outputMode("update")
        .queryName("team_totals_to_kafka")
        .start()
    )

    print(f"Streaming démarré : {TOPIC_EVENTS} -> {TOPIC_SHOTS}, {TOPIC_TOTALS}, {STREAM_SHOTS}, PostgreSQL")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
