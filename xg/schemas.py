"""Schémas explicites : on ne laisse jamais Spark inférer le schéma des événements (lent et instable)."""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _named() -> StructType:
    """Objet StatsBomb {id, name} (type, team, player, outcome...)."""
    return StructType([StructField("id", IntegerType()), StructField("name", StringType())])


_LOCATION = ArrayType(DoubleType())

FREEZE_FRAME_PLAYER = StructType(
    [
        StructField("location", _LOCATION),
        StructField("player", _named()),
        StructField("position", _named()),
        StructField("teammate", BooleanType()),
    ]
)

SHOT = StructType(
    [
        StructField("statsbomb_xg", DoubleType()),
        StructField("end_location", _LOCATION),
        StructField("key_pass_id", StringType()),
        StructField("body_part", _named()),
        StructField("type", _named()),
        StructField("outcome", _named()),
        StructField("technique", _named()),
        StructField("first_time", BooleanType()),
        StructField("one_on_one", BooleanType()),
        StructField("aerial_won", BooleanType()),
        StructField("deflected", BooleanType()),
        StructField("saved_to_post", BooleanType()),
        StructField("saved_off_target", BooleanType()),
        StructField("open_goal", BooleanType()),
        StructField("follows_dribble", BooleanType()),
        StructField("redirect", BooleanType()),
        StructField("freeze_frame", ArrayType(FREEZE_FRAME_PLAYER)),
    ]
)

# Sous-ensemble utile d'un événement StatsBomb (les champs absents du schéma sont ignorés).
EVENT_FIELDS = [
    StructField("id", StringType()),
    StructField("index", IntegerType()),
    StructField("period", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("minute", IntegerType()),
    StructField("second", IntegerType()),
    StructField("type", _named()),
    StructField("possession", IntegerType()),
    StructField("possession_team", _named()),
    StructField("play_pattern", _named()),
    StructField("team", _named()),
    StructField("player", _named()),
    StructField("position", _named()),
    StructField("location", _LOCATION),
    StructField("duration", DoubleType()),
    StructField("under_pressure", BooleanType()),
    StructField("shot", SHOT),
]

# Événement tel que stocké après traitement batch (match_id ajouté depuis le nom du fichier).
EVENT = StructType(EVENT_FIELDS + [StructField("match_id", IntegerType())])

# Message Kafka publié par le producteur de rejeu : l'événement + le contexte du match.
STREAM_MESSAGE = StructType(
    EVENT_FIELDS
    + [
        StructField("match_id", IntegerType()),
        StructField("competition_id", IntegerType()),
        StructField("season_id", IntegerType()),
        StructField("competition_name", StringType()),
        StructField("match_date", StringType()),
        StructField("home_team", StringType()),
        StructField("away_team", StringType()),
    ]
)
