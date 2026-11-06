"""Variables du modèle xG.

Une seule implémentation, utilisée par le batch (entraînement) ET par le streaming (prédiction) :
uniquement des expressions de colonnes Spark, sans UDF Python, donc compatibles Structured Streaming.

Règle anti-fuite : seules des informations connues au moment de la frappe entrent dans FEATURE_*.
`shot.statsbomb_xg`, `shot.end_location`, `shot.deflected`, `shot.saved_*` et `shot.outcome`
ne sont jamais des variables d'entrée.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from xg.config import GOAL_X, GOAL_Y, POST_HIGH_Y, POST_LOW_Y

NUMERIC_FEATURES = [
    "distance",
    "angle",
    "first_time",
    "one_on_one",
    "under_pressure",
    "n_def_in_cone",
    "gk_dist_to_goal",
    "has_freeze_frame",
    "has_gk",
]
CATEGORICAL_FEATURES = ["body_part", "shot_type", "technique", "play_pattern"]
BASELINE_FEATURES = ["distance", "angle"]
LABEL = "is_goal"


def _flag(col: Column) -> Column:
    """Booléen StatsBomb (absent = faux) -> double 0/1."""
    return F.coalesce(col, F.lit(False)).cast("double")


def _cat(col: Column) -> Column:
    return F.coalesce(col, F.lit("Unknown"))


def _sign_sql(px: str, py: str, ax: str, ay: str, bx: str, by: str) -> str:
    """Produit vectoriel 2D (P - B) x (A - B) : son signe dit de quel côté de [AB] se trouve P."""
    return f"(({px}) - ({bx})) * (({ay}) - ({by})) - (({ax}) - ({bx})) * (({py}) - ({by}))"


def _in_shooting_cone_sql(px: str, py: str) -> str:
    """Vrai si P est dans le triangle (tireur, poteau bas, poteau haut). Tireur = colonnes sx, sy."""
    a = ("sx", "sy")
    b = (str(GOAL_X), str(POST_LOW_Y))
    c = (str(GOAL_X), str(POST_HIGH_Y))
    d1 = _sign_sql(px, py, *a, *b)
    d2 = _sign_sql(px, py, *b, *c)
    d3 = _sign_sql(px, py, *c, *a)
    has_neg = f"(({d1}) < 0 OR ({d2}) < 0 OR ({d3}) < 0)"
    has_pos = f"(({d1}) > 0 OR ({d2}) > 0 OR ({d3}) > 0)"
    return f"NOT ({has_neg} AND {has_pos})"


def shot_features(events: DataFrame) -> DataFrame:
    """Prend des événements au schéma `xg.schemas.EVENT` (ou STREAM_MESSAGE), garde les tirs,
    ajoute les variables du modèle, la cible et les colonnes de contexte.

    Colonnes ajoutées : NUMERIC_FEATURES, CATEGORICAL_FEATURES, is_goal, is_penalty,
    statsbomb_xg (référence), shot_x, shot_y, end_x, end_y (affichage uniquement).
    """
    shots = events.where(F.col("type.name") == "Shot").where(F.size("location") >= 2)

    ff = "shot.freeze_frame"
    # défenseurs de champ (gardien exclu, il a sa propre variable) dans le triangle tireur-poteaux
    opponent_in_cone = (
        f"filter({ff}, p -> NOT coalesce(p.teammate, false) "
        f"AND coalesce(p.position.name, '') <> 'Goalkeeper' AND size(p.location) >= 2 "
        f"AND {_in_shooting_cone_sql('p.location[0]', 'p.location[1]')})"
    )
    opponent_keeper = (
        f"element_at(filter({ff}, p -> NOT coalesce(p.teammate, false) "
        f"AND p.position.name = 'Goalkeeper' AND size(p.location) >= 2), 1)"
    )

    df = (
        shots.withColumn("sx", F.col("location")[0])
        .withColumn("sy", F.col("location")[1])
        .withColumn("dx", F.lit(GOAL_X) - F.col("sx"))
        .withColumn("dy", F.lit(GOAL_Y) - F.col("sy"))
        .withColumn("distance", F.sqrt(F.col("dx") ** 2 + F.col("dy") ** 2))
        # angle sous lequel le tireur voit l'espace entre les deux poteaux (radians)
        .withColumn(
            "angle",
            F.abs(
                F.atan2(F.lit(POST_HIGH_Y) - F.col("sy"), F.lit(GOAL_X) - F.col("sx"))
                - F.atan2(F.lit(POST_LOW_Y) - F.col("sy"), F.lit(GOAL_X) - F.col("sx"))
            ),
        )
        .withColumn("first_time", _flag(F.col("shot.first_time")))
        .withColumn("one_on_one", _flag(F.col("shot.one_on_one")))
        .withColumn("under_pressure", _flag(F.col("under_pressure")))
        .withColumn("body_part", _cat(F.col("shot.body_part.name")))
        .withColumn("shot_type", _cat(F.col("shot.type.name")))
        .withColumn("technique", _cat(F.col("shot.technique.name")))
        .withColumn("play_pattern", _cat(F.col("play_pattern.name")))
        # contexte défensif (freeze frame) : manquant -> 0 + indicateur, jamais de NaN dans le modèle
        .withColumn(
            "has_freeze_frame",
            (F.col(ff).isNotNull() & (F.size(ff) > 0)).cast("double"),
        )
        .withColumn("n_def_in_cone", F.coalesce(F.size(F.expr(opponent_in_cone)), F.lit(0)).cast("double"))
        .withColumn("_gk", F.expr(opponent_keeper))
        .withColumn("has_gk", F.col("_gk").isNotNull().cast("double"))
        .withColumn(
            "gk_dist_to_goal",
            F.coalesce(
                F.sqrt(
                    (F.lit(GOAL_X) - F.col("_gk.location")[0]) ** 2
                    + (F.lit(GOAL_Y) - F.col("_gk.location")[1]) ** 2
                ),
                F.lit(0.0),
            ),
        )
        # n_def_in_cone vaut -1 si size(NULL) : on le force à 0 quand il n'y a pas de freeze frame
        .withColumn(
            "n_def_in_cone",
            F.when(F.col("has_freeze_frame") == 1.0, F.col("n_def_in_cone")).otherwise(F.lit(0.0)),
        )
        .withColumn("is_goal", (F.col("shot.outcome.name") == "Goal").cast("double"))
        .withColumn("is_penalty", F.col("shot_type") == "Penalty")
        .withColumn("statsbomb_xg", F.col("shot.statsbomb_xg"))
        .withColumn("shot_x", F.col("sx"))
        .withColumn("shot_y", F.col("sy"))
        .withColumn("end_x", F.col("shot.end_location")[0])
        .withColumn("end_y", F.col("shot.end_location")[1])
        .drop("sx", "sy", "dx", "dy", "_gk")
    )
    return df
