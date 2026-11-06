"""Métriques de probabilité, calculées de la même façon pour notre xG et pour l'xG StatsBomb."""
from __future__ import annotations

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EPS = 1e-15


def probability_metrics(df: DataFrame, prob_col: str, label_col: str = "is_goal") -> dict:
    """Log loss, score de Brier, AUC ROC, buts réels vs xG total."""
    p = F.least(F.greatest(F.col(prob_col), F.lit(EPS)), F.lit(1 - EPS))
    y = F.col(label_col)
    row = df.select(
        F.mean(-(y * F.log(p) + (1 - y) * F.log(1 - p))).alias("log_loss"),
        F.mean((F.col(prob_col) - y) ** 2).alias("brier"),
        F.sum(y).alias("goals"),
        F.sum(F.col(prob_col)).alias("xg_total"),
        F.count("*").alias("n_shots"),
    ).first()
    auc = BinaryClassificationEvaluator(
        rawPredictionCol=prob_col, labelCol=label_col, metricName="areaUnderROC"
    ).evaluate(df.select(F.col(prob_col).cast("double"), y.cast("double")))
    return {
        "log_loss": float(row["log_loss"]),
        "brier": float(row["brier"]),
        "auc_roc": float(auc),
        "goals": int(row["goals"]),
        "xg_total": float(row["xg_total"]),
        "n_shots": int(row["n_shots"]),
    }


def calibration_table(df: DataFrame, prob_col: str, n_bins: int = 10, label_col: str = "is_goal") -> list[dict]:
    """Courbe de calibration : xG moyen prédit vs taux de buts observé, par tranche de probabilité."""
    bins = (
        df.withColumn("bin", F.least(F.floor(F.col(prob_col) * n_bins), F.lit(n_bins - 1)).cast("int"))
        .groupBy("bin")
        .agg(
            F.mean(prob_col).alias("mean_predicted"),
            F.mean(label_col).alias("observed_rate"),
            F.count("*").alias("n"),
        )
        .orderBy("bin")
    )
    return [r.asDict() for r in bins.collect()]


# Seuils de décision pour transformer l'xG (une probabilité) en prédiction « but / pas but ».
# Les buts sont rares (~10 % des tirs) : à 0.5 presque aucun tir n'est prédit but, d'où un seuil plus bas.
THRESHOLDS = (0.5, 0.2)


def _ratio(num: float, den: float) -> float:
    return float(num) / den if den else 0.0


def classification_metrics(
    df: DataFrame, prob_col: str, thresholds=THRESHOLDS, label_col: str = "is_goal"
) -> list[dict]:
    """Matrice de confusion, exactitude, précision, rappel et F1, pour chaque seuil.

    tp = but prédit et réel ; fp = but prédit mais raté ; fn = but non prédit ; tn = raté bien prédit.
    """
    y = F.col(label_col) == 1.0
    aggs = []
    for i, t in enumerate(thresholds):
        pred = F.col(prob_col) >= t
        for name, cond in (("tp", pred & y), ("fp", pred & ~y), ("fn", ~pred & y), ("tn", ~pred & ~y)):
            aggs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"{name}_{i}"))
    row = df.agg(*aggs).first()

    out = []
    for i, t in enumerate(thresholds):
        tp, fp, fn, tn = (int(row[f"{n}_{i}"] or 0) for n in ("tp", "fp", "fn", "tn"))
        precision = _ratio(tp, tp + fp)
        recall = _ratio(tp, tp + fn)
        out.append(
            {
                "threshold": float(t),
                "tp": tp, "fp": fp, "fn": fn, "tn": tn,
                "accuracy": _ratio(tp + tn, tp + fp + fn + tn),
                "precision_score": precision,
                "recall_score": recall,
                "f1_score": _ratio(2 * precision * recall, precision + recall),
            }
        )
    return out
