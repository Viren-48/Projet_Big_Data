"""Tests des métriques de classification (matrice de confusion, précision, rappel, F1)."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from xg.evaluation import classification_metrics


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test_evaluation").getOrCreate()
    yield session
    session.stop()


def test_confusion_matrix_and_scores(spark):
    # (probabilité, but réel) : à 0.5 -> TP=2, FP=1, FN=1, TN=2
    df = spark.createDataFrame(
        [(0.9, 1.0), (0.7, 1.0), (0.6, 0.0), (0.4, 1.0), (0.2, 0.0), (0.1, 0.0)], ["p", "is_goal"]
    )
    m = classification_metrics(df, "p", thresholds=(0.5,))[0]
    assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (2, 1, 1, 2)
    assert m["precision_score"] == pytest.approx(2 / 3)
    assert m["recall_score"] == pytest.approx(2 / 3)
    assert m["f1_score"] == pytest.approx(2 / 3)
    assert m["accuracy"] == pytest.approx(4 / 6)


def test_no_positive_prediction_gives_zero_not_error(spark):
    df = spark.createDataFrame([(0.1, 1.0), (0.2, 0.0)], ["p", "is_goal"])
    m = classification_metrics(df, "p", thresholds=(0.5,))[0]
    assert m["precision_score"] == 0.0 and m["recall_score"] == 0.0 and m["f1_score"] == 0.0
