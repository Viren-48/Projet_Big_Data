"""Couche analytique (Spark ML) : entraînement, sélection et test du modèle xG.

- Validation croisée groupée par match (foldCol) : les tirs d'un même match ne sont jamais
  à la fois en entraînement et en validation.
- Sélection du modèle sur la log loss de validation croisée, jamais sur le jeu de test.
- Le jeu de test sert uniquement au rapport final, comparé à `statsbomb_xg` sur les mêmes tirs.

Usage : spark-submit --driver-memory 2g batch/04_train.py
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from xg import db
from xg.config import FEATURES_SHOTS, MODEL_DIR, SERVING_DIR
from xg.evaluation import calibration_table, classification_metrics, probability_metrics
from xg.features import BASELINE_FEATURES, CATEGORICAL_FEATURES, LABEL, NUMERIC_FEATURES
from xg.spark import get_spark

N_FOLDS = 3
SEED = 42


def full_pipeline(classifier) -> Pipeline:
    idx = [f"{c}_idx" for c in CATEGORICAL_FEATURES]
    ohe = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]
    return Pipeline(
        stages=[
            StringIndexer(inputCols=CATEGORICAL_FEATURES, outputCols=idx, handleInvalid="keep"),
            OneHotEncoder(inputCols=idx, outputCols=ohe, handleInvalid="keep"),
            VectorAssembler(inputCols=NUMERIC_FEATURES + ohe, outputCol="features"),
            classifier,
        ]
    )


def candidates() -> dict:
    lr_base = LogisticRegression(labelCol=LABEL, featuresCol="features", maxIter=100)
    lr = LogisticRegression(labelCol=LABEL, featuresCol="features", maxIter=200)
    rf = RandomForestClassifier(labelCol=LABEL, featuresCol="features", seed=SEED)
    gbt = GBTClassifier(labelCol=LABEL, featuresCol="features", seed=SEED)
    return {
        "baseline_distance_angle": (
            Pipeline(stages=[VectorAssembler(inputCols=BASELINE_FEATURES, outputCol="features"), lr_base]),
            ParamGridBuilder().addGrid(lr_base.regParam, [0.0]).build(),
        ),
        "logistic_regression": (
            full_pipeline(lr),
            ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.001, 0.01]).build(),
        ),
        "random_forest": (
            full_pipeline(rf),
            ParamGridBuilder()
            .addGrid(rf.numTrees, [100])
            .addGrid(rf.maxDepth, [6, 10])
            .addGrid(rf.minInstancesPerNode, [20])
            .build(),
        ),
        "gbt": (
            full_pipeline(gbt),
            ParamGridBuilder()
            .addGrid(gbt.maxIter, [50, 100])
            .addGrid(gbt.maxDepth, [3, 5])
            .addGrid(gbt.stepSize, [0.1])
            .build(),
        ),
    }


def with_xg(predictions: DataFrame) -> DataFrame:
    return predictions.withColumn("xg", vector_to_array("probability")[1])


def feature_weights(model, scored: DataFrame, top: int = 15) -> list[dict]:
    """Importance (arbres) ou coefficient (régression logistique) par variable, noms lisibles."""
    attrs = scored.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
    clf = model.stages[-1]
    if hasattr(clf, "featureImportances"):
        values, kind = clf.featureImportances.toArray(), "importance"
    elif hasattr(clf, "coefficients"):
        values, kind = clf.coefficients.toArray(), "coefficient"
    else:
        return []
    ranked = sorted(range(len(values)), key=lambda i: abs(values[i]), reverse=True)[:top]
    return [{"feature": names.get(i, f"f{i}"), kind: float(values[i])} for i in ranked]


def save_to_database(report: dict) -> None:
    """Résultats du test dans PostgreSQL : métriques de probabilité + matrices de confusion."""
    metric_cols = ("log_loss", "brier", "auc_roc", "goals", "xg_total", "n_shots")
    rows, confusion = [], []
    for name, res in report["models"].items():
        rows.append(
            {
                "model": name,
                "role": "selected" if name == report["best_model"] else "candidate",
                "cv_log_loss": res["cv_log_loss"],
                **{c: res["test"][c] for c in metric_cols},
                "best_params": json.dumps(res["best_params"]),
                "trained_at": report["trained_at"],
            }
        )
        confusion += [{"model": name, **c} for c in res["test_classification"]]
    rows.append(
        {
            "model": "statsbomb_xg", "role": "reference", "cv_log_loss": None,
            **{c: report["statsbomb_test"][c] for c in metric_cols},
            "best_params": None, "trained_at": report["trained_at"],
        }
    )
    confusion += [{"model": "statsbomb_xg", **c} for c in report["statsbomb_classification"]]

    db.ensure_schema()
    db.replace("model_metrics", rows)
    db.replace("classification_metrics", confusion)
    print(f"PostgreSQL : {len(rows)} modèles et {len(confusion)} matrices de confusion enregistrés.")


def main() -> None:
    spark = get_spark("04_train")
    shots = spark.read.parquet(FEATURES_SHOTS)

    train = (
        shots.where((F.col("split") == "train") & ~F.col("is_penalty"))
        .withColumn("fold", F.pmod(F.hash("match_id"), F.lit(N_FOLDS)).cast("int"))
        .cache()
    )
    test = shots.where(
        (F.col("split") == "test") & ~F.col("is_penalty") & F.col("statsbomb_xg").isNotNull()
    ).cache()
    penalty_xg = (
        shots.where((F.col("split") == "train") & F.col("is_penalty")).agg(F.mean(LABEL)).first()[0]
    )
    if penalty_xg is None:
        raise RuntimeError("Aucun penalty dans le jeu d'entraînement : impossible de fixer l'xG des penalties.")

    evaluator = MulticlassClassificationEvaluator(
        labelCol=LABEL, probabilityCol="probability", metricName="logLoss"
    )
    report: dict = {"models": {}}
    best_name, best_cv, best_model = None, float("inf"), None

    for name, (pipeline, grid) in candidates().items():
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=N_FOLDS,
            foldCol="fold",
            parallelism=2,
            seed=SEED,
        )
        cv_model = cv.fit(train)
        best_idx = min(range(len(cv_model.avgMetrics)), key=cv_model.avgMetrics.__getitem__)
        cv_log_loss = float(cv_model.avgMetrics[best_idx])  # logLoss : plus petit = meilleur
        model = cv_model.bestModel
        scored = with_xg(model.transform(test))
        report["models"][name] = {
            "cv_log_loss": cv_log_loss,
            "best_params": {p.name: v for p, v in grid[best_idx].items()},
            "test": probability_metrics(scored, "xg"),
            "test_classification": classification_metrics(scored, "xg"),
            "feature_weights": feature_weights(model, scored),
        }
        print(f"{name:26s} CV log loss = {cv_log_loss:.4f} | test = {report['models'][name]['test']}")
        for c in report["models"][name]["test_classification"]:
            print(f"{'':26s} seuil {c['threshold']}: TP={c['tp']} FP={c['fp']} FN={c['fn']} TN={c['tn']} "
                  f"précision={c['precision_score']:.3f} rappel={c['recall_score']:.3f} F1={c['f1_score']:.3f}")
        if cv_log_loss < best_cv:
            best_name, best_cv, best_model = name, cv_log_loss, model

    best_scored = with_xg(best_model.transform(test))
    report.update(
        {
            "best_model": best_name,
            "statsbomb_test": probability_metrics(test, "statsbomb_xg"),
            "statsbomb_classification": classification_metrics(test, "statsbomb_xg"),
            "penalty_xg": float(penalty_xg),
            "n_train_shots": train.count(),
            "n_test_shots": test.count(),
            "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "calibration": {
                "model": calibration_table(best_scored, "xg"),
                "statsbomb": calibration_table(test, "statsbomb_xg"),
            },
        }
    )

    best_model.write().overwrite().save(MODEL_DIR)

    out = Path(SERVING_DIR)
    out.mkdir(parents=True, exist_ok=True)
    (out / "metrics.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    best_scored.select(
        "match_id", "competition_name", "team", "player", "minute", "shot_x", "shot_y",
        "is_goal", "xg", "statsbomb_xg",
    ).toPandas().to_parquet(out / "test_predictions.parquet", index=False)

    save_to_database(report)

    print(f"Meilleur modèle (CV) : {best_name} -> {MODEL_DIR}")
    print(f"xG StatsBomb sur le test : {report['statsbomb_test']}")
    spark.stop()


if __name__ == "__main__":
    main()
