"""Création de la SparkSession commune à tous les jobs.

Les jobs tournent sur le cluster Spark standalone (1 master + 2 workers) : le conteneur `spark`
est le *driver* (mode client), les calculs sont exécutés par les executors des deux workers.
Tout se règle par variables d'environnement (.env), voir .env.example.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from xg.config import HDFS_URI


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        # --- Cluster -------------------------------------------------------------------------
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "spark"))  # joignable par les workers
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.memory", os.environ.get("EXECUTOR_MEMORY", "1g"))
        .config("spark.executor.cores", os.environ.get("EXECUTOR_CORES", "2"))
        .config("spark.cores.max", os.environ.get("TOTAL_CORES", "4"))
        # --- HDFS : 2 DataNodes -> réplication 2 (sinon le client écrit avec la valeur par défaut, 3)
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .config("spark.hadoop.dfs.replication", os.environ.get("HDFS_REPLICATION", "2"))
        # --- SQL -----------------------------------------------------------------------------
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")  # 200 par défaut : trop pour 4 cœurs
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
