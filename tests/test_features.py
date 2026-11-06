"""Tests des variables xG sur un échantillon construit à la main (géométrie connue).
Exécution : make test (dans le conteneur spark)."""
from __future__ import annotations

import math
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from xg.features import CATEGORICAL_FEATURES, NUMERIC_FEATURES, shot_features
from xg.schemas import EVENT

FIXTURE = Path(__file__).parent / "fixtures" / "events_sample.json"


@pytest.fixture(scope="module")
def shots():
    spark = SparkSession.builder.master("local[1]").appName("test_features").getOrCreate()
    events = spark.read.option("multiLine", True).schema(EVENT).json(str(FIXTURE))
    rows = {r["id"]: r.asDict() for r in shot_features(events).collect()}
    yield rows
    spark.stop()


def test_only_shots_are_kept(shots):
    assert set(shots) == {"e-shot-central", "e-shot-header", "e-penalty"}


def test_distance_and_angle(shots):
    central = shots["e-shot-central"]
    assert central["distance"] == pytest.approx(12.0)
    assert central["angle"] == pytest.approx(2 * math.atan(4 / 12))


def test_freeze_frame_features(shots):
    central = shots["e-shot-central"]
    assert central["has_freeze_frame"] == 1.0
    assert central["n_def_in_cone"] == 2.0   # Def A et Def B ; ni le coéquipier, ni Def C, ni le gardien
    assert central["has_gk"] == 1.0
    assert central["gk_dist_to_goal"] == pytest.approx(1.0)


def test_missing_freeze_frame_is_neutral(shots):
    header = shots["e-shot-header"]
    assert header["has_freeze_frame"] == 0.0
    assert header["n_def_in_cone"] == 0.0
    assert header["has_gk"] == 0.0
    assert header["gk_dist_to_goal"] == 0.0


def test_flags_label_and_penalty(shots):
    central, header, penalty = shots["e-shot-central"], shots["e-shot-header"], shots["e-penalty"]
    assert (central["first_time"], central["under_pressure"], central["one_on_one"]) == (1.0, 1.0, 0.0)
    assert (header["first_time"], header["under_pressure"]) == (0.0, 0.0)
    assert (central["is_goal"], header["is_goal"], penalty["is_goal"]) == (1.0, 0.0, 1.0)
    assert penalty["is_penalty"] and not central["is_penalty"]
    assert header["body_part"] == "Head" and header["play_pattern"] == "From Corner"


def test_no_leakage_into_features():
    forbidden = {"statsbomb_xg", "end_x", "end_y", "is_goal", "deflected", "saved_to_post", "saved_off_target"}
    assert forbidden.isdisjoint(NUMERIC_FEATURES + CATEGORICAL_FEATURES)
