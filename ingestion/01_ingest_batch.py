"""Couche ingestion (batch) : copie le JSON brut StatsBomb du disque local vers HDFS (zone raw).

Les matchs réservés au rejeu temps réel (REPLAY_SEASONS) ne sont PAS ingérés en batch :
le modèle ne les voit jamais avant le streaming.

Usage : spark-submit ingestion/01_ingest_batch.py
"""
from __future__ import annotations

import json
from pathlib import Path

from xg.config import RAW, REPLAY_SEASONS, STATSBOMB_DIR
from xg.spark import get_spark


def replay_match_ids(root: Path) -> set[int]:
    ids: set[int] = set()
    for competition_id, season_id in REPLAY_SEASONS:
        path = root / "matches" / str(competition_id) / f"{season_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"{path} introuvable : le clone StatsBomb est-il complet ?")
        ids |= {m["match_id"] for m in json.loads(path.read_text(encoding="utf-8"))}
    return ids


def main() -> None:
    root = Path(STATSBOMB_DIR)
    spark = get_spark("01_ingest_batch")
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(RAW), hadoop_conf)
    Path_ = jvm.org.apache.hadoop.fs.Path

    def put(local: Path, remote: str) -> None:
        fs.copyFromLocalFile(False, True, Path_(f"file://{local}"), Path_(remote))

    excluded = replay_match_ids(root)

    put(root / "competitions.json", f"{RAW}/competitions.json")
    match_files = sorted((root / "matches").glob("*/*.json"))
    for f in match_files:
        put(f, f"{RAW}/matches/{f.parent.name}/{f.name}")

    event_files = sorted((root / "events").glob("*.json"))
    kept = [f for f in event_files if int(f.stem) not in excluded]
    for i, f in enumerate(kept, 1):
        put(f, f"{RAW}/events/{f.name}")
        if i % 500 == 0:
            print(f"  {i}/{len(kept)} fichiers d'événements copiés")

    print(
        f"Ingestion terminée : {len(match_files)} fichiers de matchs, {len(kept)} fichiers d'événements "
        f"copiés vers {RAW} ; {len(event_files) - len(kept)} matchs réservés au rejeu exclus."
    )
    spark.stop()


if __name__ == "__main__":
    main()
