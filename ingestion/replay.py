"""Producteur de rejeu : publie les événements des matchs réservés (REPLAY_SEASONS) dans Kafka,
dans l'ordre du match, en respectant le temps de jeu accéléré d'un facteur --speed.

Exemples :
  python ingestion/replay.py                       # tous les matchs Euro 2024 + CAN 2023, x60
  python ingestion/replay.py --match-id <match_id>  # un seul match (ids : data/matches/55/282.json)
  python ingestion/replay.py --speed 0             # aussi vite que possible (chargement massif)
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path
from typing import Iterable, Iterator

from xg.config import KAFKA_BOOTSTRAP, REPLAY_SEASONS, STATSBOMB_DIR, TOPIC_EVENTS


def game_seconds(event: dict) -> int:
    """Temps de jeu en secondes. `minute` est cumulée sur le match (la 2e période démarre à 45)."""
    return int(event.get("minute", 0)) * 60 + int(event.get("second", 0))


def paced(events: Iterable[dict], speed: float, sleep=time.sleep) -> Iterator[dict]:
    """Rend les événements en dormant (écart de temps de jeu) / speed entre deux événements.
    Un écart négatif (changement de période, prolongation) ne provoque aucune attente."""
    previous = None
    for event in events:
        t = game_seconds(event)
        if speed > 0 and previous is not None and t > previous:
            sleep((t - previous) / speed)
        previous = t
        yield event


def load_matches(root: Path, match_id: int | None) -> list[dict]:
    matches = []
    for competition_id, season_id in sorted(REPLAY_SEASONS):
        path = root / "matches" / str(competition_id) / f"{season_id}.json"
        matches.extend(json.loads(path.read_text(encoding="utf-8")))
    if match_id is not None:
        matches = [m for m in matches if m["match_id"] == match_id]
        if not matches:
            sys.exit(f"Match {match_id} absent des compétitions de rejeu {sorted(REPLAY_SEASONS)}.")
    return sorted(matches, key=lambda m: (m["match_date"], m.get("kick_off") or "", m["match_id"]))


def enrich(event: dict, match: dict) -> dict:
    return {
        **event,
        "match_id": match["match_id"],
        "competition_id": match["competition"]["competition_id"],
        "season_id": match["season"]["season_id"],
        "competition_name": match["competition"]["competition_name"],
        "match_date": match["match_date"],
        "home_team": match["home_team"]["home_team_name"],
        "away_team": match["away_team"]["away_team_name"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default=STATSBOMB_DIR)
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC_EVENTS)
    parser.add_argument("--speed", type=float, default=60.0, help="facteur d'accélération (0 = sans pause)")
    parser.add_argument("--match-id", type=int, default=None)
    args = parser.parse_args()

    from confluent_kafka import Producer  # import tardif : les tests unitaires n'en ont pas besoin

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20, "acks": "all"})
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    root = Path(args.data_dir)

    for match in load_matches(root, args.match_id):
        events = json.loads((root / "events" / f"{match['match_id']}.json").read_text(encoding="utf-8"))
        label = f"{match['home_team']['home_team_name']} - {match['away_team']['away_team_name']}"
        print(f"[{match['match_date']}] {label} : {len(events)} événements", flush=True)
        for event in paced(events, args.speed):
            payload = json.dumps(enrich(event, match), ensure_ascii=False)
            while True:
                try:
                    producer.produce(args.topic, key=str(match["match_id"]), value=payload)
                    break
                except BufferError:  # file d'envoi locale pleine (--speed 0) : on laisse Kafka absorber
                    producer.poll(1)
            producer.poll(0)
        producer.flush()

    print("Rejeu terminé.")


if __name__ == "__main__":
    main()
