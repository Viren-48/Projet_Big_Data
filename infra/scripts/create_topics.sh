#!/usr/bin/env bash
# Crée les topics Kafka (idempotent). Lancé depuis l'hôte : make topics
set -euo pipefail
for topic in match-events xg-shots xg-team-totals; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
