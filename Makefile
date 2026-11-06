SHELL := /bin/bash
-include .env
DRIVER_MEMORY ?= 2g
# Les jobs sont lancés depuis le conteneur `spark` (driver) ; le master est lu dans .env (SPARK_MASTER)
SUBMIT    := docker compose exec spark spark-submit --driver-memory $(DRIVER_MEMORY)
KAFKA_PKG := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
SPEED     ?= 60
MATCH     ?=

.PHONY: help init up down data topics ingest process shots train batch stream replay test logs reset-stream status psql

help:            ## Liste des commandes
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

init:            ## Crée .env à partir de .env.example s'il n'existe pas
	@test -f .env || (cp .env.example .env && echo ".env créé : pensez à changer POSTGRES_PASSWORD")

up: init         ## Démarre HDFS (1 NN + 2 DN), Kafka, Spark (1 master + 2 workers), PostgreSQL, dashboard
	docker compose up -d --build

down:            ## Arrête tout (les volumes HDFS, PostgreSQL et /serving sont conservés)
	docker compose down

status:          ## État du cluster : DataNodes HDFS vivants et workers Spark
	docker compose exec namenode hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity"
	@echo "Workers Spark : http://localhost:8090"

data:            ## Télécharge StatsBomb Open Data dans ./data (clone partiel)
	./infra/scripts/download_data.sh data/statsbomb

topics:          ## Crée les topics Kafka
	./infra/scripts/create_topics.sh

ingest:          ## Couche ingestion batch : JSON brut -> HDFS /raw
	$(SUBMIT) ingestion/01_ingest_batch.py

process:         ## Couche traitement (Spark Core) : /raw -> /curated (Parquet)
	$(SUBMIT) batch/02_process_events.py

shots:           ## Couche analytique (Spark SQL) : table des tirs + indicateurs batch -> PostgreSQL
	$(SUBMIT) batch/03_build_shots.py

train:           ## Couche analytique (Spark ML) : entraînement, test, export du modèle, métriques -> PostgreSQL
	$(SUBMIT) batch/04_train.py

batch: ingest process shots train  ## Toute la chaîne batch

stream:          ## Streaming : Kafka -> Spark Structured Streaming -> xG (laisser tourner)
	$(SUBMIT) --packages $(KAFKA_PKG) streaming/05_stream_xg.py

replay:          ## Rejoue Euro 2024 + CAN 2023 dans Kafka (SPEED=60, MATCH=<id> optionnel)
	docker compose exec dashboard python ingestion/replay.py --speed $(SPEED) $(if $(MATCH),--match-id $(MATCH),)

reset-stream:    ## Efface checkpoints et sorties du streaming (rejouer depuis zéro)
	docker compose exec namenode hdfs dfs -rm -r -f /checkpoints /serving/stream_shots
	docker compose exec postgres sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB -c "TRUNCATE shot_predictions"'

psql:            ## Ouvre un shell SQL sur la base de restitution
	docker compose exec postgres sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'

test:            ## Tests unitaires (Spark + dashboard + producteur)
	docker compose exec spark python -m pytest -q tests/test_features.py tests/test_evaluation.py
	docker compose exec dashboard python -m pytest -q tests/test_logic.py tests/test_replay.py

logs:            ## Journaux des services
	docker compose logs -f --tail=100
