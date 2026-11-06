-- Base de restitution xG (PostgreSQL). Idempotent : appliqué par xg.db.ensure_schema().

-- Prédictions en direct (une ligne par tir), écrites par le job streaming.
CREATE TABLE IF NOT EXISTS shot_predictions (
    id               TEXT PRIMARY KEY,
    match_id         INTEGER,
    competition_name TEXT,
    match_date       TEXT,
    home_team        TEXT,
    away_team        TEXT,
    period           INTEGER,
    minute           INTEGER,
    "second"         INTEGER,
    team             TEXT,
    player           TEXT,
    shot_x           DOUBLE PRECISION,
    shot_y           DOUBLE PRECISION,
    end_x            DOUBLE PRECISION,
    end_y            DOUBLE PRECISION,
    shot_type        TEXT,
    body_part        TEXT,
    is_penalty       BOOLEAN,
    is_goal          DOUBLE PRECISION,
    xg               DOUBLE PRECISION,
    statsbomb_xg     DOUBLE PRECISION,
    kafka_ts         TIMESTAMPTZ,                       -- instant de publication dans Kafka
    predicted_at     TIMESTAMPTZ DEFAULT now()          -- instant d'écriture de la prédiction
);

-- xG, buts et tirs cumulés par équipe et par match (calculés à la volée sur les prédictions).
CREATE OR REPLACE VIEW team_totals AS
SELECT match_id, team,
       ROUND(SUM(xg)::numeric, 3) AS xg,
       SUM(is_goal)::int          AS goals,
       COUNT(*)                   AS shots,
       MAX(minute)                AS last_minute
FROM shot_predictions
GROUP BY match_id, team;

-- Délai observé entre la publication dans Kafka et l'écriture de la prédiction.
CREATE OR REPLACE VIEW stream_latency AS
SELECT COUNT(*)                                                                    AS shots,
       ROUND(AVG(EXTRACT(EPOCH FROM predicted_at - kafka_ts))::numeric, 2)         AS avg_seconds,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY EXTRACT(EPOCH FROM predicted_at - kafka_ts)))::numeric, 2) AS median_seconds,
       ROUND(MAX(EXTRACT(EPOCH FROM predicted_at - kafka_ts))::numeric, 2)         AS max_seconds
FROM shot_predictions
WHERE kafka_ts IS NOT NULL;

-- Indicateurs batch (job 03) : tables remplacées à chaque exécution.
CREATE TABLE IF NOT EXISTS batch_split_summary (
    split                  TEXT PRIMARY KEY,
    shots                  BIGINT,
    matches                BIGINT,
    goal_rate              DOUBLE PRECISION,
    penalty_goal_rate      DOUBLE PRECISION,
    freeze_frame_share     DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS batch_competition_summary (
    competition_name TEXT,
    season_name      TEXT,
    shots            BIGINT,
    goals            BIGINT,
    goal_rate        DOUBLE PRECISION,
    avg_distance     DOUBLE PRECISION,
    PRIMARY KEY (competition_name, season_name)
);

CREATE TABLE IF NOT EXISTS batch_goal_rate_by_distance (
    distance_bucket TEXT PRIMARY KEY,
    shots           BIGINT,
    goals           BIGINT,
    goal_rate       DOUBLE PRECISION
);

-- Résultats du modèle (job 04) : un modèle candidat ou la référence StatsBomb par ligne.
CREATE TABLE IF NOT EXISTS model_metrics (
    model         TEXT PRIMARY KEY,
    role          TEXT,                 -- candidate | selected | reference
    cv_log_loss   DOUBLE PRECISION,
    log_loss      DOUBLE PRECISION,
    brier         DOUBLE PRECISION,
    auc_roc       DOUBLE PRECISION,
    goals         BIGINT,
    xg_total      DOUBLE PRECISION,
    n_shots       BIGINT,
    best_params   TEXT,
    trained_at    TIMESTAMPTZ
);

-- Matrice de confusion et métriques de classification, à plusieurs seuils de décision.
CREATE TABLE IF NOT EXISTS classification_metrics (
    model     TEXT,
    threshold DOUBLE PRECISION,
    tp        BIGINT,
    fp        BIGINT,
    fn        BIGINT,
    tn        BIGINT,
    accuracy  DOUBLE PRECISION,
    precision_score DOUBLE PRECISION,
    recall_score    DOUBLE PRECISION,
    f1_score        DOUBLE PRECISION,
    PRIMARY KEY (model, threshold)
);
