"""Accès à la base de restitution (PostgreSQL) : prédictions en direct et indicateurs batch.

Les écritures se font depuis le driver Spark avec psycopg2 :
- `upsert`  : insertion idempotente (utilisée par le streaming, un micro-batch peut être rejoué
              après une reprise sur checkpoint sans créer de doublon) ;
- `replace` : vide la table puis la remplit (utilisée par les jobs batch, relançables à volonté).
"""
from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Iterable, Sequence

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "infra" / "postgres" / "init.sql"


def connect():
    import psycopg2  # import tardif : les tests unitaires n'en ont pas besoin

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "xg"),
        user=os.environ.get("POSTGRES_USER", "xg"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def ensure_schema() -> None:
    """Crée les tables si elles n'existent pas (idempotent)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))


def clean(value):
    """NaN / infini -> NULL (PostgreSQL accepte NaN, mais il fausserait les moyennes)."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _prepare(rows: Iterable[dict]) -> tuple[list[str], list[tuple]]:
    rows = list(rows)
    if not rows:
        return [], []
    columns = list(rows[0].keys())
    return columns, [tuple(clean(r[c]) for c in columns) for r in rows]


def upsert(table: str, rows: Iterable[dict], key: Sequence[str]) -> int:
    """INSERT ... ON CONFLICT (key) DO UPDATE. Renvoie le nombre de lignes envoyées."""
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    columns, values = _prepare(rows)
    if not values:
        return 0
    updates = [c for c in columns if c not in key]
    query = sql.SQL("INSERT INTO {t} ({cols}) VALUES %s ON CONFLICT ({keys}) DO UPDATE SET {sets}").format(
        t=sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
        keys=sql.SQL(", ").join(map(sql.Identifier, key)),
        sets=sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in updates
        ),
    )
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, query, values)
    return len(values)


def replace(table: str, rows: Iterable[dict]) -> int:
    """TRUNCATE puis INSERT dans une seule transaction."""
    from psycopg2 import sql
    from psycopg2.extras import execute_values

    columns, values = _prepare(rows)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE {t}").format(t=sql.Identifier(table)))
        if values:
            query = sql.SQL("INSERT INTO {t} ({cols}) VALUES %s").format(
                t=sql.Identifier(table),
                cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
            )
            execute_values(cur, query, values)
    return len(values)
