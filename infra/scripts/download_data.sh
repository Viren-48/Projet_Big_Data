#!/usr/bin/env bash
# Clone partiel du dépôt StatsBomb Open Data (matchs + événements, sans 360 ni compositions).
# STATSBOMB_REF=<sha> pour figer une version précise (reproductibilité).
set -euo pipefail

DEST="${1:-data/statsbomb}"
REPO="https://github.com/hudl/open-data.git"

if [ -d "$DEST/.git" ]; then
  echo "Clone déjà présent dans $DEST"
else
  git clone --depth 1 --filter=blob:none --sparse "$REPO" "$DEST"
  git -C "$DEST" sparse-checkout set data/matches data/events
fi

if [ -n "${STATSBOMB_REF:-}" ]; then
  git -C "$DEST" fetch --depth 1 origin "$STATSBOMB_REF"
  git -C "$DEST" checkout --quiet FETCH_HEAD
fi

git -C "$DEST" rev-parse HEAD > "$DEST/../STATSBOMB_COMMIT"
echo "StatsBomb Open Data @ $(cat "$DEST/../STATSBOMB_COMMIT")"
echo "Fichiers d'événements : $(find "$DEST/data/events" -name '*.json' | wc -l)"
du -sh "$DEST/data"
