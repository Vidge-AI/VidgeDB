#!/bin/sh
# Entrypoint VidgeDB — le "un clic" qui refuse de faire une bêtise.
#
# Deux garde-fous, parce que le HTTP natif de VidgeDB n'a PAS de TLS :
#   1. on exige un token dès qu'on écoute hors boucle (VIDGEDB_TOKEN vide
#      → démarrage refusé, avec la marche à suivre) ;
#   2. on écoute en 0.0.0.0 DANS le conteneur uniquement parce que le
#      publish Docker (`-p 127.0.0.1:8888:8888`) est le vrai périmètre.
#      Ne publie jamais `-p 8888:8888` sur une machine exposée : mets un
#      reverse proxy TLS (Caddy/Nginx/traefik) devant.
set -eu

DB="${VIDGEDB_DB:-/data/twin.vdg}"
PORT="${VIDGEDB_PORT:-8888}"
ROLE="${VIDGEDB_ROLE:-ingest}"
RETENTION="${VIDGEDB_RETENTION_DAYS:-}"

if [ -z "${VIDGEDB_TOKEN:-}" ]; then
  cat >&2 <<'EOF'
vidgedb: VIDGEDB_TOKEN is empty — refusing to start.
vidgedb: the native HTTP endpoint has NO TLS; without a token anyone who
vidgedb: reaches the port can read the twin (and write, if role=writer).
vidgedb: fix:  docker run -e VIDGEDB_TOKEN=$(openssl rand -hex 24) ...
EOF
  exit 2
fi

set -- --http "$DB" --port "$PORT" --bind 0.0.0.0 \
       --http-token "$VIDGEDB_TOKEN" --agent-id "${VIDGEDB_AGENT_ID:-container}" \
       --role "$ROLE"

if [ -n "$RETENTION" ]; then
  set -- "$@" --retention-days "$RETENTION"
fi

echo "vidgedb: starting on :$PORT (db=$DB role=$ROLE)"
exec /usr/local/bin/vidgedb "$@"
