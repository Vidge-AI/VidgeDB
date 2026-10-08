# VidgeDB — image conteneur (multi-stage, runtime minimal).
#
# Build :  docker build -t vidgedb .
# Run   :  docker run -d -p 127.0.0.1:8888:8888 -v vidgedb-data:/data \
#                    -e VIDGEDB_TOKEN=change-me vidgedb
#
# Pourquoi ce Dockerfile : c'est le "un clic" le plus portable du produit —
# le même artefact tourne sur PC de bureau (Docker Desktop), serveur d'atelier
# Linux, NAS, ou un mini-PC dans l'armoire. Zéro compilation chez l'utilisateur.

# ---------- stage 1 : build statique musl ----------
FROM rust:alpine AS build
# openssl-sys est en mode `vendored-openssl` (OPC-UA) : il compile OpenSSL,
# donc perl + make + un compilateur sont nécessaires, même sur musl.
RUN apk add --no-cache musl-dev perl make gcc g++ linux-headers
WORKDIR /src
# Cargo.lock est versionné : on le copie pour un build reproductible.
COPY Cargo.toml Cargo.lock ./
# Seul src/ est nécessaire pour `--bin vidgedb` (pas tests/ ni examples/).
COPY src ./src
RUN cargo build --release --bin vidgedb \
 && strip target/release/vidgedb \
 && ls -l target/release/vidgedb

# ---------- stage 2 : runtime ----------
FROM alpine:3
RUN apk add --no-cache ca-certificates tini \
 && addgroup -g 1000 vidgedb \
 && adduser -D -u 1000 -G vidgedb vidgedb \
 && mkdir -p /data && chown vidgedb:vidgedb /data
COPY --from=build /src/target/release/vidgedb /usr/local/bin/vidgedb
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER vidgedb
VOLUME ["/data"]
EXPOSE 8888
# tini = reaping correct des signaux (docker stop → SIGTERM → sortie propre).
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]
