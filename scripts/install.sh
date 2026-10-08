#!/bin/sh
# VidgeDB — installation "un clic" (aucune compilation, aucune dépendance).
#
#   curl -fsSL <BASE>/install.sh | sh
#
# Le script détecte l'OS + l'architecture, télécharge l'archive de release
# correspondante, vérifie qu'elle s'exécute, et installe le binaire dans
# ~/.local/bin (ou $VIDGEDB_PREFIX/bin).
#
# Variables :
#   VIDGEDB_BASE_URL   base des releases (défaut : GitHub Releases du dépôt)
#   VIDGEDB_VERSION    version à installer (ex. v0.1.0 ; défaut : latest)
#   VIDGEDB_PREFIX     préfixe d'installation (défaut : $HOME/.local)
set -eu

BASE="${VIDGEDB_BASE_URL:-https://github.com/Vidge-AI/VidgeDB/releases}"
VERSION="${VIDGEDB_VERSION:-latest}"
PREFIX="${VIDGEDB_PREFIX:-$HOME/.local}"
BINDIR="$PREFIX/bin"

die() { printf 'vidgedb-install: %s\n' "$1" >&2; exit 1; }

case "$(uname -s)" in
  Linux)  OS=linux ;;
  Darwin) OS=macos ;;
  *) die "OS non supporté : $(uname -s). Sur Windows, utilise l'image conteneur
     (docker run -e VIDGEDB_TOKEN=... -p 127.0.0.1:8888:8888 ghcr.io/vidge-ai/vidgedb)
     ou télécharge vidgedb-windows-x86_64.zip depuis les releases." ;;
esac

case "$(uname -m)" in
  x86_64|amd64) ARCH=x86_64 ;;
  aarch64|arm64) ARCH=aarch64 ;;
  *) die "architecture non supportée : $(uname -m)" ;;
esac

# Nom d'artefact (aligné sur .github/workflows/release.yml)
if [ "$OS" = linux ] && [ "$ARCH" = aarch64 ]; then
  NAME="vidgedb-linux-aarch64-musl"     # statique : Raspberry Pi
elif [ "$OS" = linux ]; then
  NAME="vidgedb-linux-x86_64"
else
  NAME="vidgedb-macos-$ARCH"
fi
ARCHIVE="$NAME.tar.gz"

if [ "$VERSION" = latest ]; then
  URL="$BASE/latest/download/$ARCHIVE"
else
  URL="$BASE/download/$VERSION/$ARCHIVE"
fi

command -v curl >/dev/null 2>&1 || die "curl est requis"
command -v tar  >/dev/null 2>&1 || die "tar est requis"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

printf 'vidgedb-install: %s → %s\n' "$URL" "$BINDIR"
curl -fsSL "$URL" -o "$TMP/$ARCHIVE" || die "téléchargement impossible ($URL)
     — vérifie que la release existe et que le dépôt est public."

tar -xzf "$TMP/$ARCHIVE" -C "$TMP" || die "archive illisible"
SRC="$TMP/vidgedb/vidgedb"
[ -f "$SRC" ] || SRC="$(find "$TMP" -type f -name vidgedb | head -1)"
[ -n "$SRC" ] && [ -f "$SRC" ] || die "binaire vidgedb introuvable dans l'archive"

mkdir -p "$BINDIR"
install -m 0755 "$SRC" "$BINDIR/vidgedb"

# Preuve d'exécution : on n'annonce pas un succès sans l'avoir constaté.
if ! "$BINDIR/vidgedb" --version >/dev/null 2>&1; then
  die "le binaire installé ne s'exécute pas (mauvaise architecture ?)"
fi

printf 'vidgedb-install: OK — %s\n' "$("$BINDIR/vidgedb" --version)"
case ":${PATH}:" in
  *":$BINDIR:"*) ;;
  *) printf 'vidgedb-install: ajoute %s à ton PATH :\n  export PATH="%s:$PATH"\n' "$BINDIR" "$BINDIR" ;;
esac
