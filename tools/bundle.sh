#!/usr/bin/env bash
#
# Build recomp-net-server locally and package a ZIP to extract on the server.
#
# This exists because building on the dedicated server is slow and copying a
# raw binary across is not enough on its own: a Rust binary carries the glibc
# floor of the machine that built it, and a bundle needs the runtime files that
# are not compiled in. This script does the build, checks the glibc floor
# against what the target server can actually run, stages everything, and
# writes dist/recomp-net-server-<version>-<sha>-<target>.zip.
#
# Usage:
#   tools/bundle.sh                       # build + bundle for a glibc >= 2.39 server
#   tools/bundle.sh --glibc-max 2.36      # bundle for Debian 12
#   tools/bundle.sh --no-build            # package target/release as it stands
#   tools/bundle.sh --target x86_64-unknown-linux-musl
#   tools/bundle.sh --allow-dirty         # bundle with uncommitted changes
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Ubuntu 24.04 is glibc 2.39, Debian 13 is 2.41, Debian 12 is 2.36,
# Ubuntu 22.04 is 2.35. Set this to the *server's* glibc.
GLIBC_MAX="2.39"
TARGET=""
DO_BUILD=1
ALLOW_DIRTY=0
OUT_DIR="$REPO_ROOT/dist"

die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }
warn() { printf '\033[33mwarn:\033[0m %s\n'  "$*" >&2; }
note() { printf '\033[36m==>\033[0m %s\n'    "$*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --glibc-max)  GLIBC_MAX="${2:?--glibc-max needs a version}"; shift 2 ;;
    --target)     TARGET="${2:?--target needs a triple}"; shift 2 ;;
    --out)        OUT_DIR="${2:?--out needs a directory}"; shift 2 ;;
    --no-build)   DO_BUILD=0; shift ;;
    --allow-dirty) ALLOW_DIRTY=1; shift ;;
    -h|--help)    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)            die "unknown argument: $1 (try --help)" ;;
  esac
done

command -v cargo >/dev/null || die "cargo not found"
# zip (not python's zipfile) because it preserves the executable bit, and the
# whole point of the bundle is that the binary runs after extraction.
command -v zip   >/dev/null || die "zip not found -- install it (pacman -S zip)"

# ---------------------------------------------------------------- build ------

BUILD_ARGS=(build --release --locked)
if [[ -n "$TARGET" ]]; then
  BUILD_ARGS+=(--target "$TARGET")
  BIN_DIR="target/$TARGET/release"
else
  TARGET="$(rustc -vV | awk '/^host: /{print $2}')"
  BIN_DIR="target/release"
fi
BIN="$BIN_DIR/recomp-net-server"

if [[ $DO_BUILD -eq 1 ]]; then
  note "cargo ${BUILD_ARGS[*]}"
  cargo "${BUILD_ARGS[@]}"
else
  note "skipping build (--no-build)"
fi
[[ -x "$BIN" ]] || die "no binary at $BIN -- drop --no-build, or check --target"

# --------------------------------------------------- runtime compatibility ---

# A binary is only portable down to the oldest glibc it references. Catching
# this here is the difference between a failed deploy and a failed bundle.
if [[ "$TARGET" == *musl* ]]; then
  note "musl target: statically linked, no glibc floor"
  GLIBC_FLOOR="none (static musl)"
else
  GLIBC_FLOOR="$(objdump -T "$BIN" 2>/dev/null \
    | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1)"
  [[ -n "$GLIBC_FLOOR" ]] || GLIBC_FLOOR="unknown"
  note "binary requires glibc >= $GLIBC_FLOOR; server declared as $GLIBC_MAX"
  if [[ "$GLIBC_FLOOR" != "unknown" ]] \
     && [[ "$(printf '%s\n%s\n' "$GLIBC_FLOOR" "$GLIBC_MAX" | sort -V | tail -1)" != "$GLIBC_MAX" ]]; then
    die "this binary needs glibc $GLIBC_FLOOR but the server has $GLIBC_MAX.
       It would fail to start with a 'version GLIBC_$GLIBC_FLOOR not found' error.
       Either pass the real server glibc with --glibc-max, or build static:
         rustup target add x86_64-unknown-linux-musl
         tools/bundle.sh --target x86_64-unknown-linux-musl"
  fi

  # Anything beyond the base system libraries has to exist on the server too.
  EXTRA_LIBS="$(ldd "$BIN" | awk '{print $1}' \
    | grep -Ev '^(linux-vdso|libgcc_s|libm|libc|libdl|libpthread|librt|/lib64/ld-linux)' || true)"
  [[ -z "$EXTRA_LIBS" ]] || warn "binary links non-base libraries, which must be present on the server:
$EXTRA_LIBS"
fi

# ------------------------------------------------------------ provenance -----

VERSION="$(awk '/^\[package\]/{p=1;next} /^\[/{p=0} p && /^version *=/{gsub(/[",]/,"");print $3;exit}' Cargo.toml)"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
GIT_DIRTY=""
if ! git diff --quiet HEAD 2>/dev/null; then
  GIT_DIRTY="-dirty"
  if [[ $ALLOW_DIRTY -eq 0 ]]; then
    warn "working tree has uncommitted changes; the bundle is marked -dirty"
    warn "(pass --allow-dirty to silence this, or commit first)"
  fi
fi

NAME="recomp-net-server-${VERSION}-${GIT_SHA}${GIT_DIRTY}-${TARGET}"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
PKG="$STAGE/$NAME"
mkdir -p "$PKG"

# ---------------------------------------------------------------- stage ------

# data/ip_country.bin and data/chat_filter_words.txt are include_bytes!/
# include_str! into the binary, and migrations/ is embedded by sqlx::migrate!,
# so none of them are runtime files. migrations/ ships anyway as a readable
# record of the schema the binary will apply.
install -m 0755 "$BIN"        "$PKG/recomp-net-server"
install -m 0644 .env.example  "$PKG/.env.example"
install -m 0644 README.md     "$PKG/README.md"
install -m 0644 LICENSE       "$PKG/LICENSE"
cp -r docs       "$PKG/docs"
cp -r migrations "$PKG/migrations"

BIN_SHA="$(sha256sum "$PKG/recomp-net-server" | cut -d' ' -f1)"

cat > "$PKG/MANIFEST.txt" <<EOF
recomp-net-server bundle
========================
version        : $VERSION
git commit     : $GIT_SHA${GIT_DIRTY:+ (uncommitted changes present at build)}
target triple  : $TARGET
glibc floor    : $GLIBC_FLOOR
built on       : $(uname -srm) / $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")
rustc          : $(rustc -V)
built at       : $(date -u +%Y-%m-%dT%H:%M:%SZ)
sha256(binary) : $BIN_SHA

Embedded in the binary (not shipped as loose files):
  migrations/*.sql          sqlx::migrate!
  data/ip_country.bin       include_bytes!
  data/chat_filter_words.txt include_str!
EOF

cat > "$PKG/DEPLOY.md" <<'EOF'
# Deploying this bundle

Extract on the server, then:

```bash
# 1. Verify the binary survived the trip and matches MANIFEST.txt
sha256sum recomp-net-server
chmod +x recomp-net-server          # only if the exec bit was lost in transit

# 2. Configure. .env is read from the working directory at startup.
cp .env.example .env
$EDITOR .env                        # BIND_ADDR at minimum

# 3. Run
./recomp-net-server
```

## What this bundle does and does not contain

The schema migrations, the IP→country table, and the chat filter word list are
compiled into the executable. There is nothing to install alongside it and no
path to configure for them — `migrations/` ships only so the schema is readable.

State the server creates for itself, relative to the working directory:

- `recomp-net-server.db` — SQLite, unless `DATABASE_URL` says otherwise.

`.env` is **not** in the bundle. Secrets do not travel in a ZIP; copy the
server's own `.env` in separately, or edit `.env.example` in place on the box.

## If it will not start

- `version 'GLIBC_2.xx' not found` — the bundle was built against a newer glibc
  than this server has. Rebuild with `tools/bundle.sh --glibc-max <server's
  glibc>`, which will refuse rather than hand you a broken bundle, or build
  static with `--target x86_64-unknown-linux-musl`.
- `Permission denied` — `chmod +x recomp-net-server`.
- `failed to connect database` — the working directory is not writable, or
  `DATABASE_URL` points somewhere that is not.

Check the server's glibc with `ldd --version`.
EOF

# ------------------------------------------------------------------ zip ------

mkdir -p "$OUT_DIR"
ZIP="$OUT_DIR/$NAME.zip"
rm -f "$ZIP"
( cd "$STAGE" && zip -qr "$ZIP" "$NAME" )

note "wrote $ZIP ($(du -h "$ZIP" | cut -f1))"
printf '    sha256(binary) %s\n' "$BIN_SHA"
printf '\n    scp %s server:~/\n' "${ZIP/#$REPO_ROOT\//}"
printf '    ssh server '\''unzip -o %s.zip && cd %s && ./recomp-net-server'\''\n' "$NAME" "$NAME"
