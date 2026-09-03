#!/usr/bin/env bash
# Build the wasm module and the static site.
#
# BASE_PATH is what GitHub Pages needs when the site lives under a repository
# subpath: every asset URL has to be relative to it, and Vite bakes that in at
# build time.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> wasm"
wasm-pack build crates/wasm --release --target web \
  --out-dir ../../web/src/pkg --out-name raftsim

echo "==> web"
cd web
npm ci --no-audit --no-fund 2>/dev/null || npm install --no-audit --no-fund
BASE_PATH="${BASE_PATH:-/}" npm run build

echo "==> done: web/dist"
