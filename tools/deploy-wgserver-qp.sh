#!/usr/bin/env bash
set -Eeuo pipefail

SRC_DIR="/root/work/wgServer-qp/wgServer"
DIST_DIR="/root/work/wgServer-qp/wgServer/dist-cjs"
ECOSYSTEM_FILE="/root/work/wgServer-qp/wgServer/ecosystem.config.cjs"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command git
require_command npm
require_command pnpm
require_command pm2

if [[ ! -d "$SRC_DIR" ]]; then
  echo "Source directory not found: $SRC_DIR" >&2
  exit 1
fi

if [[ ! -f "$ECOSYSTEM_FILE" ]]; then
  echo "Ecosystem config not found: $ECOSYSTEM_FILE" >&2
  exit 1
fi

log "Entering source directory: $SRC_DIR"
cd "$SRC_DIR"

log "Updating source code with git pull"
git pull

log "Building project: npm run build:pro"
npm run build:pro

if [[ ! -d "$DIST_DIR" ]]; then
  echo "Build output directory not found: $DIST_DIR" >&2
  exit 1
fi

log "Copying ecosystem config to dist directory"
cp "$ECOSYSTEM_FILE" "$DIST_DIR/"

log "Entering dist directory: $DIST_DIR"
cd "$DIST_DIR"

log "Installing bytenode with pnpm"
pnpm i bytenode

log "Restarting PM2 processes"
pm2 restart all --update-env

log "Deploy completed successfully"
