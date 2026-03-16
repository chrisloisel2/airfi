#!/usr/bin/env bash

set -euo pipefail

NAS="exoria@192.168.88.5"
ROOT_DIR="/srv/exoria/inbox/select"
OUT_DIR="/srv/exoria/inbox/hourly_archives"

ssh "$NAS" 'bash -s' <<'EOF'
set -euo pipefail

ROOT_DIR="/srv/exoria/inbox/select"
OUT_DIR="/srv/exoria/inbox/hourly_archives"

mkdir -p "$OUT_DIR"

declare -A groups

for d in "$ROOT_DIR"/session_*; do
    [ -d "$d" ] || continue

    name=$(basename "$d")

    key=$(echo "$name" | sed -E "s/session_([0-9]{8})_([0-9]{2}).*/\1_\2/")

    groups["$key"]+="$d "
done

for key in "${!groups[@]}"; do
    archive="$OUT_DIR/sessions_${key}.tar.gz"

    tar -czf "$archive" ${groups[$key]}

    echo "Archive créée : $archive"
done
EOF
