#!/usr/bin/env bash

set -Eeuo pipefail

NAS="exoria@192.168.88.5"

ssh "$NAS" 'bash -s' <<'EOF'
set -Eeuo pipefail
shopt -s nullglob

ROOT_DIR="/srv/exoria/inbox/"
QUEUE_DIR="/src/exoria/queu"
OUT_DIR="/srv/exoria/inbox/hourly_archives"
TMP_RAW_DIR="/tmp/exoria_sessions"
LOCK_FILE="/var/lock/exoria_hourly_pipeline.lock"

log() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

fail() {
    log "ERREUR: $*"
    exit 1
}

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        log "Pipeline interrompue avec code=$rc"
    fi
}
trap cleanup EXIT

mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
flock -n 9 || fail "Une autre execution est deja en cours"

mkdir -p "$ROOT_DIR" "$QUEUE_DIR" "$OUT_DIR" "$TMP_RAW_DIR"

declare -A groups=()

log "Phase 1: deplacement des sessions vers la queue + copie brute vers /tmp"

for d in "$ROOT_DIR"/session_*; do
    [ -d "$d" ] || continue

    name="$(basename "$d")"

    if [[ ! "$name" =~ ^session_([0-9]{8})_([0-9]{2}) ]]; then
        log "Ignore: nom non conforme: $name"
        continue
    fi

    key="${BASH_REMATCH[1]}_${BASH_REMATCH[2]}"
    queue_target="$QUEUE_DIR/$name"
    tmp_target="$TMP_RAW_DIR/$name"

    if [ -e "$queue_target" ]; then
        fail "La cible existe deja dans la queue: $queue_target"
    fi

    mv "$d" "$queue_target"

    rm -rf -- "$tmp_target"
    cp -a -- "$queue_target" "$tmp_target"

    groups["$key"]+="$name"$'\n'

    log "Session traitee: $name -> $queue_target ; copie brute -> $tmp_target"
done

if [ "${#groups[@]}" -eq 0 ]; then
    log "Aucune nouvelle session a traiter"
    exit 0
fi

log "Phase 2: creation des archives horaires"

for key in "${!groups[@]}"; do
    archive="$OUT_DIR/sessions_${key}.tar.gz"
    tmp_archive="$archive.tmp.$$"
    manifest="$OUT_DIR/sessions_${key}.manifest.txt"
    checksum="$OUT_DIR/sessions_${key}.sha256"

    mapfile -t entries < <(printf '%s' "${groups[$key]}" | sed '/^$/d' | sort -u)

    [ "${#entries[@]}" -gt 0 ] || continue

    for entry in "${entries[@]}"; do
        [ -d "$QUEUE_DIR/$entry" ] || fail "Session absente au moment du tar: $QUEUE_DIR/$entry"
    done

    tar -C "$QUEUE_DIR" -czf "$tmp_archive" "${entries[@]}"
    mv -f "$tmp_archive" "$archive"

    {
        printf 'archive=%s\n' "$archive"
        printf 'created_at=%s\n' "$(date '+%F %T')"
        printf 'group=%s\n' "$key"
        printf 'sessions_count=%s\n' "${#entries[@]}"
        printf 'sessions:\n'
        printf ' - %s\n' "${entries[@]}"
    } > "$manifest"

    sha256sum "$archive" > "$checksum"

    log "Archive creee: $archive"
    log "Manifest: $manifest"
    log "Checksum: $checksum"
done

log "Phase 3: purge du brut /tmp programmee a minuit via tmpfiles/systemd si configure cote hote"
log "Brut disponible dans: $TMP_RAW_DIR"
EOF
