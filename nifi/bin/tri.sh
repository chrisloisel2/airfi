#!/usr/bin/env bash

set -Eeuo pipefail

NAS="exoria@192.168.88.5"

ssh -tt "$NAS" <<'EOF'
set -Eeuo pipefail

sudo bash <<'ROOTSCRIPT'
set -Eeuo pipefail
shopt -s nullglob

INBOX="/srv/exoria/inbox/select"
VIDEO_DIR="$INBOX/video"
FRAMES_DIR="$INBOX/frames"
ZIP_DIR="$INBOX/zip"
LOCK_FILE="/var/lock/exoria_inbox_sort.lock"

log() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

fail() {
    log "ERREUR: $*"
    exit 1
}

mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
flock -n 9 || fail "Une autre execution est deja en cours"

mkdir -p "$VIDEO_DIR" "$FRAMES_DIR" "$ZIP_DIR"

is_video_session() {
    local dir="$1"
    find "$dir" -type f \( \
        -iname '*.mp4' -o \
        -iname '*.mov' -o \
        -iname '*.mkv' -o \
        -iname '*.avi' -o \
        -iname '*.mxf' -o \
        -iname '*.webm' \
    \) -print -quit | grep -q .
}

is_frames_session() {
    local dir="$1"
    find "$dir" -type f \( \
        -iname '*.jpg' -o \
        -iname '*.jpeg' -o \
        -iname '*.png' -o \
        -iname '*.tif' -o \
        -iname '*.tiff' -o \
        -iname '*.bmp' -o \
        -iname '*.webp' -o \
        -iname '*.exr' -o \
        -iname '*.dpx' \
    \) -print -quit | grep -q .
}

move_unique() {
    local src="$1"
    local dst_dir="$2"
    local base
    base="$(basename "$src")"

    if [ -e "$dst_dir/$base" ]; then
        fail "Collision detectee: $dst_dir/$base existe deja"
    fi

    mv "$src" "$dst_dir/"
    log "Deplace: $src -> $dst_dir/$base"
}

log "Debut du tri dans $INBOX"

for item in "$INBOX"/*; do
    [ -e "$item" ] || continue

    base="$(basename "$item")"

    case "$base" in
        video|frames|zip)
            continue
            ;;
    esac

    if [ -f "$item" ]; then
        case "${base,,}" in
            *.zip|*.tar|*.tar.gz|*.tgz|*.7z|*.rar)
                move_unique "$item" "$ZIP_DIR"
                ;;
        esac
        continue
    fi

    if [ -d "$item" ]; then
        has_video=0
        has_frames=0

        if is_video_session "$item"; then
            has_video=1
        fi

        if is_frames_session "$item"; then
            has_frames=1
        fi

        if [ "$has_video" -eq 1 ]; then
            move_unique "$item" "$VIDEO_DIR"
            continue
        fi

        if [ "$has_frames" -eq 1 ]; then
            move_unique "$item" "$FRAMES_DIR"
            continue
        fi

        log "Ignore: aucun contenu video/frame detecte dans $item"
    fi
done

log "Tri termine"
ROOTSCRIPT
EOF
