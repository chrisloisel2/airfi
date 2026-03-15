#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_scan_inbox.sh
#
# Monte l'inbox SFTP via sshfs, exécute scan_inbox.py, puis démonte.
# Cible : Debian 12
#
# Prérequis :
#   apt-get install -y sshfs sshpass python3
#
# Usage : bash run_scan_inbox.sh [options supplémentaires pour scan_inbox.py]
# ---------------------------------------------------------------------------
set -euo pipefail

# ── Config SFTP ──────────────────────────────────────────────────────────────
SPOOL_HOST="192.168.88.5"
SPOOL_PORT=22
SPOOL_USER="exoria"
SPOOL_PASSWORD="Admin123456"
SPOOL_SFTP_INBOX_BASE="/srv/exoria/inbox"

# ── Chemins locaux ───────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOUNT_POINT="/tmp/exoria_inbox_mount"
REPORT_PATH="/tmp/scan_inbox_report.json"

# ── Couleurs log ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Installation automatique des dépendances ─────────────────────────────────
install_deps() {
  local pkgs=()
  command -v sshfs   &>/dev/null || pkgs+=(sshfs)
  command -v sshpass &>/dev/null || pkgs+=(sshpass)
  command -v python3 &>/dev/null || pkgs+=(python3)

  if [[ ${#pkgs[@]} -eq 0 ]]; then
    return 0
  fi

  log_info "Paquets manquants : ${pkgs[*]} — installation en cours..."

  if [[ $EUID -ne 0 ]]; then
    if ! command -v sudo &>/dev/null; then
      log_error "sudo introuvable et script non lancé en root. Installez manuellement : apt-get install -y ${pkgs[*]}"
      exit 1
    fi
    sudo apt-get install -y "${pkgs[@]}"
  else
    apt-get install -y "${pkgs[@]}"
  fi

  log_info "Dépendances installées."
}

# ── Nettoyage au exit ─────────────────────────────────────────────────────────
cleanup() {
  if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    log_info "Démontage de $MOUNT_POINT..."
    fusermount -u "$MOUNT_POINT" 2>/dev/null || umount "$MOUNT_POINT" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ── Montage SSHFS ─────────────────────────────────────────────────────────────
mount_inbox() {
  mkdir -p "$MOUNT_POINT"

  # Démonte si déjà monté (relance propre)
  if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    log_warn "Point de montage déjà actif, démontage préalable..."
    fusermount -u "$MOUNT_POINT" 2>/dev/null || umount "$MOUNT_POINT" 2>/dev/null || true
    sleep 1
  fi

  log_info "Montage SFTP : ${SPOOL_USER}@${SPOOL_HOST}:${SPOOL_SFTP_INBOX_BASE} → ${MOUNT_POINT}"

  sshpass -p "$SPOOL_PASSWORD" sshfs \
    "${SPOOL_USER}@${SPOOL_HOST}:${SPOOL_SFTP_INBOX_BASE}" \
    "$MOUNT_POINT" \
    -p "$SPOOL_PORT" \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o reconnect \
    -o ServerAliveInterval=15 \
    -o ServerAliveCountMax=3

  log_info "Montage OK."
}

# ── Lancement du scan ─────────────────────────────────────────────────────────
run_scan() {
  log_info "Démarrage du scan → rapport : ${REPORT_PATH}"

  python3 "${SCRIPT_DIR}/scan_inbox.py" \
    --inbox          "$MOUNT_POINT" \
    --report         "$REPORT_PATH" \
    --stable-seconds 120 \
    --video-name     video.mp4 \
    --metadata-name  metadata.json \
    --manifest-name  manifest.json \
    --min-video-bytes 1024 \
    --session-prefix session_ \
    --log-level      INFO \
    "$@"

  local rc=$?
  if [[ $rc -eq 0 ]]; then
    log_info "Scan terminé avec succès."
    if [[ -f "$REPORT_PATH" ]]; then
      echo ""
      log_info "Résumé du rapport :"
      python3 - <<'PYEOF'
import json
try:
    with open("/tmp/scan_inbox_report.json") as f:
        r = json.load(f)
    print(f"  Sessions totales : {r['total_sessions']}")
    for status, count in sorted(r['counts_by_status'].items()):
        print(f"  {status:12s} : {count}")
except Exception as e:
    print(f"  (impossible de lire le rapport : {e})")
PYEOF
    fi
  else
    log_error "scan_inbox.py a retourné le code $rc."
  fi
  return $rc
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  install_deps
  mount_inbox
  run_scan "$@"
}

main "$@"
