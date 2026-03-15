#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scan_inbox.py

Scanner professionnel de l'inbox NAS pour sessions vidéo.

Responsabilités:
- parcourir les dossiers de session d'un répertoire inbox
- vérifier que chaque session est stable (pas encore en cours d'écriture)
- vérifier les fichiers attendus
- valider le JSON de metadata si présent
- détecter les fichiers vides ou illisibles
- générer ou mettre à jour un manifest.json dans chaque session
- produire un rapport global JSON

Statuts possibles:
- processing : dossier trop récent ou encore en cours d'écriture
- incomplete : fichiers requis manquants
- corrupted  : fichiers présents mais invalides / vides / JSON cassé
- ready      : session exploitable
- uploaded   : session déjà marquée envoyée (checks.upload_ok ou status uploaded_to_s3)

Principes:
- idempotent
- pas de suppression
- pas de déplacement
- pas d'upload
- pas de mutation métier autre que manifest/report
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("scan_inbox")
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tmp:
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)


def load_json_if_exists(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def path_mtime_epoch(path: Path) -> float:
    return path.stat().st_mtime


def newest_mtime_in_tree(root: Path) -> float:
    newest = path_mtime_epoch(root)
    for p in root.rglob("*"):
        try:
            newest = max(newest, path_mtime_epoch(p))
        except FileNotFoundError:
            continue
    return newest


def file_size(path: Path) -> int:
    return path.stat().st_size


@dataclass
class SessionCheckResult:
    session_id: str
    session_dir: str
    status: str
    files_present: Dict[str, bool]
    files_sizes: Dict[str, Optional[int]]
    newest_mtime_epoch: float
    newest_mtime_iso: str
    stable_for_sec: float
    issues: List[str]
    warnings: List[str]
    video_path: Optional[str]
    metadata_path: Optional[str]
    manifest_path: str


def epoch_to_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan professionnel de l'inbox NAS")
    parser.add_argument("--inbox", required=True, help="Chemin du répertoire inbox sur le NAS")
    parser.add_argument(
        "--report",
        required=True,
        help="Chemin du rapport global JSON généré par le scan",
    )
    parser.add_argument(
        "--stable-seconds",
        type=int,
        default=120,
        help="Âge minimum en secondes du dossier pour le considérer stable",
    )
    parser.add_argument(
        "--video-name",
        default="video.mp4",
        help="Nom du fichier vidéo attendu dans chaque session",
    )
    parser.add_argument(
        "--metadata-name",
        default="metadata.json",
        help="Nom du fichier metadata attendu dans chaque session",
    )
    parser.add_argument(
        "--manifest-name",
        default="manifest.json",
        help="Nom du manifest de session",
    )
    parser.add_argument(
        "--allow-missing-metadata",
        action="store_true",
        help="Autorise l'absence de metadata.json sans classer la session incomplete",
    )
    parser.add_argument(
        "--min-video-bytes",
        type=int,
        default=1024,
        help="Taille minimale attendue du fichier vidéo",
    )
    parser.add_argument(
        "--session-prefix",
        default="session_",
        help="Préfixe attendu des dossiers de session",
    )
    parser.add_argument(
        "--recursive-depth",
        type=int,
        default=1,
        help="Profondeur de scan. 1 = sous-dossiers directs de inbox",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="DEBUG, INFO, WARNING, ERROR",
    )
    return parser.parse_args()


def iter_session_dirs(inbox: Path, depth: int) -> List[Path]:
    if depth != 1:
        raise ValueError("Cette version professionnelle supporte recursive-depth=1 uniquement.")
    return sorted([p for p in inbox.iterdir() if p.is_dir()])


def validate_metadata_json(path: Path) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return False, "metadata.json doit contenir un objet JSON", None
        return True, None, data
    except Exception as exc:
        return False, f"metadata.json invalide: {exc}", None


def build_or_update_manifest(
    session_dir: Path,
    manifest_path: Path,
    result: SessionCheckResult,
    metadata_json: Optional[Dict[str, Any]],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    existing = load_json_if_exists(manifest_path) or {}

    manifest: Dict[str, Any] = {
        "run_id": existing.get("run_id"),
        "session_id": result.session_id,
        "project_id": existing.get("project_id"),
        "site_id": existing.get("site_id"),
        "source_stage": "inbox",
        "status": result.status,
        "scanner": {
            "host": socket.gethostname(),
            "scanned_at": utc_now_iso(),
            "stable_seconds_threshold": args.stable_seconds,
            "min_video_bytes": args.min_video_bytes,
            "script": "scan_inbox.py",
            "version": "1.0.0",
        },
        "paths": {
            "root_dir": str(session_dir),
            "video_path": result.video_path,
            "metadata_path": result.metadata_path,
            "clean_video_path": existing.get("paths", {}).get("clean_video_path"),
            "annotation_path": existing.get("paths", {}).get("annotation_path"),
            "report_path": existing.get("paths", {}).get("report_path"),
        },
        "video": {
            "expected_fps": existing.get("video", {}).get("expected_fps"),
            "expected_min_duration_sec": existing.get("video", {}).get("expected_min_duration_sec"),
            "expected_max_duration_sec": existing.get("video", {}).get("expected_max_duration_sec"),
            "codec": existing.get("video", {}).get("codec"),
            "duration_sec": existing.get("video", {}).get("duration_sec"),
            "size_bytes": result.files_sizes.get(args.video_name),
            "width": existing.get("video", {}).get("width"),
            "height": existing.get("video", {}).get("height"),
        },
        "checks": {
            "verification_ok": existing.get("checks", {}).get("verification_ok", False),
            "cleanup_ok": existing.get("checks", {}).get("cleanup_ok", False),
            "annotation_ok": existing.get("checks", {}).get("annotation_ok", False),
            "upload_ok": existing.get("checks", {}).get("upload_ok", False),
            "scan_ok": result.status in {"ready", "uploaded"},
            "scan_status": result.status,
            "scan_issues": result.issues,
            "scan_warnings": result.warnings,
            "files_present": result.files_present,
            "files_sizes": result.files_sizes,
            "newest_mtime_epoch": result.newest_mtime_epoch,
            "newest_mtime_iso": result.newest_mtime_iso,
            "stable_for_sec": round(result.stable_for_sec, 3),
        },
        "metadata_snapshot": metadata_json,
        "s3": existing.get("s3", {
            "bucket": None,
            "prefix": None,
            "video_key": None,
            "annotation_key": None,
            "manifest_key": None,
        }),
        "history": existing.get("history", []),
        "errors": existing.get("errors", []),
    }

    history_entry = {
        "at": utc_now_iso(),
        "stage": "scan_inbox",
        "message": f"Scan inbox: status={result.status}",
        "extra": {
            "issues": result.issues,
            "warnings": result.warnings,
            "stable_for_sec": round(result.stable_for_sec, 3),
        },
    }
    manifest["history"].append(history_entry)

    return manifest


def scan_session(session_dir: Path, args: argparse.Namespace, now_epoch: float) -> Tuple[SessionCheckResult, Optional[Dict[str, Any]]]:
    session_id = session_dir.name
    video_path = session_dir / args.video_name
    metadata_path = session_dir / args.metadata_name
    manifest_path = session_dir / args.manifest_name

    issues: List[str] = []
    warnings: List[str] = []
    files_present = {
        args.video_name: video_path.exists(),
        args.metadata_name: metadata_path.exists(),
        args.manifest_name: manifest_path.exists(),
    }
    files_sizes = {
        args.video_name: file_size(video_path) if video_path.exists() and video_path.is_file() else None,
        args.metadata_name: file_size(metadata_path) if metadata_path.exists() and metadata_path.is_file() else None,
        args.manifest_name: file_size(manifest_path) if manifest_path.exists() and manifest_path.is_file() else None,
    }

    existing_manifest = load_json_if_exists(manifest_path)
    if existing_manifest:
        if existing_manifest.get("checks", {}).get("upload_ok") is True or existing_manifest.get("status") == "uploaded_to_s3":
            newest = newest_mtime_in_tree(session_dir)
            result = SessionCheckResult(
                session_id=session_id,
                session_dir=str(session_dir),
                status="uploaded",
                files_present=files_present,
                files_sizes=files_sizes,
                newest_mtime_epoch=newest,
                newest_mtime_iso=epoch_to_iso(newest),
                stable_for_sec=now_epoch - newest,
                issues=[],
                warnings=[],
                video_path=str(video_path) if video_path.exists() else None,
                metadata_path=str(metadata_path) if metadata_path.exists() else None,
                manifest_path=str(manifest_path),
            )
            return result, existing_manifest.get("metadata_snapshot")

    if not session_id.startswith(args.session_prefix):
        warnings.append(f"Nom de dossier inattendu: {session_id}")

    newest = newest_mtime_in_tree(session_dir)
    stable_for_sec = now_epoch - newest

    metadata_json: Optional[Dict[str, Any]] = None

    if stable_for_sec < args.stable_seconds:
        status = "processing"
    else:
        status = "ready"

    if not video_path.exists():
        issues.append(f"Fichier requis manquant: {args.video_name}")
        status = "incomplete"
    elif not video_path.is_file():
        issues.append(f"{args.video_name} existe mais n'est pas un fichier")
        status = "corrupted"
    else:
        size = file_size(video_path)
        if size < args.min_video_bytes:
            issues.append(f"{args.video_name} trop petit: {size} octets < {args.min_video_bytes}")
            status = "corrupted"

    if not metadata_path.exists():
        if not args.allow_missing_metadata:
            issues.append(f"Fichier requis manquant: {args.metadata_name}")
            if status != "corrupted":
                status = "incomplete"
    elif not metadata_path.is_file():
        issues.append(f"{args.metadata_name} existe mais n'est pas un fichier")
        status = "corrupted"
    else:
        valid, error_message, metadata_json = validate_metadata_json(metadata_path)
        if not valid:
            issues.append(error_message or "metadata.json invalide")
            status = "corrupted"
        else:
            for mandatory_key in ("run_id", "project_id", "site_id"):
                if mandatory_key not in metadata_json:
                    warnings.append(f"metadata.json ne contient pas la clé '{mandatory_key}'")

    if status == "ready" and issues:
        status = "corrupted"

    result = SessionCheckResult(
        session_id=session_id,
        session_dir=str(session_dir),
        status=status,
        files_present=files_present,
        files_sizes=files_sizes,
        newest_mtime_epoch=newest,
        newest_mtime_iso=epoch_to_iso(newest),
        stable_for_sec=stable_for_sec,
        issues=issues,
        warnings=warnings,
        video_path=str(video_path) if video_path.exists() else None,
        metadata_path=str(metadata_path) if metadata_path.exists() else None,
        manifest_path=str(manifest_path),
    )
    return result, metadata_json


def build_global_report(
    inbox: Path,
    results: List[SessionCheckResult],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    by_status: Dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1

    return {
        "scanner": {
            "host": socket.gethostname(),
            "scanned_at": utc_now_iso(),
            "script": "scan_inbox.py",
            "version": "1.0.0",
        },
        "inbox": str(inbox),
        "report_path": args.report,
        "stable_seconds_threshold": args.stable_seconds,
        "min_video_bytes": args.min_video_bytes,
        "total_sessions": len(results),
        "counts_by_status": by_status,
        "sessions": [asdict(r) for r in results],
    }


def main() -> int:
    args = parse_args()
    logger = setup_logger(args.log_level)

    inbox = Path(args.inbox).resolve()
    report_path = Path(args.report).resolve()

    if not inbox.exists():
        logger.error("Inbox introuvable: %s", inbox)
        return 2
    if not inbox.is_dir():
        logger.error("Inbox n'est pas un répertoire: %s", inbox)
        return 2

    now_epoch = datetime.now(timezone.utc).timestamp()
    session_dirs = iter_session_dirs(inbox, args.recursive_depth)

    logger.info("Scan de %d dossier(s) dans %s", len(session_dirs), inbox)

    results: List[SessionCheckResult] = []

    for session_dir in session_dirs:
        try:
            result, metadata_json = scan_session(session_dir, args, now_epoch)
            manifest_path = session_dir / args.manifest_name
            manifest = build_or_update_manifest(session_dir, manifest_path, result, metadata_json, args)
            atomic_write_json(manifest_path, manifest)
            results.append(result)
            logger.info("Session %s -> %s", result.session_id, result.status)
        except Exception as exc:
            logger.exception("Échec lors du scan de %s", session_dir)
            fallback = SessionCheckResult(
                session_id=session_dir.name,
                session_dir=str(session_dir),
                status="corrupted",
                files_present={},
                files_sizes={},
                newest_mtime_epoch=0.0,
                newest_mtime_iso="",
                stable_for_sec=0.0,
                issues=[f"Erreur interne du scanner: {exc}"],
                warnings=[],
                video_path=None,
                metadata_path=None,
                manifest_path=str(session_dir / args.manifest_name),
            )
            results.append(fallback)

    report = build_global_report(inbox, results, args)
    atomic_write_json(report_path, report)

    ready_count = sum(1 for r in results if r.status == "ready")
    logger.info("Scan terminé. Sessions prêtes: %d / %d", ready_count, len(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
