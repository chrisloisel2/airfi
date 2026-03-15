#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
common.py
Fonctions partagées par tous les scripts du pipeline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
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


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def move_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        raise FileExistsError(f"Destination already exists: {dst}")
    shutil.move(str(src), str(dst))


def append_history(
    manifest: Dict[str, Any],
    stage: str,
    message: str,
    extra: Dict[str, Any] | None = None,
) -> None:
    entry: Dict[str, Any] = {"at": utc_now_iso(), "stage": stage, "message": message}
    if extra:
        entry["extra"] = extra
    manifest.setdefault("history", []).append(entry)


def append_error(
    manifest: Dict[str, Any],
    stage: str,
    message: str,
    extra: Dict[str, Any] | None = None,
) -> None:
    entry: Dict[str, Any] = {"at": utc_now_iso(), "stage": stage, "message": message}
    if extra:
        entry["extra"] = extra
    manifest.setdefault("errors", []).append(entry)


def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} introuvable: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"{label} n'est pas un fichier: {path}")


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--manifest", required=True, help="Chemin complet vers le manifest JSON")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return parser.parse_args()
