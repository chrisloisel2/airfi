#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
cleanup_run.py

Nettoyage et préparation du run avant annotation.
- Copie (ou normalise via ffmpeg) la vidéo source en clean.mp4
- Génère cleanup_report.json
- Prépare le dossier processed/

Code retour: 0 succès / 1 échec
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from common import (
    append_error,
    append_history,
    ensure_dir,
    load_json,
    parse_args,
    require_file,
    save_json,
    setup_logger,
)


def main() -> int:
    args = parse_args("Épuration d'un run")
    logger = setup_logger("cleanup_run", args.log_level)

    manifest_path = Path(args.manifest)
    manifest = load_json(manifest_path)

    try:
        append_history(manifest, "cleanup", "Début de l'épuration")
        save_json(manifest_path, manifest)

        if not manifest["checks"].get("verification_ok"):
            raise RuntimeError("La vérification doit être OK avant l'épuration")

        root_dir = Path(manifest["paths"]["root_dir"])
        video_path = Path(manifest["paths"]["video_path"])
        require_file(video_path, "Vidéo source")

        processed_dir = Path(str(root_dir).replace("/work/", "/processed/"))
        ensure_dir(processed_dir)

        clean_video_path = processed_dir / "clean.mp4"
        report_path = processed_dir / "cleanup_report.json"
        new_manifest_path = processed_dir / "manifest.json"

        # Bootstrap: copie directe. Remplacer par ffmpeg/filtrage/normalisation selon besoin.
        shutil.copy2(video_path, clean_video_path)

        cleanup_report = {
            "ok": True,
            "run_id": manifest["run_id"],
            "source_video": str(video_path),
            "clean_video": str(clean_video_path),
            "operations": ["copy_source_video_to_clean_output"],
            "removed_items": 0,
            "warnings": [],
        }
        report_path.write_text(json.dumps(cleanup_report, indent=2, ensure_ascii=False), encoding="utf-8")

        manifest["paths"]["clean_video_path"] = str(clean_video_path)
        manifest["paths"]["report_path"] = str(report_path)
        manifest["paths"]["root_dir"] = str(processed_dir)
        manifest["checks"]["cleanup_ok"] = True
        manifest["status"] = "cleanup_ok"
        manifest["source_stage"] = "processed"

        append_history(manifest, "cleanup", "Épuration terminée", {
            "processed_dir": str(processed_dir),
            "clean_video_path": str(clean_video_path),
        })

        save_json(new_manifest_path, manifest)
        logger.info("Run %s épuré → %s", manifest["run_id"], processed_dir)
        return 0

    except Exception as exc:
        logger.exception("Échec de l'épuration pour %s", manifest.get("run_id", "?"))
        manifest["checks"]["cleanup_ok"] = False
        manifest["status"] = "cleanup_failed"
        append_error(manifest, "cleanup", str(exc))
        save_json(manifest_path, manifest)
        return 1


if __name__ == "__main__":
    sys.exit(main())
