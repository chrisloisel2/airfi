#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
annotate_run.py

Produit les annotations à partir de la vidéo nettoyée.
Bootstrap: génère un annotations.json de structure stable.
Remplacer fake_annotation_engine() par le vrai moteur ML/CV.

Code retour: 0 succès / 1 échec
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from common import (
    append_error,
    append_history,
    load_json,
    parse_args,
    require_file,
    save_json,
    setup_logger,
)


def fake_annotation_engine(video_path: Path, run_id: str) -> dict:
    """
    Bootstrap minimal — remplacer par le vrai moteur d'annotation.
    """
    return {
        "run_id": run_id,
        "format": "custom_v1",
        "video_path": str(video_path),
        "annotations": [
            {"frame_index": 0, "timestamp_sec": 0.0, "objects": []}
        ],
        "summary": {
            "frame_count_estimate": None,
            "object_count": 0,
            "label_count": 0,
        },
    }


def main() -> int:
    args = parse_args("Annotation d'un run")
    logger = setup_logger("annotate_run", args.log_level)

    manifest_path = Path(args.manifest)
    manifest = load_json(manifest_path)

    try:
        append_history(manifest, "annotation", "Début de l'annotation")
        save_json(manifest_path, manifest)

        if not manifest["checks"].get("cleanup_ok"):
            raise RuntimeError("L'épuration doit être OK avant l'annotation")

        clean_video_path = Path(manifest["paths"]["clean_video_path"])
        require_file(clean_video_path, "Vidéo nettoyée")

        processed_dir = clean_video_path.parent
        annotation_path = processed_dir / "annotations.json"

        annotations = fake_annotation_engine(clean_video_path, manifest["run_id"])
        annotation_path.write_text(json.dumps(annotations, indent=2, ensure_ascii=False), encoding="utf-8")

        manifest["paths"]["annotation_path"] = str(annotation_path)
        manifest["checks"]["annotation_ok"] = True
        manifest["status"] = "annotation_ok"

        append_history(manifest, "annotation", "Annotation terminée", {
            "annotation_path": str(annotation_path),
            "format": annotations["format"],
        })

        save_json(manifest_path, manifest)
        logger.info("Run %s annoté → %s", manifest["run_id"], annotation_path)
        return 0

    except Exception as exc:
        logger.exception("Échec de l'annotation pour %s", manifest.get("run_id", "?"))
        manifest["checks"]["annotation_ok"] = False
        manifest["status"] = "annotation_failed"
        append_error(manifest, "annotation", str(exc))
        save_json(manifest_path, manifest)
        return 1


if __name__ == "__main__":
    sys.exit(main())
