#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
verify_run.py

Vérifie qu'un run reçu sur le NAS est valide avant traitement.
- Contrôle présence et intégrité des fichiers
- Extrait les métadonnées vidéo via ffprobe
- Déplace le run de inbox vers work si OK

Code retour: 0 succès / 1 échec
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from common import (
    append_error,
    append_history,
    ensure_dir,
    load_json,
    move_tree,
    parse_args,
    require_file,
    save_json,
    setup_logger,
)


def ffprobe_video(video_path: Path) -> dict:
    cmd = [
        "ffprobe", "-v", "error",
        "-print_format", "json",
        "-show_format", "-show_streams",
        str(video_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def extract_video_info(probe: dict) -> dict:
    streams = probe.get("streams", [])
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    if not video_stream:
        raise ValueError("Aucun flux vidéo détecté")

    fmt = probe.get("format", {})
    duration = float(fmt.get("duration", 0) or 0)
    size_bytes = int(fmt.get("size", 0) or 0)

    num, den = video_stream.get("r_frame_rate", "0/1").split("/")
    fps = float(num) / float(den) if float(den) != 0 else 0

    return {
        "codec": video_stream.get("codec_name"),
        "duration_sec": duration,
        "size_bytes": size_bytes,
        "width": int(video_stream.get("width", 0) or 0),
        "height": int(video_stream.get("height", 0) or 0),
        "fps": fps,
    }


def main() -> int:
    args = parse_args("Vérification d'un run vidéo")
    logger = setup_logger("verify_run", args.log_level)

    manifest_path = Path(args.manifest)
    manifest = load_json(manifest_path)

    try:
        append_history(manifest, "verification", "Début de la vérification")
        save_json(manifest_path, manifest)

        run_id = manifest["run_id"]
        root_dir = Path(manifest["paths"]["root_dir"])
        video_path = Path(manifest["paths"]["video_path"])

        require_file(manifest_path, "Manifest")
        require_file(video_path, "Vidéo")

        metadata_path = manifest["paths"].get("metadata_path")
        if metadata_path:
            require_file(Path(metadata_path), "Metadata")

        probe = ffprobe_video(video_path)
        info = extract_video_info(probe)

        min_dur = float(manifest["video"].get("expected_min_duration_sec", 0) or 0)
        max_dur = float(manifest["video"].get("expected_max_duration_sec", 999999) or 999999)
        expected_fps = float(manifest["video"].get("expected_fps", 0) or 0)

        if info["size_bytes"] <= 0:
            raise ValueError("Fichier vidéo vide")
        if info["duration_sec"] < min_dur:
            raise ValueError(f"Durée trop courte: {info['duration_sec']:.1f}s < {min_dur}s")
        if info["duration_sec"] > max_dur:
            raise ValueError(f"Durée trop longue: {info['duration_sec']:.1f}s > {max_dur}s")
        if expected_fps and abs(info["fps"] - expected_fps) > 1.0:
            raise ValueError(f"FPS inattendu: {info['fps']:.2f} au lieu de {expected_fps}")

        manifest["video"].update({
            "codec": info["codec"],
            "duration_sec": info["duration_sec"],
            "size_bytes": info["size_bytes"],
            "width": info["width"],
            "height": info["height"],
        })
        manifest["checks"]["verification_ok"] = True
        manifest["status"] = "verification_ok"
        append_history(manifest, "verification", "Vérification réussie", info)
        save_json(manifest_path, manifest)

        if "/inbox/" in str(root_dir):
            work_dir = Path(str(root_dir).replace("/inbox/", "/work/"))
            ensure_dir(work_dir.parent)
            move_tree(root_dir, work_dir)

            manifest["paths"]["root_dir"] = str(work_dir)
            manifest["paths"]["video_path"] = str(work_dir / video_path.name)
            if metadata_path:
                manifest["paths"]["metadata_path"] = str(work_dir / Path(metadata_path).name)
            manifest["source_stage"] = "work"
            append_history(manifest, "verification", "Run déplacé vers work", {"work_dir": str(work_dir)})

            save_json(work_dir / manifest_path.name, manifest)
            logger.info("Run %s validé → %s", run_id, work_dir)
        else:
            save_json(manifest_path, manifest)
            logger.info("Run %s validé (déjà hors inbox)", run_id)

        return 0

    except Exception as exc:
        logger.exception("Échec de vérification pour %s", manifest.get("run_id", "?"))
        manifest["checks"]["verification_ok"] = False
        manifest["status"] = "verification_failed"
        append_error(manifest, "verification", str(exc))
        save_json(manifest_path, manifest)
        return 1


if __name__ == "__main__":
    sys.exit(main())
