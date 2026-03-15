#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
upload_s3.py

Upload vers S3 des artefacts finaux: clean.mp4, annotations.json, manifest.json.

Prérequis: variables AWS dans l'environnement (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) ou rôle IAM attaché.

Code retour: 0 succès / 1 échec
"""

from __future__ import annotations

import mimetypes
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from common import (
    append_error,
    append_history,
    load_json,
    parse_args,
    require_file,
    save_json,
    setup_logger,
)


def upload_file(s3_client, bucket: str, local_path: Path, key: str) -> None:
    content_type, _ = mimetypes.guess_type(str(local_path))
    extra = {"ContentType": content_type} if content_type else {}
    s3_client.upload_file(str(local_path), bucket, key, ExtraArgs=extra if extra else None)


def main() -> int:
    args = parse_args("Upload S3 d'un run")
    logger = setup_logger("upload_s3", args.log_level)

    manifest_path = Path(args.manifest)
    manifest = load_json(manifest_path)

    try:
        append_history(manifest, "upload_s3", "Début de l'upload S3")
        save_json(manifest_path, manifest)

        if not manifest["checks"].get("annotation_ok"):
            raise RuntimeError("L'annotation doit être OK avant l'upload S3")

        bucket = manifest["s3"]["bucket"]
        prefix = manifest["s3"]["prefix"]

        clean_video_path = Path(manifest["paths"]["clean_video_path"])
        annotation_path = Path(manifest["paths"]["annotation_path"])
        require_file(clean_video_path, "Vidéo nettoyée")
        require_file(annotation_path, "Annotations")
        require_file(manifest_path, "Manifest")

        video_key = f"{prefix}/clean.mp4"
        annotation_key = f"{prefix}/annotations.json"
        manifest_key = f"{prefix}/manifest.json"

        s3 = boto3.client("s3")
        upload_file(s3, bucket, clean_video_path, video_key)
        logger.info("Uploadé: s3://%s/%s", bucket, video_key)
        upload_file(s3, bucket, annotation_path, annotation_key)
        logger.info("Uploadé: s3://%s/%s", bucket, annotation_key)
        upload_file(s3, bucket, manifest_path, manifest_key)
        logger.info("Uploadé: s3://%s/%s", bucket, manifest_key)

        manifest["s3"].update({
            "video_key": video_key,
            "annotation_key": annotation_key,
            "manifest_key": manifest_key,
        })
        manifest["checks"]["upload_ok"] = True
        manifest["status"] = "uploaded_to_s3"

        append_history(manifest, "upload_s3", "Upload S3 terminé", {
            "bucket": bucket,
            "video_key": video_key,
            "annotation_key": annotation_key,
            "manifest_key": manifest_key,
        })

        save_json(manifest_path, manifest)
        logger.info("Run %s uploadé → s3://%s/%s", manifest["run_id"], bucket, prefix)
        return 0

    except (BotoCoreError, ClientError) as exc:
        logger.error("Erreur AWS S3: %s", exc)
        manifest["checks"]["upload_ok"] = False
        manifest["status"] = "upload_failed"
        append_error(manifest, "upload_s3", str(exc))
        save_json(manifest_path, manifest)
        return 1
    except Exception as exc:
        logger.exception("Échec upload S3 pour %s", manifest.get("run_id", "?"))
        manifest["checks"]["upload_ok"] = False
        manifest["status"] = "upload_failed"
        append_error(manifest, "upload_s3", str(exc))
        save_json(manifest_path, manifest)
        return 1


if __name__ == "__main__":
    sys.exit(main())
