"""
retrigger_lambda_originals.py
------------------------------
Re-triggeriza el Lambda de procesamiento de imágenes copiando cada objeto
de originals/ sobre sí mismo en S3 (copy-in-place).

Esto genera un nuevo PUT event sin necesidad de re-scrapear ni mover archivos.
Útil después de deployar un fix al Lambda y querer re-procesar imágenes ya subidas.

Uso:
    python tools/retrigger_lambda_originals.py
    python tools/retrigger_lambda_originals.py --subfolder chilesuplementos
    python tools/retrigger_lambda_originals.py --subfolder chilesuplementos --dry-run
"""

import argparse
import os
import sys
import time
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

BUCKET      = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")
PREFIX_BASE = "assets/img/originals/"


def retrigger(subfolder: str | None, dry_run: bool, delay: float):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-2"),
    )

    prefix = PREFIX_BASE + (f"{subfolder}/" if subfolder else "")
    print(f"Listando objetos en s3://{BUCKET}/{prefix} ...")

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                keys.append(key)

    print(f"Encontrados: {len(keys)} archivos de imagen.")
    if not keys:
        print("Nada que procesar.")
        return

    if dry_run:
        print(f"\n[DRY RUN] Se copiarían {len(keys)} objetos:")
        for k in keys[:10]:
            print(f"  {k}")
        if len(keys) > 10:
            print(f"  ... y {len(keys) - 10} más.")
        return

    print(f"\nCopiando {len(keys)} objetos sobre sí mismos para re-triggerizar el Lambda ...")
    ok = 0
    errors = 0
    for i, key in enumerate(keys, 1):
        try:
            s3.copy_object(
                Bucket=BUCKET,
                CopySource={"Bucket": BUCKET, "Key": key},
                Key=key,
                MetadataDirective="COPY",
            )
            print(f"  [{i:4d}/{len(keys)}] OK  {key}")
            ok += 1
        except Exception as e:
            print(f"  [{i:4d}/{len(keys)}] ERR {key}: {e}")
            errors += 1

        if delay > 0 and i < len(keys):
            time.sleep(delay)

    print(f"\nListo. {ok} copiados, {errors} errores.")
    print("El Lambda procesará cada imagen en background.")


def main():
    parser = argparse.ArgumentParser(
        description="Re-triggeriza el Lambda copiando imágenes originals/ sobre sí mismas."
    )
    parser.add_argument(
        "--subfolder", type=str, default=None,
        help="Subcarpeta dentro de originals/ (ej: chilesuplementos). Por defecto procesa todos los sitios."
    )
    parser.add_argument(
        "--all-sites", action="store_true",
        help="Procesar todos los sitios (equivalente a no pasar --subfolder)."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Solo listar, no copiar."
    )
    parser.add_argument(
        "--delay", type=float, default=0.05,
        help="Segundos entre copias para no saturar el Lambda (default: 0.05)."
    )
    args = parser.parse_args()

    subfolder = None if args.all_sites else args.subfolder
    retrigger(subfolder, args.dry_run, args.delay)


if __name__ == "__main__":
    main()
