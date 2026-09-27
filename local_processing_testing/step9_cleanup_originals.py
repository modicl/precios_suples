"""
step9_cleanup_originals.py — Elimina imágenes de assets/img/originals/ en S3
que ya fueron procesadas por Lambda (tienen su equivalente en assets/img/resized/).

Por defecto solo borra un original si existe el archivo correspondiente en resized/.
Los archivos sin versión resized (Lambda aún no los procesó) se omiten con advertencia.

Flags:
  --dry-run   Muestra qué se borraría sin ejecutar ninguna eliminación en S3.
  --force     Borra todos los originals sin verificar si existen en resized/.

Variables de entorno requeridas:
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_BUCKET_NAME  (default: suplescrapper-images)
  AWS_REGION       (default: us-east-2)
"""

import os
import sys

import boto3
from dotenv import load_dotenv

load_dotenv()

BUCKET      = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")
REGION      = os.getenv("AWS_REGION", "us-east-2")
ORIGINALS   = "assets/img/originals/"
RESIZED     = "assets/img/resized/"
BATCH_SIZE  = 1000  # límite de S3 delete_objects


def build_s3_client() -> boto3.client:
    key    = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not key or not secret:
        print("[step9] ERROR: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY no configuradas.")
        sys.exit(1)
    return boto3.client("s3", aws_access_key_id=key, aws_secret_access_key=secret, region_name=REGION)


def list_keys(s3, prefix: str) -> set[str]:
    """Retorna el conjunto de todas las keys bajo un prefix."""
    keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return keys


def delete_batch(s3, keys: list[str], dry_run: bool) -> int:
    """Borra un lote de keys (máx 1000). Retorna cantidad borrada."""
    if dry_run:
        for k in keys:
            print(f"  [DRY-RUN] borraría: {k}")
        return len(keys)

    objects = [{"Key": k} for k in keys]
    resp = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objects, "Quiet": True})
    errors = resp.get("Errors", [])
    if errors:
        for err in errors:
            print(f"  [WARN] No se pudo borrar {err['Key']}: {err['Message']}")
    deleted = len(objects) - len(errors)
    return deleted


def main():
    dry_run = "--dry-run" in sys.argv
    force   = "--force"   in sys.argv

    if dry_run:
        print("[step9] Modo DRY-RUN — no se borrará nada.")
    if force:
        print("[step9] Modo FORCE — se borran todos los originals sin verificar resized.")

    s3 = build_s3_client()

    print(f"[step9] Listando originals en s3://{BUCKET}/{ORIGINALS} ...")
    originals = list_keys(s3, ORIGINALS)
    print(f"[step9] Originals encontrados: {len(originals)}")

    if not originals:
        print("[step9] Nada que borrar. Fin.")
        return

    resized = set()
    if not force:
        print(f"[step9] Listando resized en s3://{BUCKET}/{RESIZED} para cross-check ...")
        resized = list_keys(s3, RESIZED)
        print(f"[step9] Resized encontrados: {len(resized)}")

    to_delete  = []
    skipped    = 0

    for original_key in originals:
        if force:
            to_delete.append(original_key)
            continue

        # assets/img/originals/subfolder/hash.ext → assets/img/resized/subfolder/hash.ext
        resized_key = original_key.replace(ORIGINALS, RESIZED, 1)
        if resized_key in resized:
            to_delete.append(original_key)
        else:
            skipped += 1
            print(f"  [SKIP] Sin resized aún: {original_key}")

    print(f"\n[step9] Para borrar: {len(to_delete)} | Pendientes de Lambda: {skipped}")

    if not to_delete:
        print("[step9] Nada que borrar. Fin.")
        return

    # Borrar en lotes de 1000
    total_deleted = 0
    for i in range(0, len(to_delete), BATCH_SIZE):
        batch = to_delete[i : i + BATCH_SIZE]
        deleted = delete_batch(s3, batch, dry_run)
        total_deleted += deleted
        print(f"[step9] Lote {i // BATCH_SIZE + 1}: {deleted} archivos {'(simulados)' if dry_run else 'borrados'}.")

    label = "se borrarían" if dry_run else "borrados"
    print(f"\n[step9] Completado — {total_deleted} originals {label}.")


if __name__ == "__main__":
    main()
