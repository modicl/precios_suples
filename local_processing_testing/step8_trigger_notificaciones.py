"""
step8_trigger_notificaciones.py — Dispara el procesamiento de alertas de precio.

Hace un POST al backend de producción para que evalúe y envíe notificaciones
a usuarios con alertas configuradas. Este step siempre apunta a producción:
el backend tiene su propia conexión a Neon y procesa los precios del día.

Variables de entorno requeridas:
  BACKEND_URL          — URL base del backend en Cloud Run
  INTERNAL_API_SECRET  — Secret compartido con el backend

Si alguna variable falta, el step termina con exit 0 (no aborta el pipeline).
"""

import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL     = os.environ.get("BACKEND_URL", "").strip()
INTERNAL_SECRET = os.environ.get("INTERNAL_API_SECRET", "").strip()


def main():
    if not BACKEND_URL or not INTERNAL_SECRET:
        print(
            "[step8] BACKEND_URL o INTERNAL_API_SECRET no configuradas. "
            "Notificaciones omitidas."
        )
        sys.exit(0)

    endpoint = f"{BACKEND_URL}/api/v1/notificaciones/process"
    print(f"[step8] POST {endpoint}")

    try:
        resp = requests.post(
            endpoint,
            headers={"x-comparafit-signature": INTERNAL_SECRET},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        procesados    = data.get("procesados", "?")
        notificaciones = data.get("notificaciones", "?")
        print(
            f"[step8] OK — alertas evaluadas: {procesados} | "
            f"notificaciones enviadas: {notificaciones}"
        )
    except requests.exceptions.HTTPError as e:
        print(f"[step8] Error HTTP {e.response.status_code}: {e.response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"[step8] Error inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
