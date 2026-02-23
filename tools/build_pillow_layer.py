"""
build_pillow_layer.py
----------------------
Construye un ZIP listo para subir como Lambda Layer con Pillow actualizado.
Requiere Docker instalado (usa la imagen de AWS Lambda para compatibilidad exacta).

Si no tienes Docker, usa el metodo alternativo comentado al final.

Uso:
    python tools/build_pillow_layer.py
    
Output:
    pillow_layer.zip  (en la raiz del proyecto)
"""

import subprocess
import sys
import os

PILLOW_VERSION = "11.1.0"  # ultima estable compatible con python3.12
OUTPUT_ZIP = "pillow_layer.zip"


def build_with_docker():
    print(f"Construyendo layer con Pillow {PILLOW_VERSION} via Docker (python3.12 / x86_64)...")
    cmd = [
        "docker", "run", "--rm",
        "--entrypoint", "bash",
        "-v", f"{os.getcwd()}:/out",
        "public.ecr.aws/lambda/python:3.12",
        "-c",
        f"microdnf install -y zip > /dev/null 2>&1 && "
        f"pip install Pillow=={PILLOW_VERSION} -t /tmp/python/lib/python3.12/site-packages/ --quiet --root-user-action=ignore && "
        f"cd /tmp && zip -r /out/{OUTPUT_ZIP} python/ -x '*.pyc' -x '*/__pycache__/*' && "
        f"echo 'ZIP generado: {OUTPUT_ZIP}'"
    ]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("ERROR: Docker falló. Prueba el método alternativo.")
        sys.exit(1)
    print(f"\nListo: {OUTPUT_ZIP}")
    print_deploy_instructions()


def print_deploy_instructions():
    print("""
=== PASOS PARA DEPLOYAR EN AWS ===

1. Subir la layer en us-east-2:
   AWS Console → Lambda → Layers → Create layer
   - Name: MiPillowLayer
   - Upload: pillow_layer.zip
   - Compatible runtimes: python3.12
   - Compatible architectures: x86_64
   - Region: us-east-2  ← IMPORTANTE

2. Actualizar la función Lambda:
   AWS Console → Lambda → suplescrapper-image-processor → Configuration → Layers
   - Remover la layer actual (us-east-1)
   - Add layer → Custom layers → MiPillowLayer → version 1

3. Re-triggerizar las imágenes:
   python tools/retrigger_lambda_originals.py
""")


if __name__ == "__main__":
    build_with_docker()
