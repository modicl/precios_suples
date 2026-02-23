# Lambda para redimensionar imagenes trigger por S3

import boto3
import os
import uuid
import urllib.parse
from PIL import Image

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        print(f"Procesando: {key}")

        if not key.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
            print("No es una imagen soportada.")
            continue

        filename = os.path.basename(key)
        name_root, ext = os.path.splitext(filename)
        ext = ext.lower()

        download_path   = f"/tmp/{uuid.uuid4()}-{filename}"
        upload_path_600 = f"/tmp/resized-{name_root}{ext}"
        upload_path_200 = f"/tmp/mini-{name_root}{ext}"

        s3_client.download_file(bucket, key, download_path)

        # --- PROCESAMIENTO ---
        with Image.open(download_path) as image:
            # Forzar carga completa antes de cualquier operacion (evita lazy-load bugs)
            image.load()

            # Conversion robusta a RGB — SIEMPRE, antes de cualquier resize.
            # Crítico para imágenes RGBA (ej. WebP con fondo transparente de ChileSuplementos):
            # sin esto, las zonas transparentes se muestran como glitches oscuros en el frontend.
            if image.mode in ("RGBA", "P"):
                # Modo paleta: puede tener transparencia embebida, pasar por RGBA primero
                if image.mode == "P":
                    image = image.convert("RGBA")
                # Compositar sobre fondo blanco usando alpha_composite.
                # Evita el LANCZOS ringing/halo que produce paste(mask=alpha)
                # en bordes abruptos (transparente→color) de WebP RGBA.
                background = Image.new("RGBA", image.size, (245, 245, 245, 255))
                image = Image.alpha_composite(background, image).convert("RGB")
            elif image.mode != "RGB":
                image = image.convert("RGB")

            # --- VERSION 600px (ancho máximo, proporcional) ---
            img_600 = image.copy()
            if img_600.width > 600:
                w_percent = 600 / img_600.width
                h_size = int(img_600.height * w_percent)
                img_600 = img_600.resize((600, h_size), Image.Resampling.LANCZOS)

            # --- VERSION 200px (thumbnail bounding-box) ---
            img_200 = image.copy()
            img_200.thumbnail((200, 200), Image.Resampling.LANCZOS)

            # Guardar preservando el formato original (WebP→WebP, PNG→PNG, etc.)
            # para que el key de S3 coincida con la URL que guarda BaseScraper en la BD.
            save_kwargs = {"quality": 85}
            if ext in (".jpg", ".jpeg"):
                save_kwargs["optimize"] = True
            img_600.save(upload_path_600, **save_kwargs)
            img_200.save(upload_path_200, **save_kwargs)

        # --- SUBIDA A S3 ---
        # Preservar extensión original en el key de destino para que coincida
        # con la URL pre-calculada que BaseScraper guarda en la BD.
        base_path   = key.replace("assets/img/originals/", "assets/img/resized/", 1)
        folder_path = os.path.dirname(base_path)

        key_600 = f"{folder_path}/{name_root}{ext}"
        key_200 = f"{folder_path}/{name_root}-mini{ext}"

        content_type_map = {
            ".jpg":  "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png":  "image/png",
            ".webp": "image/webp",
        }
        content_type = content_type_map.get(ext, "image/jpeg")

        s3_client.upload_file(
            upload_path_600, bucket, key_600,
            ExtraArgs={"ContentType": content_type}
        )
        s3_client.upload_file(
            upload_path_200, bucket, key_200,
            ExtraArgs={"ContentType": content_type}
        )

        print(f"Subido: {key_600} y {key_200}")

    return {'statusCode': 200, 'body': 'Imagenes procesadas'}
