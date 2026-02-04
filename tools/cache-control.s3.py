# Ejecutar 1 vez!!!
import boto3
import os
from dotenv import load_dotenv

# Cargar variables desde el .env
load_dotenv()

# Configuración del cliente S3
s3 = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

BUCKET_NAME = 'suplescrapper-images'
CACHE_SETTING = 'max-age=2592000, public' # 30 días

def update_s3_cache():
    paginator = s3.get_paginator('list_objects_v2')
    
    print(f"🚀 Iniciando actualización en el bucket: {BUCKET_NAME}")
    
    # Iterar sobre todos los objetos del bucket
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        if 'Contents' not in page:
            print("El bucket está vacío.")
            return

        for obj in page['Contents']:
            key = obj['Key']
            
            # 1. Obtener metadatos actuales para preservar el ContentType (webp, png, etc.)
            head = s3.head_object(Bucket=BUCKET_NAME, Key=key)
            current_content_type = head.get('ContentType', 'application/octet-stream')
            
            # 2. Realizar el Copy-in-place para inyectar el Cache-Control
            try:
                s3.copy_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                    CacheControl=CACHE_SETTING,
                    ContentType=current_content_type,
                    MetadataDirective='REPLACE' # Obligatorio para cambiar metadatos
                )
                print(f"✅ Actualizado: {key} [{current_content_type}]")
            except Exception as e:
                print(f"❌ Error en {key}: {e}")

if __name__ == "__main__":
    update_s3_cache()
    print("\n✨ Proceso completado.")