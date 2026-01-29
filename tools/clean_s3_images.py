import boto3
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")

def clean_s3_folder(s3_client, bucket, prefix):
    """Elimina todos los objetos dentro de un prefijo (carpeta) en S3."""
    print(f"Buscando objetos en '{prefix}'...")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    deleted_count = 0
    
    for page in pages:
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            
            # Borrar en lotes de 1000 (limite de S3 delete_objects)
            if objects_to_delete:
                print(f"Borrando {len(objects_to_delete)} objetos...")
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects_to_delete}
                )
                deleted_count += len(objects_to_delete)
    
    print(f"Total borrados en '{prefix}': {deleted_count}")

def main():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print("Error: Credenciales AWS no encontradas en .env")
        return

    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    print(f"Conectado a S3. Bucket: {BUCKET_NAME}")
    print("Este script borrará TODAS las imágenes en 'assets/img/resized' y 'assets/img/originals'.")
    confirm = input("Escribe 'BORRAR' para confirmar: ")
    
    if confirm != "BORRAR":
        print("Operación cancelada.")
        return

    # Borramos el contenido de las carpetas principales
    # Nota: En S3 las carpetas desaparecen si no tienen objetos, pero el scraper las "creará" de nuevo al subir archivos.
    clean_s3_folder(s3, BUCKET_NAME, "assets/img/resized/")
    clean_s3_folder(s3, BUCKET_NAME, "assets/img/originals/")
    
    print("\nLimpieza completada.")

if __name__ == "__main__":
    main()
