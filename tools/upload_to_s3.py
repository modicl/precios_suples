import os
import boto3
import mimetypes
from botocore.exceptions import ClientError
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Configuración
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")
REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Directorios
LOCAL_ROOT = "assets/img"
# S3_FOLDER_PREFIX = "assets/img" # Not used directly, but logic follows relative path

def get_s3_url(key):
    return f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{key}"

def file_exists_in_s3(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def run_migration():
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("Error: AWS credentials not found in .env")
        return

    # 1. Clientes
    s3 = boto3.client('s3', 
                      aws_access_key_id=AWS_ACCESS_KEY, 
                      aws_secret_access_key=AWS_SECRET_KEY, 
                      region_name=REGION)
    
    # DB Engine
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    DB_PORT = os.getenv("DB_PORT")
    
    if not all([DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT]):
        print("Error: Database credentials not found in .env")
        return

    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = sa.create_engine(DATABASE_URL)

    print(f"Iniciando migración a Bucket: {BUCKET_NAME} (Region: {REGION})")

    # 2. Recorrer archivos locales
    if not os.path.exists(LOCAL_ROOT):
        print(f"Directorio local no encontrado: {LOCAL_ROOT}")
        return

    # Preparar conexión a BD para updates incrementales
    with engine.connect() as conn:
        for root, dirs, files in os.walk(LOCAL_ROOT):
            print(f"Procesando directorio: {root}")
            updates_mapping = {} 
            
            for file in files:
                local_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_path, start=".") 
                s3_key = relative_path.replace("\\", "/")
                
                content_type, _ = mimetypes.guess_type(local_path)
                if not content_type: content_type = 'application/octet-stream'

                # Subir si no existe
                if not file_exists_in_s3(s3, BUCKET_NAME, s3_key):
                    # print(f"Subiendo: {s3_key}")
                    try:
                        s3.upload_file(local_path, BUCKET_NAME, s3_key, ExtraArgs={'ContentType': content_type})
                    except Exception as e:
                        print(f"Error subiendo {s3_key}: {e}")
                        continue
                
                s3_url = get_s3_url(s3_key)
                updates_mapping[s3_key] = s3_url

            # Actualizar BD por directorio
            if updates_mapping:
                try:
                    transaction = conn.begin()
                    count = 0
                    
                    query_img = sa.text("""
                        UPDATE productos 
                        SET url_imagen = :s3_url 
                        WHERE REPLACE(url_imagen, '\\', '/') = :local_key
                           OR url_imagen = :local_key 
                    """)

                    query_thumb = sa.text("""
                        UPDATE productos 
                        SET url_thumb_imagen = :s3_url 
                        WHERE REPLACE(url_thumb_imagen, '\\', '/') = :local_key
                           OR url_thumb_imagen = :local_key
                    """)

                    for local_key, s3_url in updates_mapping.items():
                        res1 = conn.execute(query_img, {"s3_url": s3_url, "local_key": local_key})
                        res2 = conn.execute(query_thumb, {"s3_url": s3_url, "local_key": local_key})
                        count += res1.rowcount + res2.rowcount
                    
                    transaction.commit()
                    if count > 0:
                        print(f"  -> Actualizados {count} registros en BD para {root}")
                except Exception as e:
                    transaction.rollback()
                    print(f"Error en transacción BD para {root}: {e}")

    print("Migración completada.")

if __name__ == "__main__":
    run_migration()
