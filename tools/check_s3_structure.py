import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")

def check_structure():
    s3 = boto3.client(
        's3', 
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
        region_name=AWS_REGION
    )

    print(f"Listando objetos bajo 'assets/img/' en {BUCKET_NAME}...")
    
    # List objects to see "folders" (prefixes)
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='assets/img/', Delimiter='/')

    print("Carpetas encontradas (CommonPrefixes):")
    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            print(f" - {prefix['Prefix']}")
    else:
        print(" - Ninguna carpeta encontrada.")

    print("\nArchivos sueltos en 'assets/img/':")
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'] != 'assets/img/':
                print(f" - {obj['Key']}")

if __name__ == "__main__":
    check_structure()
