import csv
import os
import hashlib
import re
import requests
import shutil
import io
import logging
import sys
import os
import json
import ollama
from datetime import datetime
from playwright.sync_api import sync_playwright, Page, Playwright
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from rich import print

# Ensure project root is in sys.path to allow importing data_processing
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from data_processing.brand_matcher import BrandMatcher

load_dotenv()

class BaseScraper:
    # Constructor que recibe la URL base, el modo headless, urls de categorías y selectores
    def __init__(self, base_url: str, headless: bool = False, category_urls: list = None, selectors: dict = None, site_name: str = "N/D"):
        self.base_url = base_url # URL base del sitio web
        self.headless = headless # Modo headless
        self.category_urls = category_urls if category_urls else [] # Categorias de productos y su URL
        self.selectors = selectors if selectors else {} # Selectores para extraer datos en forma de diccionario
        self.site_name = site_name # Nombre del sitio web
        
        # Inicializar BrandMatcher
        self.brand_matcher = BrandMatcher()

        # Configuración S3
        self.bucket_name = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")
        self.aws_region = os.getenv("AWS_REGION", "us-east-2")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.s3_client = None
        
        # Caché de imágenes en memoria: { 'subcarpeta': {'hash1.jpg', 'hash2.png'} }
        self.s3_cache = {}

        # Configuración de Logging
        self.logger = None
        self._setup_logging()

        if self.aws_access_key and self.aws_secret_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.aws_region
            )

        # Load valid categories for LLM classification
        self.valid_categories = []
        self._load_valid_categories()

    def _load_valid_categories(self):
        try:
            # Assumes running from root
            csv_path = 'diccionario_categorias.csv'
            if not os.path.exists(csv_path):
                 # Fallback for when running inside scrapers/ or elsewhere
                 csv_path = '../diccionario_categorias.csv'
            
            if os.path.exists(csv_path):
                with open(csv_path, mode='r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Find column regardless of BOM or case
                        key = next((k for k in row.keys() if 'nombre_categoria' in k.lower()), None)
                        if key and row[key]:
                            self.valid_categories.append(row[key])
            else:
                 print("[yellow]Warning: diccionario_categorias.csv not found.[/yellow]")
        except Exception as e:
            print(f"[red]Error loading categories: {e}[/red]")
            
        if not self.valid_categories:
             # Fallback default list
             self.valid_categories = ["Proteinas", "Vitaminas", "Creatina", "Aminoacidos"]

    def classify_batch(self, products):
        """
        Clasifica una lista de productos usando un LLM (Ollama).
        """
        if not products:
            return []
            
        items_to_classify = []
        for p in products:
            # Truncate description to avoid massive prompts and timeouts
            desc = p['description']
            if desc and len(desc) > 300:
                desc = desc[:300] + "..."
                
            items_to_classify.append({
                "name": p['product_name'],
                "description": desc
            })
            
        system_prompt = "Eres un asistente de clasificación de datos experto. Tu tarea es asignar la categoría CORRECTA de la lista proporcionada."
        user_prompt = f"""
        INSTRUCCIONES:
        1. Analiza cada producto (Nombre y Descripción).
        2. Asigna UNA categoría de la lista de 'Valid Categories'.
        3. Si es un accesorio (guantes, shaker, etc), usa 'Ofertas' o 'OTROS' si no encaja.
        4. Responde SOLO con un JSON donde la llave es el nombre exacto del producto.

        Valid Categories: {", ".join(self.valid_categories)}
        
        Items:
        {json.dumps(items_to_classify, ensure_ascii=False)}
        """
        
        try:
            # Switch to robust model Qwen 2.5 14B and use Client with timeout
            # Initialize client here or in __init__. initializing here is safer against connection issues if long running.
            client = ollama.Client(timeout=45) 
            
            response = client.chat(model='qwen2.5:14b', messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_prompt},
            ], format='json', options={'temperature': 0})
            
            content = response['message']['content']
            result = json.loads(content)
            
            for p in products:
                p_name = p['product_name']
                
                # Robust key search (Exact -> Case Insensitive -> Fuzzy)
                val = result.get(p_name)
                if not val:
                     for k, v in result.items():
                         if k.lower().strip() == p_name.lower().strip():
                             val = v
                             break
                
                cat = 'OTROS'
                sub = 'OTROS'

                if isinstance(val, str):
                    cat = val
                    sub = val # If string, assume category = subcategory
                elif isinstance(val, dict):
                    cat = val.get('category', 'OTROS')
                    sub = val.get('subcategory', 'OTROS')
                
                p['category'] = cat
                p['subcategory'] = sub
                    
        except Exception as e:
            print(f"[red]Error in batch classification (Timeout or other): {e}[/red]")
            # Fallback to OTROS
            for p in products:
                p['category'] = 'OTROS'
                p['subcategory'] = 'OTROS'
                
        return products

    def enrich_brand(self, brand: str, product_name: str) -> str:
        """
        Intenta mejorar la marca detectada:
        1. Si la marca ya es válida (no es 'N/D' ni vacía), se mantiene.
        2. Si no es válida, usa BrandMatcher para buscarla en el nombre del producto.
        """
        # Check against list of "unknown" indicators, including "ND" which might result from aggressive cleaning
        invalid_brands = ["N/D", "ND", "N.D.", ""]
        if brand and brand.strip().upper() not in invalid_brands:
            return brand
        
        return self.brand_matcher.get_best_match(product_name)

    def clean_text(self, text: str) -> str:
        """
        Elimina emojis y caracteres no deseados del texto.
        Preserva caracteres alfanuméricos, espacios, acentos, puntuación básica (.,-) y barras (/).
        """
        if not text:
            return ""
        # Regex para eliminar emojis y símbolos gráficos, preservando acentos y '/'
        return re.sub(r'[^\w\s\u00C0-\u00FF\u002D\u002E\u002C\u002F]', '', text).strip()

    def _setup_logging(self):
        """Configura el logger específico para el scraper."""
        if self.site_name == "N/D":
            return

        # Crear estructura de carpetas: logs/{site_name}/
        # Reemplazar espacios por guiones bajos para el nombre de la carpeta
        site_folder = self.site_name.replace(" ", "_")
        log_dir = os.path.join("logs", site_folder)
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"execution_{datetime.now().strftime('%Y-%m-%d')}.log")

        # Configurar logger
        logger_name = f"Scraper_{self.site_name}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        
        # Evitar duplicar handlers si se instancia varias veces
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _log_info(self, message: str):
        if self.logger:
            self.logger.info(message)

    def _ensure_s3_cache(self, subfolder: str, prefix: str = None):
        """
        Carga la lista de objetos de S3 para una subcarpeta específica en memoria (una sola vez).
        Por defecto apunta a la carpeta 'assets/img/resized/'.
        """
        if not self.s3_client or subfolder in self.s3_cache:
            return

        # Si no se especifica prefix, usamos 'assets/img/resized/{subfolder}'
        target_prefix = prefix if prefix else f"assets/img/resized/{subfolder}"

        print(f"Cargando inventario S3 para: {target_prefix} ...")
        self._log_info(f"Cargando inventario S3 para subcarpeta: {subfolder} (Prefix: {target_prefix})")
        
        self.s3_cache[subfolder] = set()
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=target_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Guardamos solo el nombre del archivo (ej: 'assets/img/resized/strongest/abc.jpg' -> 'abc.jpg')
                        key = obj['Key']
                        filename = os.path.basename(key)
                        self.s3_cache[subfolder].add(filename)
            self._log_info(f"Inventario cargado. Total archivos en {subfolder}: {len(self.s3_cache[subfolder])}")
            
        except Exception as e:
            msg = f"Advertencia: No se pudo cargar caché S3 para {subfolder}: {e}"
            print(f"[yellow]{msg}[/yellow]")
            self._log_info(msg)

    def download_image(self, url: str, subfolder: str = "") -> str:
        """
        Gestiona la imagen con estrategia Fire and Forget:
        1. Calcula Hash.
        2. Verifica si existe en S3 (carpeta 'assets/img/resized/').
        3. Si existe: Retorna URL de 'assets/img/resized/'.
        4. Si no: Descarga (RAM) -> Sube a 'assets/img/originals/' -> Retorna URL de 'assets/img/originals/'.
        """
        if not url or url == "N/D":
            return ""

        # Generar nombre de archivo único
        file_ext = os.path.splitext(url.split('?')[0])[1]
        if not file_ext or len(file_ext) > 5:
            file_ext = ".jpg" # Fallback extension
        
        # Generar hash basado en la URL limpia (sin parametros dinamicos) para evitar duplicados
        clean_url_key = url.split('?')[0]
        filename = hashlib.md5(clean_url_key.encode('utf-8')).hexdigest() + file_ext
        
        # Definir rutas S3 (dentro de assets/img)
        s3_resized_key = f"assets/img/resized/{subfolder}/{filename}"
        s3_original_key = f"assets/img/originals/{subfolder}/{filename}"
        
        s3_resized_url = f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/{s3_resized_key}"
        s3_original_url = f"https://{self.bucket_name}.s3.{self.aws_region}.amazonaws.com/{s3_original_key}"

        # --- Lógica S3 ---
        if self.s3_client:
            # 1. Carga Lazy del inventario (busca en resized por defecto)
            self._ensure_s3_cache(subfolder)
            
            # 2. Verificación en Memoria (Zero Latency)
            if filename in self.s3_cache.get(subfolder, set()):
                # LOG REQUERIDO: Imagen encontrada en resized
                self._log_info(f"Imagen encontrada en resized: {filename} (URL: {url}) - SKIP DOWNLOAD")
                print(f"[green]   [S3 CACHE] Imagen ya existe (resized): {filename}[/green]")
                return s3_resized_url

            # 3. Descarga y Subida (Stream) a ORIGINALS
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(url, headers=headers, stream=True, timeout=15)
                
                if response.status_code == 200:
                    response.raw.decode_content = True
                    
                    # Detectar content-type real si es posible, o fallback
                    content_type = response.headers.get('Content-Type', 'application/octet-stream')
                    
                    # Leer en memoria (Buffer)
                    image_data = io.BytesIO(response.content)
                    
                    # Subir a S3 (Originals)
                    self._log_info(f"Imagen no encontrada en resized, subiendo a originals: {filename}")
                    print(f"[yellow]   [S3 UPLOAD] Subiendo original: {filename}[/yellow]")
                    self.s3_client.upload_fileobj(
                        image_data, 
                        self.bucket_name, 
                        s3_original_key, 
                        ExtraArgs={
                            'ContentType': content_type,
                            'CacheControl': 'max-age=2592000' # 30 días
                        }
                    )
                    
                    # NOTA: Retornamos la URL de RESIZED para que la BD guarde la URL final/canónica.
                    # Esto significa que la imagen dará 404 hasta que el proceso Lambda se ejecute
                    # y genere el archivo en la carpeta 'resized'.
                    self._log_info(f"Retornando URL futura (resized): {s3_resized_url}")
                        
                    return s3_resized_url
                else:
                    msg = f"Error {response.status_code} descargando: {url}"
                    print(f"[yellow]{msg}[/yellow]")
                    self._log_info(msg)
                    return ""

            except Exception as e:
                msg = f"Error procesando imagen {url}: {e}"
                print(f"[red]{msg}[/red]")
                self._log_info(msg)
                return ""
        
        else:
            # --- Fallback Local (si no hay credenciales AWS) ---
            try:
                base_img_dir = "assets/img"
                target_dir = os.path.join(base_img_dir, subfolder)
                os.makedirs(target_dir, exist_ok=True)
                file_path = os.path.join(target_dir, filename)
                
                if os.path.exists(file_path):
                    return f"assets/img/{subfolder}/{filename}" # Return local relative path

                headers = {'User-Agent': 'Mozilla/5.0...'}
                response = requests.get(url, headers=headers, stream=True, timeout=10)
                if response.status_code == 200:
                    response.raw.decode_content = True
                    with open(file_path, 'wb') as f:
                        shutil.copyfileobj(response.raw, f)
                    return f"assets/img/{subfolder}/{filename}"
                return ""
            except Exception as e:
                print(f"Error local: {e}")
                return ""

    # Metodo que inicia el navegador y el proceso de scraping
    def run(self):
        """
        Inicia el navegador y el proceso de scraping.
        """
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context()
            page = context.new_page()
            
            # Crear directorio raw_data si no existe
            output_dir = "raw_data"
            os.makedirs(output_dir, exist_ok=True)
            
            # Nombre del archivo CSV estandarizado dentro de raw_data
            csv_filename = f"productos_{self.site_name.replace(' ', '_').lower()}_{datetime.now().strftime('%Y-%m-%d')}.csv"
            csv_file = os.path.join(output_dir, csv_filename)
            
            csv_headers = ['date', 'site_name', 'category', 'subcategory', 'product_name', 'brand', 'price', 'link', 'rating', 'reviews', 'active_discount', 'thumbnail_image_url', 'image_url', 'sku', 'description']
            
            print(f"Guardando datos en {csv_file}...")
            self._log_info(f"Iniciando scraping para {self.site_name}. Archivo destino: {csv_filename}")
            
            try:
                with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=csv_headers)
                    writer.writeheader()
                
                    print(f"Navegando a: {self.base_url}")
                    # page.goto(self.base_url, wait_until="domcontentloaded") 
                    # El extract_process de AllNutrition navega a URLs propias, pero dejamos esto por si acaso
                    
                    # Consumimos el generador
                    count = 0
                    for product_data in self.extract_process(page):
                        writer.writerow(product_data)
                        f.flush() # Guardado progresivo
                        count += 1
                    
                    self._log_info(f"Finalizado scraping. Total productos extraídos: {count}")
                        
            except Exception as e:
                print(f"Ocurrió un error: {e}")
                self._log_info(f"ERROR CRÍTICO durante scraping: {e}")
            finally:
                browser.close()

    # Metodo abstracto que debe ser implementado por las clases hijas
    def extract_process(self, page: Page):
        """
        Método que implementa la lógica de extracción específica.
        Debe ser sobreescrito por las clases hijas y debe funcionar como un Generador (yield).
        """
        raise NotImplementedError("El método extract_process debe ser implementado por la clase hija.")
