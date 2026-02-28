# Precios Suples

## Descripcion General

Precios Suples es una plataforma automatizada de recoleccion, limpieza, consolidacion y analisis de precios de suplementos deportivos en diversas tiendas. El proyecto consta de herramientas de web scraping para la extraccion de datos y un pipeline de procesamiento robusto en Python que se encarga de estructurar la informacion dentro de una base de datos PostgreSQL.

## Estructura de Directorios Principal

- `scrapers_v2/`: Contiene los scripts encargados del scraping de informacion y precios en las paginas web de las tiendas de suplementos.
- `local_processing_testing/`: Almacena el pipeline de procesamiento de los datos extraidos, dividido en pasos discretos (limpieza, normalizacion, etc.).
- `bd/`: Contiene el esquema de base de datos (`schema.prisma`) utilizado para gestionar PostgreSQL mediante Prisma ORM.
- `raw_data/`: Directorio temporal o principal donde los scrapers vuelcan la informacion recolectada en crudo.
- `logs/`: Registros de error y ejecucion generados por los scrapers y el pipeline.
- `run_scrapers.bat`: Archivo batch de Windows que orquesta la ejecucion de los scrapers.
- `run_pipeline.bat`: Archivo batch de Windows que orquesta la ejecucion del pipeline de procesamiento de datos de principio a fin.

## Orquestacion del Pipeline

El flujo de limpieza y estructuracion de los datos esta dividido en 6 etapas especificas, gestionadas a traves de `run_pipeline.bat`:

1. **Paso 1: Limpieza de nombres** (`step1_clean_names.py`) - Estandariza los nombres crudos obtenidos durante el scraping.
2. **Paso 2: Normalizacion / clustering** (`step2_normalization.py`) - Compara entidades y agrupa o unifica productos iguales usando clustering y tecnicas de similitud de texto.
3. **Paso 3: Insercion en base de datos** (`step3_db_insertion.py`) - Carga la data ya estructurada a las tablas transaccionales en PostgreSQL.
4. **Paso 4: Deduplicacion** (`step4_deduplication.py`) - Elimina duplicados que pudieran existir en los datos consolidados.
5. **Paso 5: Refresh de vistas materializadas** (`step5_refresh_views.py`) - Ejecuta funciones y comandos para actualizar las vistas que abastecen al frontend de manera veloz.
6. **Paso 6: Generacion de descripciones LLM** (`step6_generate_descriptions.py`) - Utliza Inteligencia Artificial y Modelos de Lenguaje para generar o mejorar la descripcion comercial de los productos registrados.

## Modelado de Datos

El proyecto utiliza PostgreSQL y la interaccion se maneja mediante Prisma. Entidades destacadas:

- `productos`: Catalogo unificado de suplementos deportivos.
- `tiendas`: Establecimientos y paginas de los distribuidores monitorizados.
- `producto_tienda`: Entidad que conecta un producto en particular con una tienda particular.
- `historia_precios`: Almacenamiento longitudinal para permitir el analisis historico del valor del producto.
- `categorias` y `subcategorias`: Taxonomia de productos.

## Configuracion y Preparacion del Entorno

1. Instalar dependencias de Python: `pip install -r requirements.txt` (es recomendable utilizar el entorno virtual ubicado en `venv/`).
2. Configurar variables de entorno: Asegurar la correcta creacion y poblamiento del archivo `.env` (credenciales de base de datos e integraciones externas como LLMs).
3. Preparar prisma: Ejecutar `npx prisma generate` dentro del proyecto, o bien donde radique el archivo `package.json` correspondiente. Esto permitira al ORM interactuar con la base de datos definida en el schema.

## Ejecucion (Entorno Windows)

El flujo completo puede activarse corriendo los archivos en formato Batch:

- **Ejecutar web scrapers:** `run_scrapers.bat` (extrae informacion nueva a la carpeta correspondiente).
- **Consolidar los datos:** `run_pipeline.bat` (toma archivos crudos, procesa y almacena en base de datos de manera definitiva).
