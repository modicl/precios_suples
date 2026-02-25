import pandas as pd
import os
import re
import json
import glob
from datetime import datetime

# ---------------------------------------------------------------------------
# Encoding fix map — corrige diacríticos corruptos que vienen de algunos
# scrapers/CSVs con encoding mal detectado (latin-1 leído como cp1252, etc.)
# ---------------------------------------------------------------------------
ENCODING_FIX = {
    "Prote\xedna":    "Proteína",
    "Prote\xeena":    "Proteína",
    "Case\xedna":     "Caseína",
    "Multivitam\xednicos": "Multivitamínicos",
    "Multivitam\xeenicos": "Multivitamínicos",
    "Probi\xf3ticos": "Probióticos",
    "Col\xe1geno":    "Colágeno",
    "Hidrataci\xf3n": "Hidratación",
    "Energ\xeda":     "Energía",
    "Energ\xeea":     "Energía",
    "Caf\xe9":        "Café",
    "\xd3xido":       "Óxido",
    "Isot\xf3nicos":  "Isotónicos",
    "Isot\xf3nica":   "Isotónica",
    "Bebidas Energ\xe9ticas": "Bebidas Energéticas",
    "Batidos de prote\xedna": "Batidos de proteína",
    "Batidos de prote\xeena": "Batidos de proteína",
    "Probiticos":     "Probióticos",
    "Colgeno":        "Colágeno",
    "Protena":        "Proteína",
    "Casena":         "Caseína",
    "\xc1cido":       "Ácido",
    "Botell\xf3n":    "Botellón",
    "Porta Prote\xedna": "Porta Proteína",
    "Porta Prote\xeena": "Porta Proteína",
    "Bot\xe9ll\xf3n": "Botellón",
}


def fix_encoding(text):
    """Corrige diacríticos corruptos en un string."""
    if not isinstance(text, str):
        return text
    for bad, good in ENCODING_FIX.items():
        text = text.replace(bad, good)
    return text


def fix_encoding_series(series):
    """Aplica fix_encoding a toda una columna pandas."""
    return series.apply(fix_encoding)


# ---------------------------------------------------------------------------
# Brand loading
# ---------------------------------------------------------------------------

def load_brands(json_path):
    """Carga todos los keywords de marcas desde keywords_marcas.json, ordenados de mayor a menor longitud."""
    if not os.path.exists(json_path):
        print(f"[WARNING] No se encontró keywords_marcas.json en {json_path}")
        return []
    try:
        with open(json_path, encoding='utf-8') as f:
            data = json.load(f)
        keywords = []
        for info in data.values():
            for kw in info.get('keywords', []):
                kw = kw.strip()
                if kw:
                    keywords.append(kw)
        # Deduplicar y ordenar de mayor a menor (longest-first evita sub-matches)
        seen = set()
        unique_kw = []
        for kw in sorted(keywords, key=len, reverse=True):
            if kw.lower() not in seen:
                seen.add(kw.lower())
                unique_kw.append(kw)
        return unique_kw
    except Exception as e:
        print(f"[ERROR] Leyendo keywords de marcas: {e}")
    return []


# ---------------------------------------------------------------------------
# Smart title-case: evita el bug de str.title() con apóstrofes/siglas
# "bcaa's" -> "Bcaa's"  (no "Bcaa'S")
# ---------------------------------------------------------------------------

def smart_title(s):
    """
    Title-case que no rompe apóstrofes ni siglas, y maneja letras acentuadas.

    El regex original [A-Za-z] solo matcheaba ASCII, por lo que caracteres
    como 'é', 'ó', 'Á' actuaban como separadores y la letra siguiente se
    capitalizaba → "EdicióN", "CáPsula".  Con \\w (Unicode-aware) + exclusión
    de dígitos/_ se captura la palabra completa incluyendo tildes.

    Unidades de medida (lb, kg, g, ml, oz, mg, mcg, gr) siempre quedan en
    minúsculas aunque vayan pegadas a un número (ej. "5Lb" → "5lb").
    """
    _UNITS = r'(?:mcg|mg|ml|kg|lb|oz|gr|g)'

    # \w en Python con re es Unicode-aware; excluimos dígitos y _ explícitamente
    result = re.sub(
        r"[^\W\d_]+('[^\W\d_]+)?",
        lambda m: m.group(0)[0].upper() + m.group(0)[1:].lower(),
        s,
    )
    # Post-fix: número seguido de unidad (con o sin espacio) → unidad en minúsculas
    # Orden: mcg/mg/ml primero (más largos) antes de g/m para evitar sub-matches
    result = re.sub(
        r'(\d\s?)(' + _UNITS + r')\b',
        lambda m: m.group(1) + m.group(2).lower(),
        result,
        flags=re.IGNORECASE,
    )
    return result


# ---------------------------------------------------------------------------
# WILD-brand aggressive cleaning (single place)
# ---------------------------------------------------------------------------

def _apply_wild_cleaning(name):
    """
    Limpieza agresiva para productos de marca WILD.
    Elimina palabras de formato genéricas que rompen la agrupación.
    """
    name = re.sub(r'\b(Barra|Barrita)s? (de )?Prote[íi]nas?\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(Barra|Barrita)s?\b',   '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bProtein Bars?\b',         '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bOriginal\b',              '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bSabor\b',                 '', name, flags=re.IGNORECASE)
    name = re.sub(r'\bVegan[oa]s?\b',           '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b\d+\s*g(r)?\b',           '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b45\s*gr?\b',              '', name, flags=re.IGNORECASE)
    return name


# ---------------------------------------------------------------------------
# Core cleaning functions
# ---------------------------------------------------------------------------

def basic_clean(name):
    """
    Limpieza básica de puntuación/espacios SIN eliminar la marca.
    Produce el display_name: nombre legible para el usuario final.
    """
    if not isinstance(name, str) or not name:
        return ""
    cleaned = re.sub(r'\s+', ' ', name)
    cleaned = cleaned.strip(" -.,:;|/()%")
    cleaned = cleaned.replace(" - ", " ")
    if not cleaned:
        return ""
    return smart_title(cleaned.strip())


def normalize_matching_terms(name: str) -> str:
    """
    Normaliza términos que tienen múltiples variantes ortográficas para que
    el matching fuzzy en step2 los trate como equivalentes.

    Solo aplica al clean_name (campo de matching), NO al display_name.

    Normalizaciones:
    - ISO100 / ISO100% → ISO 100  (número pegado al texto)
    - hydrolized / hidrolizada / hidrolizado / hidrolizad* → hydrolyzed
    """
    # ISO100 / ISO100% → ISO 100  (solo este caso conocido; no afecta otros alpha+num)
    name = re.sub(r'\bISO\s*100%?\b', 'ISO 100', name, flags=re.IGNORECASE)

    # Variantes de hydrolyzed → forma canónica inglesa
    # Cubre: hydrolized, hydrolyzed, hidrolizada, hidrolizado, hidrolizad*
    name = re.sub(
        r'\b(hydrolized|hidrolizad[ao]?|hidrolizada|hidrolizado)\b',
        'hydrolyzed',
        name,
        flags=re.IGNORECASE,
    )

    return name


def clean_name_logic(name, brands_pattern):
    """Elimina la marca del nombre y aplica limpieza básica."""
    if not isinstance(name, str) or not name:
        return ""

    is_wild = name.lower().startswith("wild")

    # 1. Eliminar marca
    cleaned = brands_pattern.sub('', name)

    # 2. Limpiar puntuación residual
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip(" -.,:;|/()%")
    cleaned = cleaned.replace(" - ", " ")

    # 3. Limpieza agresiva WILD (solo si era producto WILD)
    if is_wild:
        cleaned = _apply_wild_cleaning(cleaned)

    # 4. Eliminar ", wild" al final
    cleaned = re.sub(r',\s*wild\s*$', '', cleaned, flags=re.IGNORECASE)

    # 5. Normalizar variantes ortográficas para matching (hydrolyzed, iso100, etc.)
    cleaned = normalize_matching_terms(cleaned)

    if not cleaned:
        return ""

    return smart_title(cleaned.strip())


def standardize_units(name, is_pack: bool = False):
    """Normaliza unidades de peso/volumen/porciones y elimina texto promocional.

    is_pack: cuando True (categoría "Packs"), omite el strip de Shaker/Vaso/Regalo
             porque esas palabras son parte constitutiva del bundle, no texto promo.
    """
    if not isinstance(name, str):
        return ""

    # Pesos / volúmenes
    name = re.sub(r'\b(\d+(\.\d+)?)\s*(lbs?|libras?)\b',        r'\1lb',   name, flags=re.IGNORECASE)
    name = re.sub(r'\b(\d+(\.\d+)?)\s*(kgs?|kilos?)\b',          r'\1kg',   name, flags=re.IGNORECASE)
    name = re.sub(r'\b(\d+)\s*(mg|mcg|g)\b',                     r'\1\2',   name, flags=re.IGNORECASE)
    name = re.sub(r'\b(\d+)\s*(caps?|capsulas?|softgels?|tabletas?|servicios?|servs?)\b',
                  r'\1\2', name, flags=re.IGNORECASE)

    # Normalizar "100% Whey" -> "Whey"
    name = re.sub(r'\b(100%|100 %)\s*Whey\b', 'Whey', name, flags=re.IGNORECASE)

    # Eliminar sufijos genéricos de marca
    name = re.sub(r'\b(Nutrition|Supplements|Labs?|Pharm|Pharma)\b', '', name, flags=re.IGNORECASE)

    # Colapsar espacios dobles que pueden quedar tras eliminar palabras
    name = re.sub(r'\s{2,}', ' ', name).strip()

    # Eliminar texto promocional: truncar desde el marcador en adelante.
    # Omitir para Packs: en bundles "Shaker", "Vaso", etc. son parte del producto.
    if not is_pack:
        name = re.sub(r'(\+|con|incluye)?\s*\b(Shaker|Vaso|Regalo|Gratis)\b.*', '', name, flags=re.IGNORECASE)

    # Eliminar "(Unidad)" al final
    name = re.sub(r'[\(\s]Unidad[\)]?$', '', name, flags=re.IGNORECASE)

    # Limpieza agresiva WILD (también aplica sobre el nombre ya limpio de marca)
    if name.lower().startswith("wild"):
        name = _apply_wild_cleaning(name)

    # Stopword guard: si después de toda la limpieza solo queda una palabra
    # genérica, señalizar al caller para revertir al original.
    stopwords = {
        "unidad", "unidades", "pack", "caja", "display",
        "promo", "oferta", "barra", "barras", "sachet"
    }
    if name.lower().strip() in stopwords:
        return None  # El caller usa el nombre original

    return name.strip()


# ---------------------------------------------------------------------------
# Main pipeline step
# ---------------------------------------------------------------------------

def process_cleaning():
    print("--- PASO 1: Limpieza Determinista de Nombres ---")

    current_dir  = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)

    # --- Leer todos los CSVs raw ---
    raw_data_dir = os.path.join(project_root, "raw_data")
    raw_files    = glob.glob(os.path.join(raw_data_dir, "*.csv"))

    if not raw_files:
        print(f"Error: No hay archivos CSV en {raw_data_dir}")
        return

    print(f"Encontrados {len(raw_files)} archivos raw en {raw_data_dir}.")
    dfs = []
    for f in raw_files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print(f"Error leyendo {f}: {e}")

    if not dfs:
        return

    df = pd.concat(dfs, ignore_index=True)
    print(f"Cargados {len(df)} registros.")

    # --- Fix de encoding en columnas de texto ---
    # Algunos scrapers/CSVs producen diacríticos corruptos; los corregimos
    # antes de cualquier otro procesamiento.
    for col in ('product_name', 'category', 'subcategory', 'brand'):
        if col in df.columns:
            df[col] = fix_encoding_series(df[col])
    print("Encoding de columnas de texto corregido.")

    # --- Cargar marcas ---
    brands_path = os.path.join(project_root, "scrapers_v2", "diccionarios", "keywords_marcas.json")
    brands      = load_brands(brands_path)
    print(f"Cargados {len(brands)} keywords de marcas desde keywords_marcas.json")

    # Compilar regex de marcas (longest-first ya garantizado por load_brands)
    if brands:
        escaped      = [re.escape(b) for b in brands]
        pattern_str  = r'\b(' + '|'.join(escaped) + r')\b'
        brands_pattern = re.compile(pattern_str, re.IGNORECASE)
    else:
        brands_pattern = re.compile(r'(?!x)x')  # no hace match nunca

    # --- Aplicar limpieza fila a fila ---
    count_changed = 0

    def row_cleaner(row):
        nonlocal count_changed

        original_source = row['product_name']
        if pd.isna(original_source) or not str(original_source).strip():
            return pd.Series({"clean_name": "", "display_name": ""})
        original_source = str(original_source)
        is_pack = str(row.get('category', '')).strip().lower() == 'packs'

        # --- display_name: nombre con marca, solo limpieza básica + unidades ---
        display = basic_clean(original_source)
        display_final = standardize_units(display, is_pack=is_pack)
        if display_final is None:
            display_final = smart_title(original_source)

        # --- clean_name: nombre SIN marca (para normalización/matching) ---
        cleaned = clean_name_logic(original_source, brands_pattern)
        if not cleaned:
            cleaned = smart_title(original_source)

        final_name = standardize_units(cleaned, is_pack=is_pack)
        if final_name is None:
            # Stopword guard: revertir al original limpio
            final_name = smart_title(original_source)

        # Detectar si hubo cambio real (case-insensitive)
        if final_name and final_name.lower() != original_source.lower():
            count_changed += 1

        return pd.Series({"clean_name": final_name, "display_name": display_final})

    df[['clean_name', 'display_name']] = df.apply(row_cleaner, axis=1)
    print(f"Nombres limpiados/modificados: {count_changed}")

    # --- Guardar output ---
    today_str  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join(current_dir, "data", "1_cleaned")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"cleaned_{today_str}.csv")
    latest_path = os.path.join(output_dir, "latest_cleaned.csv")

    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    df.to_csv(latest_path, index=False, encoding='utf-8-sig')

    print(f"[EXITO] Datos limpios guardados en: {latest_path}")


if __name__ == "__main__":
    process_cleaning()
