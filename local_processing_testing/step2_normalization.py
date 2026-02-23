import pandas as pd
import os
from rapidfuzz import process, fuzz
import re
from datetime import datetime

# Import normalization helpers
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing.normalize_products import (
    extract_sizes,
    detect_packaging,
    extract_pack_quantity,
    extract_flavors,
    check_critical_mismatch,
    check_percentage_mismatch
)

def cleaner(text):
    if not isinstance(text, str): return ""
    return text.lower().strip()


# ---------------------------------------------------------------------------
# Hybrid scoring helpers
# ---------------------------------------------------------------------------

# Words that are semantically redundant and don't distinguish products.
# Stripped before scoring so "Whey" == "Whey Protein", "Elite Casein" == "Elite Casein Protein".
_GENERIC_WORDS = frozenset({
    "protein", "proteina", "proteinas", "proteins",
    "powder", "blend", "formula",
    "gainer", "mass gainer",
    "original",
    "isolate",   # descriptor de tipo de proteína, no distingue variantes
})

# Words that make two products meaningfully different when present in one
# name but not the other.  Any asymmetry → skip the pair entirely.
_HARD_BLOCKERS = frozenset({
    "remate", "oferta", "liquidacion",          # promotional / outlet
    "efervescente", "efervescentes",             # pharmaceutical form
    "medical",                                   # clinical variant
    "sweets",                                    # flavor line brand
    "limitada", "especial", "edition", "edicion",# limited editions
    "dunkin",                                    # co-brand
    "silver",                                    # product sub-line
    "tech",                                      # part of brand/product names (Nitro Tech, Cell Tech)
    "vegan", "vegana",                           # dietary variant
    "hidrolizada", "hydrolyzed",                 # processing variant
    "zero", "light",                             # caloric variant
    "lab",                                       # product sub-line (e.g. Costadoro Lab)
    "crepure", "creapure",                   # creatine quality grade (Creapure vs regular)
    "workout",                                   # distinguishes standalone vs combo packs
})


def _strip_generic(s: str) -> str:
    """Remove generic filler words before scoring."""
    words = s.split()
    return " ".join(w for w in words if w not in _GENERIC_WORDS)


def _has_blocker_asymmetry(a: str, b: str) -> bool:
    """Return True if a hard-blocker word is present in one name but not the other."""
    wa = set(a.split())
    wb = set(b.split())
    for bk in _HARD_BLOCKERS:
        if (bk in wa) != (bk in wb):
            return True
    return False


def _extra_words_are_all_generic(shorter: str, longer: str) -> bool:
    """
    Return True iff every word in `longer` that is NOT in `shorter` is either:
      - a generic filler word (in _GENERIC_WORDS), or
      - a pure number / numeric token (e.g. "100", "2", "5").

    Used to guard against token_set_ratio=100 collapsing variants like
    "Gams Protein Bar" + "Gams Protein Bar Cereal" (extra word "cereal" is
    NOT generic → they are different products).
    """
    ws = set(shorter.split())
    wl = longer.split()
    for w in wl:
        if w not in ws and w not in _GENERIC_WORDS and not w.isdigit():
            return False
    return True


def hybrid_score(a: str, b: str) -> float:
    """
    Compute a fuzzy similarity score that handles subset/superset names.

    Uses the maximum of token_sort_ratio and token_set_ratio on versions
    of the names where generic filler words have been stripped.
    This makes "Elite Casein 4lb" and "Elite Casein Protein 4lb" score 100
    while preserving the normal token_sort behaviour for truly different names.

    IMPORTANT: when token_set_ratio would return 100 (one is a word-subset of
    the other), we only honour it if the extra words are all generic/numeric.
    Otherwise the names are different variants and we fall back to
    token_sort_ratio only, which typically scores much lower.
    """
    sa = _strip_generic(a)
    sb = _strip_generic(b)
    tsr  = fuzz.token_sort_ratio(sa, sb)
    tset = fuzz.token_set_ratio(sa, sb)

    if tset > tsr:
        # token_set is boosting because one name is a subset of the other.
        # Only accept that boost if the extra words are all generic/numeric.
        shorter, longer = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
        if not _extra_words_are_all_generic(shorter, longer):
            return tsr   # ignore the token_set boost

    return max(tsr, tset)


def normalize_step(input_file, output_file):
    print(f"Cargando datos de: {input_file}")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error leyendo {input_file}: {e}")
        return

    # Defensive: clean_name column must exist (produced by step1)
    if 'clean_name' not in df.columns:
        print("Error: columna 'clean_name' no encontrada. Asegúrate de haber ejecutado step1.")
        return

    # display_name is produced by step1 >= v2; fall back to product_name if absent
    if 'display_name' not in df.columns:
        df['display_name'] = df['product_name']

    # Ensure category/subcategory columns exist
    for col in ('category', 'subcategory'):
        if col not in df.columns:
            df[col] = ''

    # Get unique (product_name, clean_name, display_name, brand, category, subcategory) tuples.
    # category and subcategory are hard blocks: products with different values
    # will never be grouped together.
    unique_products = df[['product_name', 'clean_name', 'display_name', 'brand', 'category', 'subcategory']].drop_duplicates().copy()

    # Fill missing values
    unique_products['clean_name']   = unique_products['clean_name'].fillna(unique_products['product_name'])
    unique_products['display_name'] = unique_products['display_name'].fillna(unique_products['product_name'])
    unique_products['category']     = unique_products['category'].fillna('').astype(str)
    unique_products['subcategory']  = unique_products['subcategory'].fillna('').astype(str)

    mapping = {}           # product_name -> rep_name (normalized_name)
    normalized_groups = [] # list of group dicts

    threshold = 87
    count = 0
    total = len(unique_products)

    print(f"Iniciando clustering ({total} productos únicos, umbral={threshold})...")

    for _, row in unique_products.iterrows():
        orig_name   = row['product_name']
        clean       = row['clean_name']
        display     = row['display_name']
        brand       = str(row.get('brand', ''))
        category    = str(row.get('category', ''))
        subcategory = str(row.get('subcategory', ''))

        count += 1
        if count % 100 == 0:
            print(f"  Procesando {count}/{total}...")

        target_clean = cleaner(clean)

        # Feature extraction from clean name
        sizes_candidate   = extract_sizes(clean)
        pack_candidate    = detect_packaging(clean)
        Nx_candidate      = extract_pack_quantity(clean)
        flavors_candidate = extract_flavors(clean)

        # Candidate pool filtered by category/subcategory bucket
        # We keep both the clean_key (for pre-filtering) and the group index.
        bucket_indices = [
            i for i, g in enumerate(normalized_groups)
            if g['category'] == category
            and (
                # If both sides have a defined subcategory, they must match.
                # If either side is blank, subcategory is not used as a block.
                not subcategory
                or not g['subcategory']
                or g['subcategory'] == subcategory
            )
        ]

        # Pre-filter: use rapidfuzz process.extract with token_sort for speed,
        # with a relaxed cutoff (75) to ensure candidates for hybrid scoring
        # are not dropped prematurely.
        candidates_pool = [normalized_groups[i]['clean_key'] for i in bucket_indices]

        pre_matches = process.extract(
            target_clean,
            candidates_pool,
            scorer=fuzz.token_sort_ratio,
            limit=10,
            score_cutoff=75,
        )

        best_match_rep = None

        for match_tuple in pre_matches:
            pool_idx      = match_tuple[2]
            group_idx     = bucket_indices[pool_idx]
            candidate_group = normalized_groups[group_idx]
            candidate_clean = candidate_group['clean_name']
            candidate_key   = candidate_group['clean_key']

            # Hard-blocker asymmetry check: words that make products meaningfully
            # different must be present in both or neither.
            if _has_blocker_asymmetry(target_clean, candidate_key):
                continue

            # Hybrid score: strip generic filler words, then take max of
            # token_sort and token_set. This catches subset names like
            # "Elite Casein 4lb" / "Elite Casein Protein 4lb".
            score = hybrid_score(target_clean, candidate_key)
            if score < threshold:
                continue

            # Structural guards (use clean names)
            if check_critical_mismatch(clean, candidate_clean): continue
            if check_percentage_mismatch(clean, candidate_clean): continue
            if extract_pack_quantity(candidate_clean) != Nx_candidate: continue
            if extract_sizes(candidate_clean) != sizes_candidate: continue
            if detect_packaging(candidate_clean) != pack_candidate: continue
            if extract_flavors(candidate_clean) != flavors_candidate: continue

            # Brand check
            b1 = brand.lower()
            b2 = candidate_group['brand'].lower()
            invalid_brands = {"n/d", "nan", "none", ""}
            if b1 not in invalid_brands and b2 not in invalid_brands:
                if fuzz.ratio(b1, b2) < 85: continue

            # Update rep_name if this display_name is shorter (prefer most concise
            # display name as the canonical name shown to users).
            if len(display) < len(candidate_group['rep_name']):
                candidate_group['rep_name']   = display
                candidate_group['clean_name'] = clean
                candidate_group['clean_key']  = target_clean

            best_match_rep = candidate_group['rep_name']
            break

        if best_match_rep:
            mapping[orig_name] = best_match_rep
        else:
            # New group — display_name is the representative shown to users
            mapping[orig_name] = display
            normalized_groups.append({
                'clean_key':   target_clean,
                'clean_name':  clean,
                'rep_name':    display,   # display_name used for output
                'brand':       brand,
                'category':    category,
                'subcategory': subcategory,
            })

    print(f"Clustering terminado. {total} productos únicos -> {len(normalized_groups)} grupos.")

    df['normalized_name'] = df['product_name'].map(mapping)

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Archivo normalizado guardado en: {output_file}")


def main():
    print("--- PASO 2: Normalización de Productos ---")

    current_dir = os.path.dirname(os.path.abspath(__file__))

    input_csv = os.path.join(current_dir, "data", "1_cleaned", "latest_cleaned.csv")

    today_str  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join(current_dir, "data", "2_normalized")
    os.makedirs(output_dir, exist_ok=True)
    output_csv = os.path.join(output_dir, f"normalized_{today_str}.csv")

    latest_norm_path = os.path.join(output_dir, "latest_normalized.csv")

    if not os.path.exists(input_csv):
        print(f"Error: No se encontró {input_csv}. Ejecuta el Paso 1 primero.")
        return

    normalize_step(input_csv, output_csv)

    # Copy to latest pointer
    try:
        df = pd.read_csv(output_csv)
        df.to_csv(latest_norm_path, index=False, encoding='utf-8-sig')
        print(f"Puntero 'latest' guardado en: {latest_norm_path}")
    except Exception as e:
        print(f"[WARNING] No se pudo copiar al latest: {e}")


if __name__ == "__main__":
    main()
