"""
reclasificar_db.py
------------------
Script de diagnóstico y reclasificación masiva de la BD.

Por cada producto en la BD, replica el pipeline completo de su scraper de origen
(incluyendo la lógica especial de cada tienda) y compara con la clasificación
actual, actualizando id_subcategoria cuando difiere.

Uso:
    # Diagnóstico completo (sin tocar BD):
    python local_processing_testing/reclasificar_db.py --dry-run

    # Solo una tienda:
    python local_processing_testing/reclasificar_db.py --dry-run --tienda "ChileSuplementos"

    # Solo una categoría:
    python local_processing_testing/reclasificar_db.py --dry-run --categoria "Proteinas"

    # Aplicar cambios en BD local:
    python local_processing_testing/reclasificar_db.py

    # Contra Neon (cambiar DATABASE_URL):
    DATABASE_URL="postgresql://..." python local_processing_testing/reclasificar_db.py
"""

import sys
import os
import re
import argparse
import unicodedata
import psycopg2

# Añadir scrapers_v2 al path para importar CategoryClassifier
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scrapers_v2'))
from CategoryClassifier import CategoryClassifier, normalize

# ─────────────────────────────────────────────────────────────────────────────
# Instancia global del clasificador
# ─────────────────────────────────────────────────────────────────────────────
classifier = CategoryClassifier()

# ─────────────────────────────────────────────────────────────────────────────
# Categorías a SKIP (no clasificar, no tocar)
# ─────────────────────────────────────────────────────────────────────────────
SKIP_CATEGORIES = {
    "Ofertas",
    "OTROS",
    "Packs",
    "Post Entreno",
    "Colageno",
    "Hidratación",
    "Carbohidratos",
    "Superalimento",
}

# ─────────────────────────────────────────────────────────────────────────────
# Fallbacks para subcategorías que produce FitMarketChile pero no existen en BD
# ─────────────────────────────────────────────────────────────────────────────
FITMARKET_FALLBACKS = {
    "quemadores de grasa": "Otros Perdida de Grasa",
    "quemadores":          "Otros Perdida de Grasa",
    "pre-entreno sin estimulantes": "Pre Entreno (fallback)",
    "pre-entreno con estimulantes": "Pre Entreno (fallback)",
    "arginina":            "Otros Pre Entrenos",
    "bcaas":               "Otros Pre Entrenos",
}


# ─────────────────────────────────────────────────────────────────────────────
# Lógicas especiales por tienda
# ─────────────────────────────────────────────────────────────────────────────

def _classify_chile_suplementos(title, description, main_category, det_sub, brand):
    """ChileSuplementos (id=12) — classify + 3 overrides post."""
    final_category, final_subcategory = classifier.classify(
        title, description, main_category, det_sub, brand
    )

    title_lower = title.lower()
    title_norm = normalize(title_lower)

    # Override 1: "+shaker" en el título → reclasificar sin shaker
    if re.search(r'\+shaker', title_lower):
        clean_title = re.sub(r'\+shaker\b', '', title, flags=re.IGNORECASE).strip()
        clean_norm = normalize(clean_title.lower())
        inferred_main = main_category
        if "creatina" in clean_norm or "creatine" in clean_norm or "creapure" in clean_norm:
            inferred_main = "Creatinas"
        elif "proteina" in clean_norm or "protein" in clean_norm or "whey" in clean_norm:
            inferred_main = "Proteinas"
        inferred_cat, inferred_sub = classifier.classify(
            clean_title, description, inferred_main, inferred_main, brand
        )
        if inferred_cat != "Packs":
            final_category, final_subcategory = inferred_cat, inferred_sub

    # Override 2: Bebidas energéticas en cualquier categoría
    elif classifier._any(title_norm, classifier._bebidas["bebidas_energeticas"]):
        final_category, final_subcategory = "Bebidas Nutricionales", "Bebidas Energéticas"

    # Override 3: Gainers en categorías incorrectas
    elif classifier._any(title_norm, classifier._ganadores["ganadores"]):
        final_category, final_subcategory = "Ganadores de Peso", "Ganadores De Peso"

    return final_category, final_subcategory


def _classify_passthrough(title, description, main_category, det_sub, brand):
    """Passthrough puro: AllNutrition, Suples.cl, OneNutrition, SupleTech,
    SuplementosMayoristas."""
    return classifier.classify(title, description, main_category, det_sub, brand)


def _classify_bull_chile(title, description, main_category, det_sub, brand):
    """SuplementosBullChile (id=2) — lógica inline ANTES del clasificador."""
    title_lower = title.lower()

    if "llavero" in title_lower:
        return "OTROS", "Otros"

    elif "rtd" in title_lower or "ready to drink" in title_lower:
        return "Bebidas Nutricionales", "Batidos de proteína"

    elif (("shake" in title_lower and "shaker" not in title_lower) or
          "batido" in title_lower or "bebida" in title_lower or
          "hydration" in title_lower):
        is_powder_weight = ("lb" in title_lower or "kg" in title_lower or
                            "gr" in title_lower or "servicios" in title_lower)
        is_liquid = ("ml" in title_lower or "lt" in title_lower or
                     "cc" in title_lower or "botella" in title_lower)
        if not is_powder_weight or is_liquid:
            return "Bebidas Nutricionales", "Batidos de proteína"
        else:
            return classifier.classify(title, description, main_category, det_sub, brand)

    else:
        return classifier.classify(title, description, main_category, det_sub, brand)


def _classify_cruz_verde(title, description, main_category, det_sub, brand):
    """Cruz Verde (id=13) — inferencia de categoría para 'Energia'."""
    if main_category == "Energia":
        title_lower = title.lower()
        if any(k in title_lower for k in ["gel energizante", "gel energético",
                                           "gel energetico", "energy gel", "power honey"]):
            inferred_cat = "Pre Entrenos"
        elif any(k in title_lower for k in ["isoton", "electrolit", "hidratacion",
                                              "hidratante", "ampollas bebibles"]):
            inferred_cat = "Bebidas Nutricionales"
        elif any(k in title_lower for k in ["creatina", "creatine", "monohidrato",
                                              "creapure", "kre-alkalyn"]):
            inferred_cat = "Creatinas"
        elif any(k in title_lower for k in ["pre workout", "pre-workout", "preworkout",
                                              "pre entreno", "pre-entreno", "beta alanin",
                                              "beta-alanin", "cafeina", "caffeine",
                                              "energy booster"]):
            inferred_cat = "Pre Entrenos"
        elif any(k in title_lower for k in ["carnitina", "carnitine", "l-carnitin",
                                              "quemador", "termogenico", "fat burner",
                                              "ultra ripped", "cla ", "garcinia"]):
            inferred_cat = "Perdida de Grasa"
        elif any(k in title_lower for k in ["bcaa", "aminoacido", "glutamina", "leucina",
                                              "eaa", "hmb", "citrulina", "arginina",
                                              "arginine", "taurina", "lisina"]):
            inferred_cat = "Aminoacidos y BCAA"
        elif any(k in title_lower for k in ["proteina", "whey", "protein", "caseina",
                                              "casein", "albumina", "isolate"]):
            inferred_cat = "Proteinas"
        elif any(k in title_lower for k in ["vitamina", "mineral", "magnesio", "zinc",
                                              "calcio", "hierro", "potasio", "curcuma",
                                              "curcumin", "melatonin", "multivitamin",
                                              "complejo b", "acido folico", "probiotico"]):
            inferred_cat = "Vitaminas y Minerales"
        elif any(k in title_lower for k in ["gainer", "mass gainer", "hipercalorico",
                                              "voluminizador"]):
            inferred_cat = "Ganadores de Peso"
        else:
            return "OTROS", "Otros"
        return classifier.classify(title, description, inferred_cat, inferred_cat, brand)
    else:
        return classifier.classify(title, description, main_category, det_sub, brand)


def _classify_knopp(name, description, main_category, det_sub, brand):
    """Farmacia Knopp (id=16) — SIN brand en classify() + overrides propios."""
    name_lower = normalize(name.lower())
    text = name_lower + " " + normalize((description or "").lower())

    # Override 1: Pack con número inicial y " + "
    starts_with_number = bool(re.match(r'^\d', name_lower.strip()))
    if (" + " in name_lower) and starts_with_number:
        return "Packs", "Packs"

    # Override 2: Snacks en Proteínas
    if main_category == "Proteinas":
        if (re.search(r'\bbar\b', text) or re.search(r'\bbarra\b', text) or
                "bites" in text or "whey bar" in text or "barrita" in text):
            return "Snacks y Comida", "Barritas Y Snacks Proteicas"
        elif "alfajor" in text:
            return "Snacks y Comida", "Snacks Dulces"

    # Override 3: Vitaminas con subcategoría determinística → no reclasificar
    if main_category == "Vitaminas y Minerales":
        if det_sub not in ("CATEGORIZAR_VITAMINAS", "Vitaminas y Minerales"):
            return main_category, det_sub

    # Base: CategoryClassifier SIN brand
    final_category, final_subcategory = classifier.classify(
        name, description, main_category, det_sub
        # sin brand, replicando comportamiento del scraper
    )

    # Post-clasificación: overrides proteínas Knop
    if main_category == "Proteinas" and final_category == "Proteinas":
        if "lean active" in text:
            final_subcategory = "Proteína Vegana"
        elif "yuno" in text:
            final_subcategory = "Proteína de Whey"
        elif ("wpc" in text and final_subcategory not in (
                "Proteína Aislada", "Proteína Hidrolizada", "Proteína Vegana",
                "Proteína de Carne", "Caseína")):
            final_subcategory = "Proteína de Whey"

    return final_category, final_subcategory


def _classify_suplestore(title, description, main_category, det_sub, brand):
    """SupleStore (id=10) — gated classify + post-override."""
    needs_classification = det_sub in (
        "CATEGORIZAR_PROTEINA", "Creatinas", "Vitaminas y Minerales",
        "Aminoácidos", "Quemadores", "Otros Aminoacidos y BCAA"
    )
    if needs_classification:
        final_category, final_sub = classifier.classify(
            title, description, main_category, det_sub, brand
        )
    else:
        final_category, final_sub = main_category, det_sub

    # Post-override especial
    if main_category == "Proteinas" and "cascarafoods proteina lean active" in title.lower():
        final_sub = "Proteína Aislada"

    return final_category, final_sub


def _classify_muscle_factory(title, description, main_category, det_sub, brand):
    """MuscleFactory (id=7) — pre-overrides + classify + post-overrides proteínas."""
    title_lower = normalize(title.lower())
    text = title_lower + " " + normalize((description or "").lower())

    # Pre-override: Snacks en Proteínas
    if main_category == "Proteinas":
        if "shark up" in text or "gel" in text or "isotonic" in text:
            return "Snacks y Comida", "Otros Snacks y Comida"
        elif "bariatrix" in title_lower:
            pass  # Excepción: Bariatrix no es barra
        elif (re.search(r'\bbar\b', text) or re.search(r'\bbarra\b', text) or
              "bites" in text or "whey bar" in text or "barrita" in text):
            return "Snacks y Comida", "Barritas Y Snacks Proteicas"
        elif "alfajor" in text:
            return "Snacks y Comida", "Snacks Dulces"

    # Pre-override: Creatina especial
    if main_category == "Creatinas":
        if "greatlhete" in text and "crea pro" in text:
            return "Creatinas", "Creatina Monohidrato"

    # Base: CategoryClassifier
    final_category, final_subcategory = classifier.classify(
        title, description, main_category, det_sub, brand
    )

    # Post-override proteínas
    if main_category == "Proteinas" and final_category == "Proteinas":
        if brand and "revitta" in brand.lower() and "femme" in text:
            final_subcategory = "Proteína Aislada"
        elif "cooking" in text and "winkler" in text:
            final_subcategory = "Proteína de Whey"

    return final_category, final_subcategory


def _classify_fitmarket(title, description, main_category, det_sub, brand):
    """FitMarketChile (id=8) — lógica completa con cadenas propias para Perdida de
    Grasa y Pre Entrenos, luego classify + post-override proteínas."""
    title_lower = normalize(title.lower())
    text = title_lower + " " + normalize((description or "").lower())

    # Override 1: Pack con " + " (excepto aminoácidos sin número inicial)
    is_amino_category = main_category == "Aminoacidos y BCAA"
    starts_with_number = bool(re.match(r'^\d', title_lower.strip()))
    plus_is_pack = (" + " in title_lower) and (not is_amino_category or starts_with_number)
    if plus_is_pack:
        return "Packs", "Packs"

    # Override 2: Snacks en Proteínas
    if main_category == "Proteinas":
        if "shark up" in text or "gel" in text or "isotonic" in text:
            return "Snacks y Comida", "Otros Snacks y Comida"
        elif "bariatrix" in title_lower:
            pass
        elif (re.search(r'\bbar\b', text) or re.search(r'\bbarra\b', text) or
              "bites" in text or "whey bar" in text or "barrita" in text):
            return "Snacks y Comida", "Barritas Y Snacks Proteicas"
        elif "alfajor" in text:
            return "Snacks y Comida", "Snacks Dulces"

    # Override 3: Perdida de Grasa — cadena propia, SIN clasificador
    if main_category == "Perdida de Grasa":
        cafeina_kw    = ["cafeina", "caffeine", "cafein"]
        crema_kw      = ["crema", "cream", "gel reductor", "gel reduct"]
        retencion_kw  = ["retencion", "retencion", "diuretic", "diuretico",
                         "drenante", "drain"]
        liquido_kw    = ["liquid", "liquido", "liquido", "shot", "ampolla"]
        localizado_kw = ["localizado", "localizada", "abdomin", "belly", "zona"]
        natural_kw    = ["natural", "verde", "green tea", "te verde", "garcinia",
                         "raspberry", "frambuesa", "cla", "linoleic", "linoleico",
                         "l-carnitin", "carnitina", "carnitine"]
        termogenico_kw = ["termogen", "thermogen", "thermo", "termo"]
        quemador_kw    = ["quemador", "fat burn", "fat burner", "burner",
                          "quemagras", "fat loss"]
        if any(k in text for k in cafeina_kw) and not any(k in text for k in quemador_kw + termogenico_kw):
            return "Perdida de Grasa", "Cafeína"
        elif any(k in text for k in crema_kw):
            return "Perdida de Grasa", "Cremas Reductoras"
        elif any(k in text for k in retencion_kw):
            return "Perdida de Grasa", "Eliminadores De Retencion"
        elif any(k in text for k in liquido_kw):
            return "Perdida de Grasa", "Quemadores Liquidos"
        elif any(k in text for k in localizado_kw):
            return "Perdida de Grasa", "Quemadores Localizados"
        elif any(k in text for k in natural_kw):
            return "Perdida de Grasa", "Quemadores Naturales"
        elif any(k in text for k in termogenico_kw):
            return "Perdida de Grasa", "Quemadores Termogenicos"
        elif any(k in text for k in quemador_kw):
            return "Perdida de Grasa", "Quemadores De Grasa"   # fallback manejado luego
        else:
            return "Perdida de Grasa", "Quemadores"             # fallback manejado luego

    # Override 4: Pre Entrenos — cadena propia, SIN clasificador
    if main_category == "Pre Entrenos":
        sin_estim_kw  = ["sin estimulante", "sin cafeina", "caffeine free",
                         "stimulant free", "no stimulant", "pump", "non-stim"]
        guarana_kw    = ["guarana", "guarana"]
        cafeina_kw    = ["cafeina", "caffeine", "cafein"]
        beta_ala_kw   = ["beta ala", "beta-ala", "beta alanina", "beta-alanina"]
        arginina_kw   = ["arginin", "arginina"]
        bcaa_kw       = ["bcaa", "branched", "ramificados"]
        energia_kw    = ["gel", "gel energetico", "gel energetico", "energy gel",
                         "cafe", "cafe", "coffee", "energy drink", "bebida energetica"]
        estimulantes_kw = ["estimulante", "stimulant", "pre-workout", "preworkout",
                           "pre workout", "energia", "energia", "energy"]
        if any(k in text for k in sin_estim_kw):
            return "Pre Entrenos", "Pre-Entreno Sin Estimulantes"   # fallback luego
        elif any(k in text for k in guarana_kw):
            return "Pre Entrenos", "Guarana"
        elif any(k in text for k in energia_kw):
            return "Pre Entrenos", "Energía (Geles/Café)"
        elif any(k in text for k in beta_ala_kw):
            return "Pre Entrenos", "Beta Alanina"
        elif any(k in text for k in arginina_kw):
            return "Pre Entrenos", "Arginina"                       # fallback luego
        elif any(k in text for k in bcaa_kw):
            return "Pre Entrenos", "BCAAs"                          # fallback luego
        elif any(k in text for k in cafeina_kw):
            return "Pre Entrenos", "Cafeína"
        elif any(k in text for k in estimulantes_kw):
            return "Pre Entrenos", "Pre-Entreno con Estimulantes"   # fallback luego
        else:
            return "Pre Entrenos", "Pre Entreno"

    # Base: CategoryClassifier
    final_category, final_subcategory = classifier.classify(
        title, description, main_category, det_sub, brand
    )

    # Post-override proteínas
    if main_category == "Proteinas" and final_category == "Proteinas":
        if brand and "revitta" in brand.lower() and "femme" in text:
            final_subcategory = "Proteína Aislada"
        elif "cooking" in text and "winkler" in text:
            final_subcategory = "Proteína de Whey"

    return final_category, final_subcategory


def _normalize_strongest(text):
    """normalize() standalone de Strongest (idéntico al del scraper)."""
    nfd = unicodedata.normalize('NFD', text)
    return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')


def _classify_strongest(title, description, main_category, det_sub, brand):
    """Strongest (id=4) — lógica standalone SIN CategoryClassifier."""
    title_lower = _normalize_strongest(title.lower())
    desc_norm   = _normalize_strongest((description or "").lower())
    final_category    = main_category
    final_subcategory = det_sub

    # 0. Llaveros
    if "llavero" in title_lower:
        return "OTROS", "Otros"

    # 1. Packs
    elif ("pack" in title_lower or "paquete" in title_lower or
          "combo" in title_lower or " + " in title_lower):
        return "Packs", "Packs"

    # 1.5 RTD / Bebidas
    is_liquid = ("ml" in title_lower or "lt" in title_lower or
                 "cc" in title_lower or "botella" in title_lower)
    is_powder_weight = ("lb" in title_lower or "kg" in title_lower or
                        "gr" in title_lower or "servicios" in title_lower)

    if "rtd" in title_lower or "ready to drink" in title_lower:
        return "Bebidas Nutricionales", "Batidos de proteína"
    elif (("shake" in title_lower and "shaker" not in title_lower) or
          "batido" in title_lower or "bebida" in title_lower or
          "hydration" in title_lower):
        if not is_powder_weight:
            return "Bebidas Nutricionales", "Batidos de proteína"
        elif is_liquid:
            return "Bebidas Nutricionales", "Batidos de proteína"

    # 2. Proteínas
    elif final_category == "Proteinas":
        text_to_search = _normalize_strongest((title + " " + (description or "")).lower())
        enriched_brand_lower = (brand or "").lower()

        if enriched_brand_lower == "dymatize":
            if any(k in text_to_search for k in ["iso", "isolate", "aislada", "isolated",
                                                  "isofit", "isolatada"]):
                return "Proteinas", "Proteína Aislada"
            elif any(k in text_to_search for k in ["hydro", "hidrolizada", "hydrolized",
                                                    "hydrolyzed", "hidrolizado"]):
                return "Proteinas", "Proteína Hidrolizada"
            else:
                return "Proteinas", "Proteína de Whey"
        else:
            if any(k in text_to_search for k in ["vegan", "plant", "vegana", "vegano",
                                                  "plant based"]):
                return "Proteinas", "Proteína Vegana"
            elif any(k in text_to_search for k in ["beef", "carne", "vacuno"]):
                return "Proteinas", "Proteína de Carne"
            elif any(k in text_to_search for k in ["casein", "caseina", "micelar",
                                                    "micellar"]):
                return "Proteinas", "Caseína"
            else:
                # Purity Rule
                purity_check_text = text_to_search
                benign_phrases = [
                    "se mezcla", "facil mezcla", "facil mezcla", "mezclabilidad",
                    "mezcla instantanea", "mezcla instantanea",
                    "combinacion perfecta", "perfecta combinacion",
                    "excelente combinacion", "combinacion perfecta",
                    "perfecta combinacion", "excelente combinacion",
                    "mezclar", "mezclado", "mezclando",
                    "mezcla 1 scoop", "mezcla un scoop",
                    "mezcla 1 porcion", "mezcla una porcion",
                    "mezcla 1 serv", "mezcla un serv",
                    "mezcla el polvo", "mezcla con agua", "mezcla con leche",
                ]
                for phrase in benign_phrases:
                    purity_check_text = purity_check_text.replace(phrase, "")
                if any(k in purity_check_text for k in ["concentrado", "combinacion",
                                                         "combinacion", "concentrate",
                                                         "blend", "mezcla"]):
                    return "Proteinas", "Proteína de Whey"
                elif (re.search(r'\biso\b', text_to_search) or
                      any(k in text_to_search for k in ["isolate", "aislada", "isolated",
                                                         "isofit"])):
                    return "Proteinas", "Proteína Aislada"
                elif any(k in text_to_search for k in ["hydro", "hidrolizada", "hydrolized",
                                                        "hydrolyzed", "hidrolizado"]):
                    return "Proteinas", "Proteína Hidrolizada"
                else:
                    return "Proteinas", "Proteína de Whey"

    # 3. Creatinas
    elif final_category == "Creatinas":
        text_to_search = _normalize_strongest((title + " " + (description or "")).lower())
        if any(k in text_to_search for k in ["monohidrat", "monohydrate", "creapure"]):
            return "Creatinas", "Creatina Monohidrato"
        elif any(k in text_to_search for k in ["hcl", "clorhidrato", "hydrochloride",
                                                "hidrocloruro"]):
            return "Creatinas", "Clorhidrato"
        elif any(k in text_to_search for k in ["malato", "magnesio", "magnapower"]):
            return "Creatinas", "Malato y Magnesio"
        elif any(k in text_to_search for k in ["nitrato", "nitrate"]):
            return "Creatinas", "Nitrato"
        elif any(k in text_to_search for k in ["alkalyn", "alcalina"]):
            return "Creatinas", "Otros Creatinas"
        elif any(k in text_to_search for k in ["micronizad", "micronized"]):
            return "Creatinas", "Micronizada"
        else:
            return "Creatinas", "Otros Creatinas"

    # 4. Vitaminas y Minerales
    elif final_category == "Vitaminas y Minerales":
        text_to_search = _normalize_strongest((title + " " + (description or "")).lower())
        if any(k in text_to_search for k in ["collagen", "colageno"]):
            return "Vitaminas y Minerales", "Colágeno"
        elif any(k in text_to_search for k in ["vitamin c", "vitamina c"]):
            return "Vitaminas y Minerales", "Vitamina C"
        else:
            return "Vitaminas y Minerales", "Bienestar General"

    # 5. Aminoacidos
    elif final_category == "Aminoacidos y BCAA":
        text_to_search = _normalize_strongest((title + " " + (description or "")).lower())
        if "bcaa" in text_to_search:
            return "Aminoacidos y BCAA", "BCAAs"
        else:
            return "Aminoacidos y BCAA", "Otros Aminoácidos y BCAA"

    # 6. Snacks
    elif final_category == "Snacks y Comida":
        text_to_search = _normalize_strongest((title + " " + (description or "")).lower())
        if ("shark up" in text_to_search or "gel" in text_to_search or
                "isotonic" in text_to_search):
            return "Snacks y Comida", "Otros Snacks y Comida"
        elif any(k in text_to_search for k in ["bar", "bites", "whey bar", "barra",
                                                "barrita"]):
            return "Snacks y Comida", "Barritas Y Snacks Proteicas"
        elif "alfajor" in text_to_search:
            return "Snacks y Comida", "Snacks Dulces"
        else:
            return final_category, final_subcategory

    return final_category, final_subcategory


def _classify_dr_simi(name, description, main_category, det_sub, brand):
    """Dr Simi (id=14) — override de marca + lógica por categoría."""
    # Override de marca
    name_lower_raw = name.lower()
    if "veggimilk" in name_lower_raw:
        brand = "Aquasolar"
    elif "suerox" in name_lower_raw:
        brand = "Suerox"

    # Override Vaso Shaker
    if "VASO SHAKER" in name.upper():
        return "OTROS", "Otros"

    title_lower = normalize(name.lower())

    # Vitaminas y Minerales (con subcats determinísticas)
    if main_category == "Vitaminas y Minerales":
        if det_sub == "Colágeno":
            return "Vitaminas y Minerales", "Colágeno"
        if det_sub == "Aceites Y Omegas":
            return "Vitaminas y Minerales", "Omega 3 y Aceites"
        return classifier.classify(name, description, "Vitaminas y Minerales", det_sub, brand)

    if main_category == "Bebidas Nutricionales":
        return classifier.classify(name, description, "Bebidas Nutricionales", det_sub, brand)

    if main_category == "Superalimento":
        return classifier.classify(name, description, "Snacks y Comida",
                                   "Otros Snacks y Comida", brand)

    if main_category in ("Pronutrition", "Deportistas", "OTROS", "Proteinas"):
        # Suerox → isotónico
        if "suerox" in title_lower:
            return "Bebidas Nutricionales", "Bebidas Nutricionales"
        # Snacks
        if (re.search(r'\bbar\b', title_lower) or re.search(r'\bbarra\b', title_lower) or
                "bites" in title_lower or "barrita" in title_lower or
                "cookie" in title_lower or "galleta" in title_lower):
            return "Snacks y Comida", "Barritas Y Snacks Proteicas"
        elif "alfajor" in title_lower or "brownie" in title_lower or "panqueque" in title_lower:
            return "Snacks y Comida", "Snacks Dulces"
        elif "mantequilla" in title_lower and "mani" in title_lower:
            return "Snacks y Comida", "Mantequilla De Mani"
        elif "cereal" in title_lower or "avena" in title_lower or "granola" in title_lower:
            return "Snacks y Comida", "Cereales"
        # Bebidas
        if ("isoton" in title_lower or "electrolit" in title_lower or
                "hidratacion" in title_lower):
            return "Bebidas Nutricionales", "Isotónicos"
        if (re.search(r'\brtd\b', title_lower) or "bebida proteica" in title_lower or
                "batido listo" in title_lower):
            return "Bebidas Nutricionales", "Batidos de proteína"
        if "energia" in title_lower and re.search(r'\bml\b', title_lower):
            return "Bebidas Nutricionales", "Bebidas Energéticas"
        # Inferencia por keyword en título
        kw_map = [
            (["gainer", "ganador de peso", "mass gainer", "hipercalorico",
              "weight gainer", "voluminizador"], "Ganadores de Peso"),
            (["creatina", "creatine", "monohidrato", "creapure", "kre-alkalyn",
              "creatine hcl"], "Creatinas"),
            (["pre-entreno", "pre entreno", "preworkout", "pre workout", "cafeina",
              "caffeine", "stim", "pump", "fuerza explosiva", "beta alanina",
              "energy booster", "nano cafeina", "waxy maize",
              "carbohidrato energetico"], "Pre Entrenos"),
            (["carnitina", "quemador", "termogenico", "fat burner", "cla ",
              "conjugated", "garcinia", "te verde", "diuretico"], "Perdida de Grasa"),
            (["bcaa", "aminoacido", "glutamina", "leucina", "eaa", "hmb",
              "citrulina", "arginina", "taurina", "lisina"], "Aminoacidos y BCAA"),
            (["proteina", "whey", "protein", "caseina", "casein", "albumina",
              "isolate", "hidrolizada", "concentrada", "plant protein",
              "soya protein"], "Proteinas"),
            (["omega", "aceite", "fish oil", "colageno", "collagen"],
             "Vitaminas y Minerales"),
            (["vitamina", "mineral", "magnesio", "zinc", "calcio", "hierro",
              "potasio", "selenio", "biotina", "curcuma", "curcumin", "melatonin",
              "multivitamin", "complejo b", "acido folico", "probiotico",
              "prebiotico"], "Vitaminas y Minerales"),
        ]
        for kws, inferred_cat in kw_map:
            if any(k in title_lower for k in kws):
                final_cat, final_sub = classifier.classify(
                    name, description, inferred_cat, inferred_cat, brand
                )
                # Guard: "Pronutrition" nunca en output
                if final_cat == "Pronutrition" or final_sub == "Pronutrition":
                    return "Proteinas", "Proteína de Whey"
                return final_cat, final_sub
        return "OTROS", "Otros"

    # Fallback
    final_cat, final_sub = classifier.classify(
        name, description, main_category, det_sub, brand
    )
    if final_cat == "Pronutrition" or final_sub == "Pronutrition":
        return "Proteinas", "Proteína de Whey"
    return final_cat, final_sub


def _classify_decathlon(title, description, main_category, det_sub, brand):
    """Decathlon (id=15) — gated classify solo para 3 categorías."""
    NEEDS_CLASSIFICATION = {"Proteinas", "Creatinas", "Bebidas Nutricionales"}
    if main_category not in NEEDS_CLASSIFICATION:
        return main_category, det_sub
    return classifier.classify(title, description, main_category, det_sub, brand)


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher principal
# ─────────────────────────────────────────────────────────────────────────────

SITE_CLASSIFIERS = {
    "ChileSuplementos":      _classify_chile_suplementos,
    "AllNutrition":          _classify_passthrough,
    "Suples.cl":             _classify_passthrough,
    "OneNutrition":          _classify_passthrough,
    "SupleTech":             _classify_passthrough,
    "SuplementosBullChile":  _classify_bull_chile,
    "Cruz Verde":            _classify_cruz_verde,
    "Farmacia Knopp":        _classify_knopp,
    "SupleStore":            _classify_suplestore,
    "MuscleFactory":         _classify_muscle_factory,
    # Wild Foods → SKIP (determinístico, no tocar)
    "FitMarketChile":        _classify_fitmarket,
    "Strongest":             _classify_strongest,
    "Dr Simi":               _classify_dr_simi,
    "SuplementosMayoristas": _classify_passthrough,
    "Decathlon":             _classify_decathlon,
}

SKIP_SITES = {"Wild Foods"}


def classify_for_site(site_name, title, description, main_category,
                      det_sub, brand, url_link):
    """Dispatcher: llama a la función clasificadora de la tienda correspondiente."""
    fn = SITE_CLASSIFIERS.get(site_name)
    if fn is None:
        return main_category, det_sub
    return fn(title, description, main_category, det_sub, brand or "")


# ─────────────────────────────────────────────────────────────────────────────
# Resolución de subcategoría nueva → id en BD
# ─────────────────────────────────────────────────────────────────────────────

def _apply_fitmarket_fallback(cat, sub):
    """Aplica fallbacks para subcategorías que FitMarketChile produce pero no existen en BD."""
    key = sub.lower()
    if key in FITMARKET_FALLBACKS:
        return cat, FITMARKET_FALLBACKS[key]
    return cat, sub


def resolve_sub_id(cat, sub, sub_name_to_id, site_name, warnings):
    """
    Dado (category, subcategory), retorna el id_subcategoria en la BD.
    Aplica fallbacks si es necesario y registra warnings.
    Retorna None si no se puede resolver.
    """
    # Fallbacks específicos de FitMarketChile
    if site_name == "FitMarketChile":
        cat, sub = _apply_fitmarket_fallback(cat, sub)

    key = (normalize(cat.lower().strip()), normalize(sub.lower().strip()))
    sub_id = sub_name_to_id.get(key)

    if sub_id is None:
        warnings.append(f'subcategoría "{sub}" (cat="{cat}") no existe en BD — no se puede resolver')

    return sub_id


# ─────────────────────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────────────────────

def run(dry_run=True, tienda_filter=None, categoria_filter=None, verbose=False):
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://root:root@localhost:5432/suplementos"
    )
    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()

    # ── 1. Cargar mapa de subcategorías ──────────────────────────────────────
    cur.execute("""
        SELECT s.id_subcategoria, s.nombre_subcategoria, c.nombre_categoria
        FROM subcategorias s
        JOIN categorias c ON c.id_categoria = s.id_categoria
    """)
    sub_id_to_info = {}
    sub_name_to_id = {}
    for row in cur.fetchall():
        sid, sub_name, cat_name = row
        sub_id_to_info[sid] = {"sub": sub_name, "cat": cat_name}
        key = (normalize(cat_name.lower().strip()), normalize(sub_name.lower().strip()))
        sub_name_to_id[key] = sid

    # ── 2. Cargar productos con query principal ───────────────────────────────
    query = """
        SELECT
            p.id_producto,
            p.nombre_producto,
            p.descripcion,
            p.id_subcategoria,
            s.nombre_subcategoria   AS subcategoria_actual,
            c.nombre_categoria      AS categoria_actual,
            COALESCE(m.nombre_marca, '') AS nombre_marca,
            t.nombre_tienda,
            pt.url_link
        FROM productos p
        JOIN subcategorias s ON s.id_subcategoria = p.id_subcategoria
        JOIN categorias c    ON c.id_categoria    = s.id_categoria
        LEFT JOIN marcas m   ON m.id_marca        = p.id_marca
        JOIN producto_tienda pt ON pt.id_producto = p.id_producto
        JOIN tiendas t          ON t.id_tienda    = pt.id_tienda
        WHERE pt.id_producto_tienda = (
            SELECT MIN(id_producto_tienda)
            FROM producto_tienda
            WHERE id_producto = p.id_producto
        )
    """
    params = []
    conditions = []
    if tienda_filter:
        conditions.append("t.nombre_tienda = %s")
        params.append(tienda_filter)
    if categoria_filter:
        conditions.append("c.nombre_categoria = %s")
        params.append(categoria_filter)
    if conditions:
        query += " AND " + " AND ".join(conditions)
    query += " ORDER BY t.nombre_tienda, c.nombre_categoria"

    cur.execute(query, params)
    rows = cur.fetchall()

    # ── 3. Procesar cada producto ─────────────────────────────────────────────
    mode_label = "DRY RUN" if dry_run else "APLICANDO CAMBIOS"
    print(f"\n=== RECLASIFICACIÓN DB — {mode_label} ===\n")

    total       = 0
    changed     = 0
    no_change   = 0
    skipped     = 0
    warnings_total = 0

    changes_by_site     = {}
    changes_by_category = {}

    updates_to_apply = []  # (id_producto, new_sub_id)

    for row in rows:
        (id_producto, nombre_producto, descripcion, id_subcategoria_actual,
         subcategoria_actual, categoria_actual, nombre_marca,
         nombre_tienda, url_link) = row

        total += 1

        # ── SKIP: Wild Foods ──────────────────────────────────────────────
        if nombre_tienda in SKIP_SITES:
            skipped += 1
            if verbose:
                print(f"[SKIP]    id={id_producto} | {nombre_tienda} | {categoria_actual}"
                      f"  (determinístico, no tocar)")
            continue

        # ── SKIP: categorías sin rama ─────────────────────────────────────
        if categoria_actual in SKIP_CATEGORIES:
            skipped += 1
            if verbose:
                print(f"[SKIP]    id={id_producto} | {nombre_tienda} | {categoria_actual}"
                      f"  (categoría sin rama)")
            continue

        # ── Clasificar ────────────────────────────────────────────────────
        local_warnings = []
        try:
            new_cat, new_sub = classify_for_site(
                site_name=nombre_tienda,
                title=nombre_producto or "",
                description=descripcion or "",
                main_category=categoria_actual,
                det_sub=subcategoria_actual,
                brand=nombre_marca or "",
                url_link=url_link or "",
            )
        except Exception as e:
            local_warnings.append(f"error en clasificador: {e}")
            skipped += 1
            warnings_total += 1
            print(f"[ERROR]   id={id_producto} | {nombre_tienda} | {categoria_actual}")
            print(f"          nombre: \"{nombre_producto}\"")
            print(f"          {e}")
            continue

        # ── SKIP si la nueva categoría también es de skip ─────────────────
        if new_cat in SKIP_CATEGORIES:
            skipped += 1
            if verbose:
                print(f"[SKIP]    id={id_producto} | {nombre_tienda} | {categoria_actual}"
                      f"  (nueva cat sin rama: {new_cat})")
            continue

        # ── Resolver id de la nueva subcategoría ─────────────────────────
        new_sub_id = resolve_sub_id(new_cat, new_sub, sub_name_to_id,
                                    nombre_tienda, local_warnings)

        for w in local_warnings:
            warnings_total += 1
            print(f"[WARN]    id={id_producto} | {nombre_tienda} | {w}")

        if new_sub_id is None:
            skipped += 1
            continue

        # ── Comparar ──────────────────────────────────────────────────────
        if new_sub_id != id_subcategoria_actual:
            changed += 1
            changes_by_site[nombre_tienda] = changes_by_site.get(nombre_tienda, 0) + 1
            changes_by_category[new_cat]   = changes_by_category.get(new_cat, 0) + 1

            print(f"[CHANGED] id={id_producto} | {nombre_tienda} | {categoria_actual}")
            print(f"          nombre: \"{nombre_producto}\"")
            print(f"          old: {subcategoria_actual}  →  new: {new_sub}")

            updates_to_apply.append((id_producto, new_sub_id))
        else:
            no_change += 1
            if verbose:
                print(f"[NOCHANGE] id={id_producto} | {nombre_tienda} | {categoria_actual}")

    # ── 4. Aplicar cambios (si no es dry-run) ─────────────────────────────────
    if not dry_run and updates_to_apply:
        print(f"\nAplicando {len(updates_to_apply)} cambios en la BD...")
        merged   = 0
        deleted  = 0
        updated  = 0
        for id_prod, new_sub_id in updates_to_apply:
            # Obtener nombre y marca del producto a actualizar
            cur.execute(
                "SELECT nombre_producto, id_marca FROM productos WHERE id_producto = %s",
                (id_prod,)
            )
            row = cur.fetchone()
            if row is None:
                # Ya fue eliminado en una fusión anterior de este mismo lote
                continue
            nombre_prod, id_marca = row

            # Comprobar si ya existe un producto con (nombre, marca, nueva_subcat)
            cur.execute(
                """SELECT id_producto FROM productos
                   WHERE nombre_producto = %s
                     AND id_marca IS NOT DISTINCT FROM %s
                     AND id_subcategoria = %s
                     AND id_producto <> %s""",
                (nombre_prod, id_marca, new_sub_id, id_prod)
            )
            existing = cur.fetchone()

            if existing:
                # ── Fusión: reasignar producto_tienda al producto existente ──
                id_destino = existing[0]

                # producto_tienda tiene uq(id_producto, id_tienda):
                # para cada fila de id_prod, si id_destino ya tiene esa tienda
                # → eliminar la fila de id_prod; si no → reasignar.
                cur.execute(
                    "SELECT id_producto_tienda, id_tienda FROM producto_tienda WHERE id_producto = %s",
                    (id_prod,)
                )
                pt_rows = cur.fetchall()
                for pt_id, pt_tienda in pt_rows:
                    cur.execute(
                        "SELECT id_producto_tienda FROM producto_tienda WHERE id_producto = %s AND id_tienda = %s",
                        (id_destino, pt_tienda)
                    )
                    dest_pt = cur.fetchone()
                    if dest_pt:
                        # Destino ya tiene esa tienda → reasignar historia_precios
                        # al pt_id destino (descartar los que ya existen en esa fecha)
                        dest_pt_id = dest_pt[0]
                        cur.execute(
                            """DELETE FROM historia_precios hp_orig
                               USING historia_precios hp_dest
                               WHERE hp_orig.id_producto_tienda = %s
                                 AND hp_dest.id_producto_tienda = %s
                                 AND hp_orig.fecha_precio = hp_dest.fecha_precio""",
                            (pt_id, dest_pt_id)
                        )
                        cur.execute(
                            "UPDATE historia_precios SET id_producto_tienda = %s WHERE id_producto_tienda = %s",
                            (dest_pt_id, pt_id)
                        )
                        # Ahora sí se puede eliminar la fila de producto_tienda
                        cur.execute(
                            "DELETE FROM producto_tienda WHERE id_producto_tienda = %s",
                            (pt_id,)
                        )
                    else:
                        # Reasignar al destino (historia_precios sigue el pt_id, no cambia)
                        cur.execute(
                            "UPDATE producto_tienda SET id_producto = %s WHERE id_producto_tienda = %s",
                            (id_destino, pt_id)
                        )

                # Reasignar click_analytics si los hay
                cur.execute(
                    "UPDATE click_analytics SET id_producto = %s WHERE id_producto = %s",
                    (id_destino, id_prod)
                )

                # Eliminar el producto duplicado (ya sin referencias)
                cur.execute("DELETE FROM productos WHERE id_producto = %s", (id_prod,))
                merged  += 1
                deleted += 1
                print(f"[MERGE]   id={id_prod} fusionado en id={id_destino}"
                      f" (nombre: \"{nombre_prod}\")")
            else:
                # ── UPDATE normal ─────────────────────────────────────────
                cur.execute(
                    "UPDATE productos SET id_subcategoria = %s WHERE id_producto = %s",
                    (new_sub_id, id_prod)
                )
                updated += 1

        conn.commit()
        print(f"\nCambios aplicados y commiteados.")
        print(f"  Actualizados: {updated}")
        print(f"  Fusionados (eliminados): {merged}")
    elif dry_run and updates_to_apply:
        print(f"\n(dry-run) {len(updates_to_apply)} cambios NO se aplicaron a la BD.")

    cur.close()
    conn.close()

    # ── 5. Reporte final ──────────────────────────────────────────────────────
    print(f"\n=== REPORTE FINAL ===")
    print(f"Procesados: {total}")
    print(f"Cambiados:  {changed}")
    print(f"Sin cambio: {no_change}")
    print(f"Skipped:    {skipped}")
    print(f"Warnings:   {warnings_total}")

    if changes_by_site:
        print("\nCambios por tienda:")
        for site, cnt in sorted(changes_by_site.items(), key=lambda x: -x[1]):
            print(f"  {site:<30} {cnt}")

    if changes_by_category:
        print("\nCambios por categoría:")
        for cat, cnt in sorted(changes_by_category.items(), key=lambda x: -x[1]):
            print(f"  {cat:<35} {cnt}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Diagnóstico y reclasificación masiva de la BD de suplementos."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Solo diagnóstico, no modifica la BD."
    )
    parser.add_argument(
        "--tienda", type=str, default=None,
        help="Filtrar por nombre de tienda (ej: 'ChileSuplementos')."
    )
    parser.add_argument(
        "--categoria", type=str, default=None,
        help="Filtrar por nombre de categoría (ej: 'Proteinas')."
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Mostrar también los productos sin cambio."
    )
    args = parser.parse_args()
    run(
        dry_run=args.dry_run,
        tienda_filter=args.tienda,
        categoria_filter=args.categoria,
        verbose=args.verbose,
    )
