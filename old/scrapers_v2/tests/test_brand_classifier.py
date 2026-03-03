"""
Tests para BrandClassifier.

Ejecutar desde scrapers_v2/:
    python -m pytest tests/test_brand_classifier.py -v
"""

import sys
import os

# Asegurar que scrapers_v2/ esté en el path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from BrandClassifier import BrandClassifier

bc = BrandClassifier()


# ------------------------------------------------------------------
# normalize_brand: variantes del DOM → nombre canónico
# ------------------------------------------------------------------

def test_normalize_winkler_abbreviated():
    """'WINKLER NUTRI' (DOM abreviado) → 'WINKLER NUTRITION'"""
    assert bc.normalize_brand("WINKLER NUTRI") == "WINKLER NUTRITION"

def test_normalize_winkler_exact():
    """'WINKLER NUTRITION' (ya canónico) → 'WINKLER NUTRITION'"""
    assert bc.normalize_brand("WINKLER NUTRITION") == "WINKLER NUTRITION"

def test_normalize_winkler_lowercase():
    """'winkler nutrition' → 'WINKLER NUTRITION'"""
    assert bc.normalize_brand("winkler nutrition") == "WINKLER NUTRITION"

def test_normalize_optimum_abbreviated():
    """'Optimum Nutr' → 'Optimum Nutrition'"""
    assert bc.normalize_brand("Optimum Nutr") == "Optimum Nutrition"

def test_normalize_biotechusa_variant():
    """'BIOTECH USA' → 'BioTechUSA'"""
    assert bc.normalize_brand("BIOTECH USA") == "BioTechUSA"

def test_normalize_fitsupps_with_space():
    """'Fit Supps' (con espacio) → 'FitSupps'"""
    assert bc.normalize_brand("Fit Supps") == "FitSupps"

def test_normalize_newscience_nospace():
    """'NEWSCIENCE' (sin espacio) → 'New Science'"""
    assert bc.normalize_brand("NEWSCIENCE") == "New Science"

def test_normalize_hbi_innovate_variant():
    """'HBI innovate' → 'HB INNOVATIVE'"""
    assert bc.normalize_brand("HBI innovate") == "HB INNOVATIVE"

def test_normalize_unknown_passthrough():
    """Una marca desconocida se devuelve tal cual."""
    result = bc.normalize_brand("MarcaInventada XYZ")
    assert result == "MarcaInventada XYZ"

def test_normalize_empty_returns_empty():
    assert bc.normalize_brand("") == ""

def test_normalize_nd_returns_empty():
    assert bc.normalize_brand("N/D") == "N/D"


# ------------------------------------------------------------------
# extract_from_title: escaneo de título → nombre canónico
# ------------------------------------------------------------------

def test_extract_dymatize_iso100():
    """ISO 100 es keyword de Dymatize."""
    assert bc.extract_from_title("ISO 100 Dymatize Proteina 5lb") == "Dymatize"

def test_extract_bsn():
    assert bc.extract_from_title("Proteina Whey KOS BSN 2kg") == "BSN"

def test_extract_universal_nutrition():
    assert bc.extract_from_title("Animal Pak Universal Nutrition vitaminas") == "UNIVERSAL NUTRITION"

def test_extract_ronnie_coleman_king_whey():
    """King Whey es keyword de Ronnie Coleman."""
    assert bc.extract_from_title("King Whey 2lb proteina") == "RONNIE COLEMAN"

def test_extract_optimum_100_whey_gold():
    assert bc.extract_from_title("100% Whey Gold Standard 5lb Optimum Nutrition") == "Optimum Nutrition"

def test_extract_no_match_returns_nd():
    assert bc.extract_from_title("Suplemento generico sin marca conocida") == "N/D"

def test_extract_empty_returns_nd():
    assert bc.extract_from_title("") == "N/D"


# ------------------------------------------------------------------
# classify: flujo completo (equivalente a enrich_brand en BaseScraper)
# ------------------------------------------------------------------

def test_classify_valid_brand_normalized():
    """Marca válida del DOM: normaliza sin tocar el título."""
    assert bc.classify("WINKLER NUTRI", "Proteina Winkler 1kg") == "WINKLER NUTRITION"

def test_classify_valid_brand_no_title_scan():
    """scan_title=False (default): marca válida pasa normalizada, título ignorado."""
    result = bc.classify("Optimum", "ISO 100 Dymatize producto")
    assert result == "Optimum Nutrition"

def test_classify_nd_with_scan_title():
    """N/D + scan_title=True → extrae desde título."""
    assert bc.classify("N/D", "ISO 100 Dymatize Proteina", scan_title=True) == "Dymatize"

def test_classify_nd_no_scan_title():
    """N/D + scan_title=False (default) → no escanea título, retorna N/D."""
    assert bc.classify("N/D", "ISO 100 Dymatize Proteina") == "N/D"

def test_classify_empty_brand_no_scan():
    assert bc.classify("", "BSN Syntha 6 proteina") == "N/D"

def test_classify_empty_brand_with_scan():
    assert bc.classify("", "BSN Syntha 6 proteina", scan_title=True) == "BSN"
