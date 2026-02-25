"""
test_fuzzy_clustering.py
========================
Tests the clustering logic in normalize_products.py using known pairs.

Each test case has:
  - name1, name2: product name pair
  - expect_merge: True if they should cluster, False if they should be kept separate
  - description: why this case exists

Run with:
    python tools/test_fuzzy_clustering.py
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rapidfuzz import fuzz
from data_processing_v2.normalize_products import (
    extract_sizes,
    extract_flavors,
    extract_pack_quantity,
    detect_packaging,
    check_critical_mismatch,
    check_percentage_mismatch,
)

THRESHOLD_SET  = 83
THRESHOLD_SORT = 65

def should_merge(name1: str, name2: str, brand1: str = "N/D", brand2: str = "N/D") -> tuple[bool, str]:
    """Replicates the clustering decision from normalize_products.py."""
    def clean(t): return t.lower().strip()

    tset = fuzz.token_set_ratio(clean(name1), clean(name2))
    tsrt = fuzz.token_sort_ratio(clean(name1), clean(name2))

    if tset < THRESHOLD_SET:
        return False, f"tset={tset:.1f} below {THRESHOLD_SET}"
    if tsrt < THRESHOLD_SORT:
        return False, f"tsrt={tsrt:.1f} below {THRESHOLD_SORT}"
    if check_critical_mismatch(name1, name2):
        return False, "critical_mismatch"
    if check_percentage_mismatch(name1, name2):
        return False, "pct_mismatch"
    if extract_pack_quantity(name1) != extract_pack_quantity(name2):
        return False, "pack_qty_mismatch"
    if extract_sizes(name1) != extract_sizes(name2):
        return False, "size_mismatch"
    if detect_packaging(name1) != detect_packaging(name2):
        return False, "packaging_mismatch"
    if extract_flavors(name1) != extract_flavors(name2):
        return False, "flavor_mismatch"

    INVALID_BRANDS = {"n/d", "nan", "none", ""}
    b1, b2 = brand1.lower(), brand2.lower()
    if b1 not in INVALID_BRANDS and b2 not in INVALID_BRANDS:
        if fuzz.ratio(b1, b2) < 85:
            return False, f"brand_mismatch ({b1!r} vs {b2!r})"

    return True, f"tset={tset:.1f} tsrt={tsrt:.1f}"


# ── Test cases ─────────────────────────────────────────────────────────────────
# Format: (name1, name2, brand1, brand2, expect_merge, description)
TESTS = [
    # ── SHOULD MERGE ──────────────────────────────────────────────────────────
    (
        "Creatina ON 300 Grs", "ON Creatina Powder 300 gr",
        "Optimum Nutrition", "Optimum Nutrition", True,
        "Root-cause pair: word-order swap, was missed with TSR-only"
    ),
    (
        "SERIOUS MASS 12 LBS - OPTIMUM NUTRITION",
        "Optimun Nutrition mass gainer Serious Mass 12lb",
        "Optimum Nutrition", "Optimum Nutrition", True,
        "Serious Mass 12lb pair, tset≈84 (borderline at threshold=83)"
    ),
    (
        "Whey Protein 1kg Chocolate", "Chocolate Whey Protein 1 kg",
        "N/D", "N/D", True,
        "Word-order swap with flavor/size"
    ),
    (
        "ON Gold Standard Whey 2lb", "Gold Standard Whey ON 2 lb",
        "Optimum Nutrition", "Optimum Nutrition", True,
        "Brand abbreviation shuffled in name"
    ),

    # ── SHOULD SEPARATE (Round 1 false positives) ────────────────────────────
    (
        "Protein Bar Chocolate 60g", "Protein Bar Cereal 60g",
        "N/D", "N/D", False,
        "Different flavors: chocolate vs cereal"
    ),
    (
        "Whey Isolate 1kg Vanilla", "Whey Concentrate 1kg Vanilla",
        "N/D", "N/D", False,
        "isolate vs concentrate – critical keyword"
    ),
    (
        "Omega 3 Fish Oil 1000mg 60 caps", "Omega 3 Ultra Pure 1000mg 60 caps",
        "N/D", "N/D", False,
        "ultrapure keyword differentiator"
    ),
    (
        "ZMA 90 caps", "ZMA Pro 90 caps",
        "N/D", "N/D", False,
        "pro keyword differentiator"
    ),
    (
        "Ashwagandha 60 caps", "Ashwagandha KSM-66 60 caps",
        "N/D", "N/D", False,
        "Ashwagandha base vs KSM-66 branded extract – ksm keyword should block"
    ),
    (
        "Creatina Monohidrato 300g", "Creatina CGT 300g",
        "N/D", "N/D", False,
        "cgt keyword separates creatine sub-lines"
    ),
    (
        "L-Carnitina Liquida 500ml", "L-Carnitina 90 caps",
        "N/D", "N/D", False,
        "Size mismatch: 500ml vs 90 caps"
    ),
    (
        "Complejo B 100 tabs", "Complejo B Forte 100 tabs",
        "N/D", "N/D", False,
        "forte keyword differentiator"
    ),
    (
        "Vitamina C 1000mg 60 tabs", "Vitamina D3 1000mg 60 tabs",
        "N/D", "N/D", False,
        "Different vitamins: C vs D3 – d3 keyword"
    ),
    (
        "Penne Proteico 250g", "Espaguetis Proteicos 250g",
        "N/D", "N/D", False,
        "Different pasta shapes – penne vs espaguetis keywords"
    ),

    # ── SHOULD SEPARATE (Round 2 false positives) ────────────────────────────
    (
        "Soul Protein Bar Chocolate 50g", "Protein Bar Chocolate 50g",
        "Wild Soul", "N/D", False,
        "soul keyword differentiator for Wild Soul brand"
    ),
    (
        "4Chef Pasta Proteica Penne 250g", "Pasta Proteica Penne 250g",
        "N/D", "N/D", False,
        "4chef keyword differentiator"
    ),
    (
        "Snack Proteico Chocolate 40g", "Bite Proteico Chocolate 40g",
        "N/D", "N/D", False,
        "snack vs bite keywords"
    ),
    (
        "Curcuma 500mg 60 caps", "Curcuma Forte 500mg 60 caps",
        "N/D", "N/D", False,
        "forte keyword on curcuma variant"
    ),
    (
        "Extracto de Melena de Leon 60 caps", "Melena de Leon 60 caps",
        "N/D", "N/D", False,
        "extracto keyword differentiator"
    ),
    (
        "Vitamina C 500mg 30 caps", "Vitamina C 500mg 60 caps",
        "N/D", "N/D", False,
        "Different sizes: 30 vs 60 caps"
    ),
    (
        "Espresso Cremoso 250g", "Espresso Intenso 250g",
        "N/D", "N/D", False,
        "cremoso vs intenso – cremoso in flavor list AND critical keyword"
    ),
    (
        "Omega 3 DHA 1000mg 60 caps", "Omega 3 TG 1000mg 60 caps",
        "N/D", "N/D", False,
        "DHA vs TG sub-formulas"
    ),
    (
        "Chocolate 70 Cacao 100g", "Chocolate 79 Cacao 100g",
        "N/D", "N/D", False,
        "Cacao percentage mismatch: 70 vs 79"
    ),
    (
        "Protein Bar Kiwi 50g", "Protein Bar Tropical 50g",
        "N/D", "N/D", False,
        "Different new flavors: kiwi vs tropical"
    ),
    (
        "Protein Bar Cereza 50g", "Protein Bar Durazno 50g",
        "N/D", "N/D", False,
        "Different new flavors: cereza vs durazno"
    ),
    (
        "Protein Bar Guarana 50g", "Protein Bar Cherry 50g",
        "N/D", "N/D", False,
        "Different new flavors: guarana vs cherry"
    ),
    # ── RISKY: protein keyword ─────────────────────────────────────────────────
    # protein is in check_critical_mismatch. If one has "protein" and other doesn't,
    # they'll be separated. This is fine for "Protein Bar" vs "Cereal Bar".
    # Note: "Gold Standard Whey Protein" vs "Whey Protein" is correctly SEPARATED
    # because 'gold' fires as a mismatch (one has it, the other doesn't).
    (
        "Whey Protein 1kg Vanilla", "Gold Standard Whey Protein 1kg Vanilla",
        "Optimum Nutrition", "Optimum Nutrition", False,
        "gold keyword correctly blocks: Gold Standard is a different tier than plain Whey Protein"
    ),
    (
        "Protein Bar Chocolate 60g", "Bar Chocolate 60g",
        "N/D", "N/D", False,
        "RISKY: protein in one name only – protein keyword should block"
    ),
]


def run_tests():
    passed = 0
    failed = 0
    warnings = []

    for i, (n1, n2, b1, b2, expect, desc) in enumerate(TESTS, 1):
        result, reason = should_merge(n1, n2, b1, b2)
        status = "PASS" if result == expect else "FAIL"

        if result != expect:
            failed += 1
            print(f"[{status}] #{i}: {desc}")
            print(f"       name1: {n1!r}")
            print(f"       name2: {n2!r}")
            print(f"       expected={'MERGE' if expect else 'SEPARATE'}, got={'MERGE' if result else 'SEPARATE'}, reason={reason}")
        else:
            passed += 1
            if "RISKY" in desc:
                warnings.append(f"  #{i} (RISKY, passing): {desc} | reason={reason}")
            # Uncomment to see all passes:
            # print(f"[{status}] #{i}: {desc} ({reason})")

    print(f"\n{'='*60}")
    print(f"Results: {passed}/{passed+failed} passed, {failed} failed")

    if warnings:
        print("\nWarnings (risky cases that are currently passing):")
        for w in warnings:
            print(w)

    return failed == 0


if __name__ == "__main__":
    ok = run_tests()
    sys.exit(0 if ok else 1)
