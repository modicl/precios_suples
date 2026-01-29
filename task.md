# Tasks

- [/] Investigate database for duplicate entries of "BICARBONATO DE SODIO POLVO 250 GR" <!-- id: 0 -->
    - [x] Check `productos`, `tiendas`, `producto_tienda` (Clean)
    - [ ] Check `historia_precios` for duplicates on same date <!-- id: 4 -->
- [ ] Analyze `v_main_query_home` view definition for potential duplication logic flaws <!-- id: 1 -->
- [ ] Fix the underlying cause <!-- id: 2 -->
    - [x] Remove duplicates from `historia_precios` <!-- id: 5 -->
    - [x] Add Unique Constraint to `historia_precios` or update insertion logic <!-- id: 6 -->
- [ ] Verify fix <!-- id: 3 -->

# Normalization Refinement
- [x] Investigate `normalize_products.py` logic for clustering "Bicarbonato" <!-- id: 7 -->
- [x] Adjust normalization/clustering to respect "Polvo" distinction <!-- id: 8 -->
- [x] Verify separation of variants <!-- id: 9 -->

# Iterative Clustering Refinement
- [ ] Add missing flavors (Almendra, Menta, etc.) to extraction logic <!-- id: 10 -->
- [ ] Add missing critical keywords (Nightime, Munchy) <!-- id: 11 -->
- [ ] Run normalization and evaluate matches <!-- id: 12 -->
- [ ] Verify specific problem cases are resolved <!-- id: 13 -->
