"""
Tests de la lógica pura del step 3 (shared/step3_logic.py).

Ejecutar desde la raíz del proyecto:
    python -m unittest discover -s tests -v

Los casos replican los fallos reales del run del 2026-10-05, en que
latest_normalized.csv traía dos días de scraping (2-oct y 5-oct).
"""
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shared.step3_logic import (  # noqa: E402
    latest_scrape_mask,
    dedup_links,
    dedup_prices,
    should_skip_deactivation,
)


def _df(rows):
    return pd.DataFrame(rows, columns=["date", "site_name", "link"])


class LatestScrapeMaskTests(unittest.TestCase):
    def test_marca_solo_el_dia_mas_reciente_de_cada_tienda(self):
        df = _df([
            ("2026-10-02 15:46:40", "AllNutrition", "a"),
            ("2026-10-05 16:26:02", "AllNutrition", "a"),
            ("2026-10-02 16:04:26", "Dr Simi", "b"),  # esta tienda no corrió el 5-oct
        ])
        self.assertEqual(latest_scrape_mask(df).tolist(), [False, True, True])

    def test_partes_del_mismo_dia_cuentan_como_el_mismo_scrapeo(self):
        df = _df([
            ("2026-10-05 16:22:10", "ChileSuplementos", "a"),
            ("2026-10-05 17:03:41", "ChileSuplementos", "b"),
        ])
        self.assertEqual(latest_scrape_mask(df).tolist(), [True, True])

    def test_fecha_faltante_se_trata_como_reciente(self):
        df = _df([
            (None, "AllNutrition", "a"),
            ("2026-10-02 15:46:40", "AllNutrition", "b"),
        ])
        self.assertEqual(latest_scrape_mask(df).tolist(), [True, True])

    def test_nombre_de_tienda_se_compara_normalizado(self):
        df = _df([
            ("2026-10-02 10:00:00", "Suples.cl", "a"),
            ("2026-10-05 10:00:00", " suples.cl ", "a"),
        ])
        self.assertEqual(latest_scrape_mask(df).tolist(), [False, True])


class DedupLinksTests(unittest.TestCase):
    def _row(self, pid, url, subcat, tid=1):
        return {"id_producto": pid, "id_tienda": tid, "url_link": url,
                "subcategory": subcat, "descripcion": ""}

    def test_misma_url_en_baja_prioridad_pierde_contra_la_especifica(self):
        rows = [
            self._row(10, "https://x/abe", "Pre Entreno"),
            self._row(99, "https://x/abe", "Ofertas"),
        ]
        links, winners, conflicts = dedup_links(rows)
        self.assertEqual(list(links), [(10, 1)])
        self.assertEqual(winners[(1, "https://x/abe")], 10)
        self.assertEqual(conflicts, 1)

    def test_la_fila_especifica_posterior_reemplaza_a_la_de_baja_prioridad(self):
        rows = [
            self._row(99, "https://x/abe", "Packs"),
            self._row(10, "https://x/abe", "Pre Entreno"),
        ]
        links, winners, _ = dedup_links(rows)
        self.assertEqual(list(links), [(10, 1)])
        self.assertEqual(winners[(1, "https://x/abe")], 10)

    def test_urls_distintas_no_entran_en_conflicto(self):
        rows = [self._row(1, "https://x/a", "Proteinas"),
                self._row(2, "https://x/b", "Proteinas")]
        links, _, conflicts = dedup_links(rows)
        self.assertEqual(set(links), {(1, 1), (2, 1)})
        self.assertEqual(conflicts, 0)


class DedupPricesTests(unittest.TestCase):
    def _p(self, ptid, precio, fecha, site="allnutrition"):
        return {"id_producto_tienda": ptid, "precio": precio,
                "fecha_precio": fecha, "site_key": site}

    def test_el_minimo_se_aplica_por_dia_y_no_entre_dias(self):
        # Caso real: wild-protein-vegana costaba 1390 el 2-oct y 1590 el 5-oct.
        rows = [
            self._p(1, 1390, "2026-10-02 15:46:40"),
            self._p(1, 1590, "2026-10-05 16:26:02"),
        ]
        out = {r["fecha_precio"][:10]: r["precio"] for r in dedup_prices(rows, {"allnutrition"})}
        self.assertEqual(out, {"2026-10-02": 1390, "2026-10-05": 1590})

    def test_precio_igual_en_dos_dias_conserva_ambos_dias(self):
        # Caso real: 43 ofertas de AllNutrition perdían la fila del 5-oct.
        rows = [
            self._p(1, 3890, "2026-10-02 15:46:40"),
            self._p(1, 3890, "2026-10-05 16:26:02"),
        ]
        out = dedup_prices(rows, {"allnutrition"})
        self.assertEqual(sorted(r["fecha_precio"][:10] for r in out), ["2026-10-02", "2026-10-05"])

    def test_mismo_dia_en_tienda_de_precio_minimo_gana_el_menor(self):
        rows = [
            self._p(1, 5000, "2026-10-05 10:00:00"),
            self._p(1, 4500, "2026-10-05 16:00:00"),
            self._p(1, 4800, "2026-10-05 17:00:00"),
        ]
        out = dedup_prices(rows, {"allnutrition"})
        self.assertEqual([r["precio"] for r in out], [4500])

    def test_mismo_dia_en_tienda_normal_gana_la_ultima_captura(self):
        rows = [
            self._p(1, 5000, "2026-10-05 10:00:00", site="byon"),
            self._p(1, 4800, "2026-10-05 17:00:00", site="byon"),
        ]
        out = dedup_prices(rows, {"allnutrition"})
        self.assertEqual([r["precio"] for r in out], [4800])

    def test_enlaces_distintos_no_se_mezclan(self):
        rows = [self._p(1, 100, "2026-10-05 10:00:00"),
                self._p(2, 200, "2026-10-05 10:00:00")]
        out = dedup_prices(rows, {"allnutrition"})
        self.assertEqual(sorted(r["id_producto_tienda"] for r in out), [1, 2])

    def test_fecha_como_datetime_tambien_funciona(self):
        from datetime import datetime
        rows = [self._p(1, 100, datetime(2026, 10, 5, 10, 0)),
                self._p(1, 90, datetime(2026, 10, 5, 11, 0))]
        out = dedup_prices(rows, {"allnutrition"})
        self.assertEqual([r["precio"] for r in out], [90])


class ShouldSkipDeactivationTests(unittest.TestCase):
    """Casos reales de OneNutrition (~395 productos activos)."""

    def test_corrida_parcial_del_23_oct_no_desactiva(self):
        self.assertTrue(should_skip_deactivation(seen_count=53, active_count=395))

    def test_corrida_del_2_nov_con_78_por_ciento_pasa_el_umbral(self):
        # 309 de 395 (78%) supera el 50%: la protección NO la frena (se desactiva lo no visto)
        self.assertFalse(should_skip_deactivation(seen_count=309, active_count=395))

    def test_corrida_completa_si_desactiva(self):
        self.assertFalse(should_skip_deactivation(seen_count=397, active_count=395))

    def test_justo_en_el_umbral_si_desactiva(self):
        self.assertFalse(should_skip_deactivation(seen_count=50, active_count=100))
        self.assertTrue(should_skip_deactivation(seen_count=49, active_count=100))

    def test_tienda_chica_sin_proteccion(self):
        # menos de 10 activos: una caída grande es plausible (muestra chica)
        self.assertFalse(should_skip_deactivation(seen_count=1, active_count=9))

    def test_tienda_nueva_sin_activos(self):
        self.assertFalse(should_skip_deactivation(seen_count=50, active_count=0))


if __name__ == "__main__":
    unittest.main()
