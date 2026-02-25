"""
BrandClassifier — Clasificador canónico de marcas.

Dos métodos principales:
  - normalize_brand(raw_brand): Mapea texto crudo del DOM a nombre canónico.
    Usar cuando el scraper ya extrajo la marca del HTML (scrapers deterministas).
  - extract_from_title(title): Escanea el nombre del producto buscando keywords.
    Usar SOLO en scrapers sin campo de marca en el DOM (DrSimi, KoteSport, etc.).

El método classify() combina ambos y es el punto de entrada principal para BaseScraper.
"""

import json
import os
import re


class BrandClassifier:
    _instance = None
    _brands: dict = {}      # { canonical_name: [kw1, kw2, ...] }
    _kw_index: list = []    # [(keyword_str, canonical_name), ...] sorted longest-first

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_brands()
        return cls._instance

    # ------------------------------------------------------------------
    # Carga de diccionario
    # ------------------------------------------------------------------

    def _load_brands(self):
        """Carga keywords_marcas.json y construye el índice de búsqueda."""
        if self._brands:
            return

        possible_paths = [
            os.path.join(os.path.dirname(__file__), "diccionarios", "keywords_marcas.json"),
            os.path.join(os.path.dirname(__file__), "..", "scrapers_v2", "diccionarios", "keywords_marcas.json"),
            "scrapers_v2/diccionarios/keywords_marcas.json",
            "diccionarios/keywords_marcas.json",
        ]

        json_path = None
        for p in possible_paths:
            if os.path.exists(p):
                json_path = p
                break

        if not json_path:
            print("[BrandClassifier] WARN: keywords_marcas.json no encontrado.")
            return

        try:
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)

            BrandClassifier._brands = data

            # Construir índice plano ordenado de mayor a menor keyword para que
            # "winkler nutrition" gane sobre "winkler"
            index = []
            for canonical, meta in data.items():
                for kw in meta.get("keywords", []):
                    index.append((kw.lower().strip(), canonical))

            index.sort(key=lambda x: len(x[0]), reverse=True)
            BrandClassifier._kw_index = index

            print(f"[BrandClassifier] {len(data)} marcas cargadas ({len(index)} keywords).")
        except Exception as e:
            print(f"[BrandClassifier] ERROR cargando JSON: {e}")

    # ------------------------------------------------------------------
    # Normalización interna
    # ------------------------------------------------------------------

    @staticmethod
    def _norm(text: str) -> str:
        """Lowercase, colapsa espacios, elimina puntuación extra."""
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r"[^\w\s]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    # ------------------------------------------------------------------
    # Métodos públicos
    # ------------------------------------------------------------------

    def normalize_brand(self, raw_brand: str) -> str:
        """
        Mapea el texto crudo de la marca del DOM a su nombre canónico.

        Uso: scrapers que obtienen la marca directamente del HTML pero puede
        llegar con variantes (ej. "WINKLER NUTRI" → "WINKLER NUTRITION").

        Si no encuentra match devuelve raw_brand tal cual (preserva el original
        en vez de silenciarlo, para que enrich_brand pueda detectar que es válido).
        """
        if not raw_brand or not raw_brand.strip():
            return ""

        norm = self._norm(raw_brand)

        for kw, canonical in self._kw_index:
            # Comparar keyword de forma exacta contra la cadena normalizada
            # Usamos startswith/endswith + espacios para evitar falsos positivos
            # en strings cortos como "san" vs "san nutrition"
            pattern = r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])"
            if re.search(pattern, norm):
                return canonical

        # Sin match: devolver limpio pero sin alterar (no "N/D")
        return raw_brand.strip()

    def extract_from_title(self, title: str) -> str:
        """
        Escanea el nombre del producto buscando keywords de marcas.

        Uso: scrapers sin campo de marca en el DOM (DrSimi, KoteSport como
        fallback). Devuelve "N/D" si no encuentra nada.
        """
        if not title or not title.strip():
            return "N/D"

        norm = self._norm(title)

        for kw, canonical in self._kw_index:
            pattern = r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])"
            if re.search(pattern, norm):
                return canonical

        return "N/D"

    def classify(self, raw_brand: str, product_name: str = "", scan_title: bool = False) -> str:
        """
        Punto de entrada principal para BaseScraper.enrich_brand().

        Lógica:
          1. Si raw_brand es válido → normalize_brand() para corregir escritura.
          2. Si raw_brand es "N/D"/vacío y scan_title=True → extract_from_title().
          3. Fallback: "N/D".

        Args:
            raw_brand:    Marca extraída del DOM (puede ser vacía o "N/D").
            product_name: Nombre del producto (para escanear si scan_title=True).
            scan_title:   True SOLO en scrapers sin marca en el DOM.
        """
        invalid = {"n/d", "nd", "n.d.", ""}
        if raw_brand and raw_brand.strip().lower() not in invalid:
            return self.normalize_brand(raw_brand)

        if scan_title:
            return self.extract_from_title(product_name)

        return "N/D"
