"""
CategoryClassifier.py
---------------------
Módulo centralizado de clasificación de productos para todos los scrapers.

Uso básico:
    from CategoryClassifier import CategoryClassifier
    classifier = CategoryClassifier()
    final_category, final_subcategory = classifier.classify(
        title, description, main_category, deterministic_subcategory, brand
    )

Uso con overrides (lógica extra del scraper):
    # Los overrides se aplican ANTES de la lógica base, permitiendo que
    # cada scraper mantenga su comportamiento específico.
    # Si el override retorna None, se aplica la lógica base.
    classifier = CategoryClassifier()
    # En extract_process, después de llamar a classify(), el scraper puede
    # aplicar su propia lógica adicional sobre el resultado.
"""

import json
import re
import unicodedata
import os

# Directorio donde se encuentran los JSON de keywords
_DICT_DIR = os.path.join(os.path.dirname(__file__), "diccionarios")


def _load_json(filename):
    path = os.path.join(_DICT_DIR, filename)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def normalize(text):
    """Elimina tildes y convierte a minúsculas para comparación uniforme."""
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


class CategoryClassifier:
    """
    Clasificador centralizado de productos.

    Carga los keywords desde los archivos JSON en diccionarios/ y aplica
    la lógica de clasificación consolidada de todos los scrapers.

    Cada scraper puede seguir teniendo su propia lógica adicional DESPUÉS
    de llamar a classify(), ya que el clasificador provee la base común.
    """

    def __init__(self):
        self._global      = _load_json("keywords_global.json")
        self._proteinas   = _load_json("keywords_proteinas.json")
        self._creatinas   = _load_json("keywords_creatinas.json")
        self._vitaminas   = _load_json("keywords_vitaminas.json")
        self._aminoacidos = _load_json("keywords_aminoacidos.json")
        self._pre_entrenos   = _load_json("keywords_pre_entrenos.json")
        self._perdida_grasa  = _load_json("keywords_perdida_grasa.json")
        self._ganadores      = _load_json("keywords_ganadores.json")
        self._snacks         = _load_json("keywords_snacks.json")
        self._bebidas        = _load_json("keywords_bebidas.json")

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _any(self, text, keywords):
        """Retorna True si alguna keyword está en el texto (ya normalizado)."""
        return any(k in text for k in keywords)

    def _classify_proteinas(self, text, title_lower, brand):
        """Clasifica subcategoría dentro de Proteinas."""
        kw = self._proteinas

        # Lógica especial para marca Dymatize
        if brand.lower() == "dymatize":
            if self._any(text, kw["dymatize_aislada"]):
                return "Proteína Aislada"
            elif self._any(text, kw["dymatize_hidrolizada"]):
                return "Proteína Hidrolizada"
            else:
                return "Proteína de Whey"

        # Vegana (alta prioridad)
        if self._any(text, kw["vegana"]):
            return "Proteína Vegana"

        # Carne
        if self._any(text, kw["carne"]):
            return "Proteína de Carne"

        # Caseína
        if self._any(text, kw["caseina"]):
            return "Caseína"

        # Purity Rule: si hay concentrado/blend/mezcla (limpiando frases benignas),
        # es Whey estándar aunque diga "iso" o "hydro"
        purity_text = text
        for phrase in kw["benign_phrases"]:
            purity_text = purity_text.replace(normalize(phrase), "")

        if (self._any(purity_text, kw["whey_concentrado"]) or
                self._any(purity_text, kw["whey_combinacion"]) or
                self._any(purity_text, kw["whey_mezcla"])):
            return "Proteína de Whey"

        # Aislada (con word boundary para "iso")
        if (re.search(r'\biso\b', text) or
                self._any(text, [k for k in kw["aislada"] if k != "iso"])):
            return "Proteína Aislada"

        # Hidrolizada
        if self._any(text, kw["hidrolizada"]):
            return "Proteína Hidrolizada"

        return kw["fallback"]

    def _classify_creatinas(self, text, title_lower):
        """Clasifica subcategoría dentro de Creatinas."""
        kw = self._creatinas

        # HCL tiene prioridad, pero si el título dice monohidrato, ese gana
        if self._any(text, kw["hcl"]):
            if (self._any(title_lower, kw["monohidrato"]) or
                    self._any(title_lower, kw["creapure_sello"])):
                return "Creatina Monohidrato"
            return "Creatina HCL"

        if self._any(text, kw["malato_magnesio"]):
            return "Malato y Magnesio"

        if self._any(text, kw["nitrato"]):
            return "Nitrato"

        if self._any(text, kw["alkalyn"]):
            return "Otros Creatinas"

        # Creapure es sello de monohidrato premium
        if self._any(text, kw["creapure_sello"]):
            return "Sello Creapure"

        if self._any(text, kw["monohidrato"]):
            return "Creatina Monohidrato"

        if self._any(text, kw["micronizada"]):
            return "Micronizada"

        return kw["fallback"]

    def _classify_vitaminas(self, text):
        """Clasifica subcategoría dentro de Vitaminas y Minerales."""
        kw = self._vitaminas

        # Orden de prioridad: específicos primero, genéricos al final
        checks = [
            (kw["multivitaminicos"], "Multivitamínicos"),
            (kw["magnesio"],         "Magnesio"),
            (kw["zinc"],             "Otros Vitaminas y Minerales"),
            (kw["omega"],            "Omega 3 y Aceites"),
            (kw["colageno"],         "Colágeno"),
            (kw["calcio"],           "Calcio"),
            (kw["probioticos"],      "Probióticos"),
            (kw["vitamina_b"],       "Vitamina B / Complejo B"),
            (kw["vitamina_c"],       "Vitamina C"),
            (kw["vitamina_d"],       "Vitamina D"),
            (kw["vitamina_e"],       "Vitamina E"),
            (kw["antioxidantes"],    "Antioxidantes"),
            (kw["bienestar"],        "Bienestar General"),
            (kw["gummies"],          "Gummies"),
        ]
        for keywords, subcategory in checks:
            if self._any(text, keywords):
                return subcategory

        return kw["fallback"]

    def _classify_aminoacidos(self, text, title_lower):
        """
        Clasifica subcategoría dentro de Aminoacidos y BCAA.
        Nota: ZMA puede reclasificar a Vitaminas y Minerales.
        """
        kw = self._aminoacidos

        # ZMA → reclasifica a Vitaminas
        if self._any(text, kw["zma"]):
            return "Vitaminas y Minerales", "Multivitamínicos"

        # Minerales (Magnesio/Zinc en nombre)
        if self._any(title_lower, kw["minerales"]):
            return "Aminoacidos y BCAA", "Minerales (Magnesio/ZMA)"

        # HMB
        if self._any(text, kw["hmb"]):
            return "Aminoacidos y BCAA", "Otros Aminoacidos y BCAA"

        # EAAs
        if self._any(text, kw["eaas"]):
            return "Aminoacidos y BCAA", "EAAs (Esenciales)"

        # BCAAs
        if self._any(text, kw["bcaa"]):
            return "Aminoacidos y BCAA", "BCAAs"

        # Glutamina
        if self._any(text, kw["glutamina"]):
            return "Aminoacidos y BCAA", "Glutamina"

        # Leucina
        if self._any(text, kw["leucina"]):
            return "Aminoacidos y BCAA", "Leucina"

        # Aminoácidos específicos
        if self._any(text, kw["aminoacidos_especificos"]):
            return "Aminoacidos y BCAA", "Aminoácidos"

        return "Aminoacidos y BCAA", kw["fallback"]

    def _classify_pre_entrenos(self, text):
        """Clasifica subcategoría dentro de Pre Entrenos."""
        kw = self._pre_entrenos
        if self._any(text, kw["geles_energia"]):
            return "Energía (Geles/Café)"
        if self._any(text, kw["cafeina"]):
            return "Cafeína"
        if self._any(text, kw["beta_alanina"]):
            return "Beta Alanina"
        if self._any(text, kw["oxido_nitrico"]):
            return "Óxido Nítrico"
        if self._any(text, kw["otros_pre_entrenos"]):
            return "Otros Pre Entrenos"
        return kw["fallback"]

    def _classify_perdida_grasa(self, text):
        """Clasifica subcategoría dentro de Perdida de Grasa."""
        kw = self._perdida_grasa
        if self._any(text, kw["cafeina"]):
            return "Cafeína"
        if self._any(text, kw["carnitina"]):
            return "L-Carnitina"
        if self._any(text, kw["quemadores_liquidos"]):
            return "Quemadores Liquidos"
        if self._any(text, kw["quemadores_naturales"]):
            return "Quemadores Naturales"
        if self._any(text, kw["eliminadores_retencion"]):
            return "Eliminadores De Retencion"
        if self._any(text, kw["cremas_reductoras"]):
            return "Cremas Reductoras"
        if self._any(text, kw["quemadores_localizados"]):
            return "Quemadores Localizados"
        if self._any(text, kw["termogenicos"]):
            return "Quemadores Termogenicos"
        return kw["fallback"]

    def _classify_ganadores(self, text):
        """Clasifica subcategoría dentro de Ganadores de Peso."""
        kw = self._ganadores
        return kw["fallback"]  # Solo hay una subcategoría

    def _classify_snacks(self, text):
        """Clasifica subcategoría dentro de Snacks y Comida."""
        kw = self._snacks
        if self._any(text, kw["mantequilla_mani"]):
            return "Mantequilla De Mani"
        if self._any(text, kw["barritas_proteicas"]):
            return "Barritas Y Snacks Proteicas"
        if self._any(text, kw["snacks_dulces"]):
            return "Snacks Dulces"
        if self._any(text, kw["snacks_salados"]):
            return "Snacks Salados"
        if self._any(text, kw["cereales"]):
            return "Cereales"
        return kw["fallback"]

    def _classify_bebidas(self, text):
        """Clasifica subcategoría dentro de Bebidas Nutricionales."""
        kw = self._bebidas
        if self._any(text, kw["isotonicos"]):
            return "Isotónicos"
        if self._any(text, kw["bebidas_energeticas"]):
            return "Bebidas Energéticas"
        if self._any(text, kw["geles"]):
            return "Geles Energéticos"
        if self._any(text, kw["batidos_proteina"]):
            return "Batidos de proteína"
        if self._any(text, kw["aloe_vera"]):
            return "Otros Bebidas Nutricionales"
        if self._any(text, kw["otras_bebidas"]):
            return "Otros Bebidas Nutricionales"
        return kw["fallback"]

    # ------------------------------------------------------------------
    # Método principal
    # ------------------------------------------------------------------

    def classify(self, title, description, main_category,
                 deterministic_subcategory, brand=""):
        """
        Clasifica un producto y retorna (final_category, final_subcategory).

        Parámetros
        ----------
        title : str
            Nombre del producto.
        description : str | None
            Descripción del producto (puede ser vacía o None).
        main_category : str
            Categoría principal determinada por la URL del scraper.
        deterministic_subcategory : str
            Subcategoría determinista asignada por la URL del scraper.
            Si ya es una subcategoría válida (no es un marcador como
            'CATEGORIZAR_PROTEINA'), se usa como punto de partida.
        brand : str
            Marca del producto (usada para lógica especial, ej. Dymatize).

        Retorna
        -------
        (final_category, final_subcategory) : tuple[str, str]
        """
        final_category = main_category
        final_subcategory = deterministic_subcategory

        title_norm = normalize(title.lower())
        desc_norm = normalize((description or "").lower())
        full_text = title_norm + " " + desc_norm

        gkw = self._global

        # ---------------------------------------------------------------
        # 1. Filtro global: Accesorios / Llaveros → OTROS
        # ---------------------------------------------------------------
        if self._any(title_norm, gkw["accesorios"]):
            return "OTROS", "Otros"

        # ---------------------------------------------------------------
        # 2. Packs (global, alta prioridad)
        # ---------------------------------------------------------------
        # "pack" se verifica con word boundary para evitar falsos positivos
        # con palabras como "Doypack", "Backpack", etc.
        _pack_kw_no_pack = [k for k in gkw["packs"] if k != "pack"]
        is_pack = (
            bool(re.search(r'\bpack\b', title_norm)) or
            self._any(title_norm, _pack_kw_no_pack) or
            any(sep in title_norm for sep in gkw["pack_plus_separator"])
        )
        if is_pack:
            return "Packs", "Packs"

        # ---------------------------------------------------------------
        # 3. Bebidas RTD (global)
        # ---------------------------------------------------------------
        is_liquid = self._any(title_norm, gkw["liquid_volume_markers"])
        is_powder = self._any(title_norm, gkw["powder_weight_markers"])

        # Un producto es isotónico si su título contiene keywords de isotónicos.
        # En ese caso NO aplicamos el fallback RTD (que fuerza "Batidos de proteína")
        # y dejamos que el paso 4 lo clasifique correctamente como "Isotónicos".
        is_isotonic = self._any(title_norm, self._bebidas["isotonicos"])

        # Bebidas de aloe vera o coco no son batidos de proteína: se excluyen
        # del RTD global para que lleguen al paso 4 y se clasifiquen correctamente
        # como "Otros Bebidas Nutricionales".
        is_aloe_or_coco = (
            self._any(title_norm, self._bebidas["aloe_vera"]) or
            self._any(title_norm, self._bebidas["otras_bebidas"])
        )

        if self._any(title_norm, gkw["rtd_explicit"]) and not is_isotonic and not is_aloe_or_coco:
            return "Bebidas Nutricionales", "Batidos de proteína"

        rtd_words = [w for w in gkw["rtd_liquid_indicators"]
                     if w not in gkw.get("rtd_exclusion", [])]
        if self._any(title_norm, rtd_words) and not is_isotonic and not is_aloe_or_coco:
            # Solo es bebida si tiene indicador de volumen o no tiene peso de polvo
            if is_liquid or not is_powder:
                return "Bebidas Nutricionales", "Batidos de proteína"

        # ---------------------------------------------------------------
        # 4. Clasificación por categoría principal
        # ---------------------------------------------------------------
        if final_category == "Proteinas":
            final_subcategory = self._classify_proteinas(full_text, title_norm, brand)

        elif final_category == "Creatinas":
            final_subcategory = self._classify_creatinas(full_text, title_norm)

        elif final_category == "Vitaminas y Minerales":
            # Usamos solo el título para vitaminas (más preciso, evita falsos positivos)
            final_subcategory = self._classify_vitaminas(title_norm)

        elif final_category == "Aminoacidos y BCAA":
            final_category, final_subcategory = self._classify_aminoacidos(
                full_text, title_norm
            )

        elif final_category == "Pre Entrenos":
            final_subcategory = self._classify_pre_entrenos(full_text)

        elif final_category == "Perdida de Grasa":
            final_subcategory = self._classify_perdida_grasa(full_text)

        elif final_category == "Ganadores de Peso":
            final_subcategory = self._classify_ganadores(full_text)

        elif final_category == "Snacks y Comida":
            final_subcategory = self._classify_snacks(full_text)

        elif final_category == "Bebidas Nutricionales":
            final_subcategory = self._classify_bebidas(full_text)

        return final_category, final_subcategory
