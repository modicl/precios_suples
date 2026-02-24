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
        self._pre_entrenos      = _load_json("keywords_pre_entrenos.json")
        self._perdida_grasa     = _load_json("keywords_perdida_grasa.json")
        self._ganadores         = _load_json("keywords_ganadores.json")
        self._snacks            = _load_json("keywords_snacks.json")
        self._bebidas           = _load_json("keywords_bebidas.json")
        self._pro_hormonales    = _load_json("keywords_pro_hormonales.json")

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

        # Vegana — solo título: evita falsos positivos por descripciones que
        # mencionan ingredientes vegetales en proteínas de suero convencionales
        if self._any(title_lower, kw["vegana"]):
            # Excepción: si el título también contiene señales de snack/barra,
            # redirigir a Snacks antes de clasificar como vegana.
            if self._any(title_lower, kw["snacks_redirect"]):
                return "__SNACK_REDIRECT__"
            return "Proteína Vegana"

        # Carne
        if self._any(text, kw["carne"]):
            return "Proteína de Carne"

        # Purity Rule: si hay concentrado/blend/mezcla (limpiando frases benignas),
        # es Whey estándar aunque diga "iso", "hydro" o "caseína" en la descripción.
        purity_text = text
        for phrase in kw["benign_phrases"]:
            purity_text = purity_text.replace(normalize(phrase), "")

        if (self._any(purity_text, kw["whey_concentrado"]) or
                self._any(purity_text, kw["whey_combinacion"]) or
                self._any(purity_text, kw["whey_mezcla"])):
            return "Proteína de Whey"

        # Caseína — se evalúa DESPUÉS de la Purity Rule para que productos con
        # mezcla (ej: Syntha-6 que contiene caseína entre sus ingredientes) no
        # sean clasificados como Caseína pura.
        if self._any(text, kw["caseina"]):
            return "Caseína"

        # Aislada (con word boundary para "iso")
        if (re.search(r'\biso\b', text) or
                self._any(text, [k for k in kw["aislada"] if k != "iso"])):
            return "Proteína Aislada"

        # Hidrolizada
        if self._any(text, kw["hidrolizada"]):
            return "Proteína Hidrolizada"

        # Whey genérico: el título dice "whey" o "suero" sin cualificadores
        # adicionales → es Proteína de Whey estándar (no Otros)
        if self._any(text, kw["whey_generico"]):
            return "Proteína de Whey"

        return kw["fallback"]

    def _classify_creatinas(self, text, title_lower):
        """Clasifica subcategoría dentro de Creatinas.

        Jerarquía (de mayor a menor especificidad):
          HCL          → Clorhidrato  (salvo que el título diga monohidrato)
          Malato/Mg    → Malato y Magnesio
          Nitrato      → Nitrato
          Alkalyn      → Otros Creatinas
          Creapure     → Creatina Monohidrato  (sello de monohidrato premium)
          Micronizada  → Micronizada  [evaluada ANTES que monohidrato porque es
                          más específica: toda micronizada es monohidratada, pero
                          la descripción suele decir "monohidratada micronizada"
                          y monohidrato ganaría si se evaluara primero]
          Monohidrato  → Creatina Monohidrato
          fallback     → Creatina Monohidrato
        """
        kw = self._creatinas

        # HCL tiene prioridad, pero si el título dice monohidrato, ese gana
        if self._any(text, kw["hcl"]):
            if (self._any(title_lower, kw["monohidrato"]) or
                    self._any(title_lower, kw["creapure_sello"])):
                return "Creatina Monohidrato"
            return "Clorhidrato"

        # Malato/Magnesio: keywords compuestos son seguros en full_text.
        # "magnesio"/"magnesium" solos solo se evalúan en el título — en la descripción
        # pueden aparecer como excipiente ("estearato de magnesio") y generar falsos positivos.
        if self._any(text, kw["malato_magnesio"]):
            return "Malato y Magnesio"
        if self._any(title_lower, kw.get("malato_magnesio_titulo_only", [])):
            return "Malato y Magnesio"

        if self._any(text, kw["nitrato"]):
            return "Nitrato"

        if self._any(text, kw["alkalyn"]):
            return "Otros Creatinas"

        # Creapure es sello de monohidrato premium → se mapea a la subcategoría estándar
        if self._any(text, kw["creapure_sello"]):
            return "Creatina Monohidrato"

        # Micronizada se evalúa ANTES que monohidrato: es más específica.
        # "monohidratada micronizada" en la descripción debe → Micronizada, no Monohidrato.
        # Se chequea primero en el título (señal fuerte) y luego en el texto completo.
        if self._any(title_lower, kw["micronizada"]):
            return "Micronizada"
        if self._any(text, kw["micronizada"]):
            return "Micronizada"

        if self._any(text, kw["monohidrato"]):
            return "Creatina Monohidrato"

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

        Jerarquía (de mayor a menor especificidad):
          ZMA          → reclasifica a Vitaminas y Minerales
          Minerales    → reclasifica a Vitaminas y Minerales
          BCAAs        → BCAAs  [se evalúa en título antes que EAAs para evitar
                          falsos positivos por frases de marketing en la descripción
                          como "aminoácidos esenciales para la recuperación"]
          EAAs         → EAAs (Esenciales)  [solo si no hay BCAA en título]
          Glutamina    → Glutamina
          Aislados     → Aminoacidos Aislados (un solo AA puro, no glutamina)
          Complejos    → Complejos de Aminoacidos (catch-all con "amino")
          fallback     → Complejos de Aminoacidos

        Nota sobre la prioridad BCAA > EAA:
          Las descripciones de producto suelen contener frases genéricas de marketing
          ("aminoácidos esenciales para el rendimiento") que disparan keywords de EAA
          aunque el producto sea un BCAA puro. Al chequear BCAA primero usando solo
          el TÍTULO (señal fuerte y controlada), evitamos este falso positivo.
          La única excepción es "EAA + BCAA" en el título: si el título contiene
          tanto "eaa" como "bcaa", EAA tiene prioridad porque es el producto más
          completo (contiene todos los esenciales, incluyendo los ramificados).
        """
        kw = self._aminoacidos

        # ZMA → reclasifica a Vitaminas y Minerales
        if self._any(text, kw["zma"]):
            return "Vitaminas y Minerales", "Multivitamínicos"

        # Minerales (Magnesio/Zinc en el título) → Vitaminas y Minerales
        if self._any(title_lower, kw["minerales"]):
            return "Vitaminas y Minerales", "Otros Vitaminas y Minerales"

        # BCAAs (título): se evalúa ANTES de EAAs para evitar falsos positivos
        # por frases de marketing en la descripción ("aminoácidos esenciales...").
        # Excepción: si el título tiene TANTO un keyword FUERTE de EAA ("eaa", "eaas",
        # "essential amino"...) como "bcaa", se trata como EAA completo.
        # Un keyword débil de EAA ("esenciales") en el título NO bloquea esta guarda:
        # e.g. "BCAA Matrix Aminoácidos esenciales" → BCAAs.
        title_has_bcaa     = self._any(title_lower, kw["bcaa"])
        title_has_eaa_strong = self._any(title_lower, kw["eaas"])
        if title_has_bcaa and not title_has_eaa_strong:
            return "Aminoacidos y BCAA", "BCAAs"

        # EAAs — se chequean primero keywords fuertes en el texto completo (title+desc),
        # y luego keywords débiles (esenciales/esencial/essential) SOLO en el título.
        # Esto previene que frases de marketing en la descripción como
        # "aminoácidos esenciales para la recuperación" clasifiquen un producto
        # de Glutamina o Aislado como EAA.
        if self._any(text, kw["eaas"]):
            return "Aminoacidos y BCAA", "EAAs (Esenciales)"
        if self._any(title_lower, kw.get("eaas_titulo_only", [])):
            return "Aminoacidos y BCAA", "EAAs (Esenciales)"

        # BCAAs (texto completo): cubre casos sin BCAA en título pero sí en descripción
        if self._any(text, kw["bcaa"]):
            return "Aminoacidos y BCAA", "BCAAs"

        # Glutamina — se evalúa con título primero para evitar falsos positivos
        # por keywords de EAA en la descripción que aparezcan antes de llegar aquí.
        # El check en title_lower captura "glutamina" / "l-glutamina" sin ruido.
        if self._any(title_lower, kw["glutamina"]):
            return "Aminoacidos y BCAA", "Glutamina"
        if self._any(text, kw["glutamina"]):
            return "Aminoacidos y BCAA", "Glutamina"

        # Aminoácidos Aislados: se evalúa primero en el título para evitar que
        # descripciones con keywords de EAA enmascaren un aislado puro.
        # Nota: Beta Alanina y L-Carnitina se excluyen aquí a propósito — deben
        # pasar por las reglas de Pre Entrenos / Pérdida de Grasa antes de llegar
        # a esta función (responsabilidad del paso 3 del classify principal).
        if self._any(title_lower, kw["aislados"]):
            return "Aminoacidos y BCAA", "Aminoácidos Aislados"
        if self._any(text, kw["aislados"]):
            return "Aminoacidos y BCAA", "Aminoácidos Aislados"

        # Complejos directos (nombres comerciales conocidos)
        if self._any(text, kw["complejos_directos"]):
            return "Aminoacidos y BCAA", "Complejos de Aminoácidos"

        # Heurística catch-all: contiene "amino" pero no encajó en ninguna
        # subcategoría específica → Complejo de Aminoácidos
        if self._any(text, kw["amino_generico"]):
            return "Aminoacidos y BCAA", "Complejos de Aminoácidos"

        return "Aminoacidos y BCAA", kw["fallback"]

    def _classify_pre_entrenos(self, text, title=None):
        """Clasifica subcategoría dentro de Pre Entrenos."""
        kw = self._pre_entrenos
        title = title or ""
        if self._any(text, kw["geles_energia"]):
            return "Energía (Geles/Café)"
        # Guaraná: evaluar título primero para evitar que "cafeína natural del guaraná"
        # en la descripción dispare Cafeína en productos que son puramente de guaraná.
        if self._any(title, kw["guarana"]):
            return "Guarana"
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

    def _classify_snacks(self, text, title=None):
        """Clasifica subcategoría dentro de Snacks y Comida."""
        kw = self._snacks
        title = title or ""
        if self._any(text, kw["mantequilla_mani"]):
            return "Mantequilla De Mani"
        # Barritas: evaluar título primero con keywords título-only para evitar
        # que descripciones genéricas (ej: "snack con sabor a barras de cereal")
        # disparen esta subcategoría en productos que no son barras proteicas.
        if self._any(title, kw["barritas_proteicas_titulo_only"]):
            return "Barritas Y Snacks Proteicas"
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
            return "Isotónicas"
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

    def _classify_pro_hormonales(self, text, title=None):
        """Clasifica subcategoría dentro de Pro Hormonales.

        Por ahora solo existe la subcategoría catch-all 'Pro Hormonales'.
        El método está preparado para ampliar con subcategorías en el futuro.
        Se verifica que el texto contenga al menos un keyword reconocido;
        si no, se retorna el fallback igualmente para no perder el producto.
        """
        kw = self._pro_hormonales
        # Evaluación título primero (señal más fuerte), luego full_text
        title = title or ""
        if self._any(title, kw["pro_hormonales"]) or self._any(text, kw["pro_hormonales"]):
            return kw["fallback"]
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
        # El separador " + " se excluye cuando ambos lados son ingredientes
        # conocidos (ej: "EAA + BCAA", "Creatina + Glutamina") para evitar
        # clasificar nombres de productos combinados como Packs.
        _supp_ingredients = [
            "eaa", "bcaa", "creatina", "creatine", "glutamin", "gluta",
            "proteina", "protein", "whey", "amino", "arginina", "arginine",
            "citrulina", "citrulline", "taurina", "taurine", "leucina",
            "leucine", "beta", "vitamina", "vitamin", "omega", "carnitin",
            # minerales — evita que "Zinc + Magnesio" se clasifique como Pack
            "magnesio", "zinc", "magne", "zma", "calcio", "calcium",
            "hierro", "iron", "potasio", "potassium", "sodio", "sodium",
        ]
        def _is_ingredient_combo(t):
            """True si el ' + ' separa ingredientes conocidos (no un pack)."""
            for sep in gkw["pack_plus_separator"]:
                if sep in t:
                    parts = t.split(sep)
                    if all(
                        any(ing in part for ing in _supp_ingredients)
                        for part in parts
                    ):
                        return True
            return False

        _pack_kw_no_pack = [k for k in gkw["packs"] if k != "pack"]
        is_pack = (
            bool(re.search(r'\bpack\b', title_norm)) or
            self._any(title_norm, _pack_kw_no_pack) or
            (
                any(sep in title_norm for sep in gkw["pack_plus_separator"])
                and not _is_ingredient_combo(title_norm)
            )
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

        # Bebidas energéticas tampoco son batidos de proteína: se excluyen del RTD
        # global para que lleguen al paso 4 y se clasifiquen como "Bebidas Energéticas".
        is_energy_drink = self._any(title_norm, self._bebidas["bebidas_energeticas"])

        if self._any(title_norm, gkw["rtd_explicit"]) and not is_isotonic and not is_aloe_or_coco and not is_energy_drink:
            return "Bebidas Nutricionales", "Batidos de proteína"

        rtd_words = [w for w in gkw["rtd_liquid_indicators"]
                     if w not in gkw.get("rtd_exclusion", [])]
        if self._any(title_norm, rtd_words) and not is_isotonic and not is_aloe_or_coco and not is_energy_drink:
            # Solo es bebida si tiene indicador de volumen o no tiene peso de polvo
            if is_liquid or not is_powder:
                return "Bebidas Nutricionales", "Batidos de proteína"

        # ---------------------------------------------------------------
        # 4. Clasificación por categoría principal
        # ---------------------------------------------------------------
        if final_category == "Proteinas":
            final_subcategory = self._classify_proteinas(full_text, title_norm, brand)
            # Redirección a Snacks: barras/cookies/etc. publicadas bajo Proteinas
            # (ej: "Caja Barras De Proteina Vegana Wafer Bar") deben ir a Snacks.
            if final_subcategory == "__SNACK_REDIRECT__":
                return "Snacks y Comida", self._classify_snacks(full_text, title_norm)

        elif final_category == "Creatinas":
            final_subcategory = self._classify_creatinas(full_text, title_norm)

        elif final_category == "Vitaminas y Minerales":
            # Usamos solo el título para vitaminas (más preciso, evita falsos positivos)
            final_subcategory = self._classify_vitaminas(title_norm)

        elif final_category == "Aminoacidos y BCAA":
            # Excepciones de negocio: Beta Alanina → Pre Entrenos,
            # L-Carnitina → Pérdida de Grasa. Se interceptan aquí para que
            # no lleguen a _classify_aminoacidos aunque el sitio los haya
            # publicado bajo la sección de aminoácidos.
            if self._any(title_norm, self._pre_entrenos["beta_alanina"]):
                final_category, final_subcategory = "Pre Entrenos", "Beta Alanina"
            elif self._any(title_norm, self._perdida_grasa["carnitina"]):
                final_category, final_subcategory = "Perdida de Grasa", "L-Carnitina"
            else:
                final_category, final_subcategory = self._classify_aminoacidos(
                    full_text, title_norm
                )

        elif final_category == "Pre Entrenos":
            # Corrección: algunos sitios ubican quemadores termogénicos (ej. Lipo 6)
            # dentro de su sección de pre-entrenos. Si el título contiene keywords
            # de termogénicos, reclasificamos a Perdida de Grasa en lugar de
            # respetar la categoría del sitio.
            if self._any(title_norm, self._perdida_grasa["termogenicos"]):
                final_category = "Perdida de Grasa"
                final_subcategory = self._classify_perdida_grasa(title_norm)
            else:
                final_subcategory = self._classify_pre_entrenos(full_text, title_norm)

        elif final_category == "Perdida de Grasa":
            final_subcategory = self._classify_perdida_grasa(full_text)

        elif final_category == "Ganadores de Peso":
            final_subcategory = self._classify_ganadores(full_text)

        elif final_category == "Snacks y Comida":
            final_subcategory = self._classify_snacks(full_text, title_norm)

        elif final_category == "Bebidas Nutricionales":
            final_subcategory = self._classify_bebidas(full_text)

        elif final_category == "Pro Hormonales":
            final_subcategory = self._classify_pro_hormonales(full_text, title_norm)

        return final_category, final_subcategory
