"""
step3_logic.py — Lógica pura (sin BD) del step 3 de inserción.

Contexto: el CSV normalizado puede traer más de un día de scraping por tienda
(p.ej. si un run anterior no se archivó). El step 3 usa:
  - el scrapeo MÁS RECIENTE de cada tienda para el estado actual: enlaces,
    URLs "vistas" (desactivación) y desempate de URLs duplicadas;
  - TODOS los días para rellenar historia_precios.

Los dedups de precios se aplican por (enlace, día), nunca entre días: si no,
el precio del día más nuevo se pierde (ver obsidian/bugs/step3-multi-dia-precios-perdidos.md).
"""
import unicodedata

import pandas as pd

# Subcategorías de menor prioridad: si la misma URL aparece en dos categorías
# el mismo día, se prefiere la fila cuya subcategoría NO sea de baja prioridad.
LOW_PRIORITY_SUBCATS = {"packs", "ofertas", "promo", "bundle", "outlet"}


def _key(value):
    """Lowercase + strip + NFC (misma normalización que make_key del step 3)."""
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value).lower().strip()
    return ""


def _day(value):
    """'YYYY-MM-DD' de un string/datetime/Timestamp; None si falta."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    return str(value)[:10]


def latest_scrape_mask(df):
    """
    Serie booleana: True para las filas del día de scrapeo más reciente de su
    tienda. Las partes de un mismo runner (mismo día) cuentan como un solo
    scrapeo. Filas sin fecha se consideran recientes (el step 3 les asigna now()).
    """
    days = df["date"].map(_day)
    sites = df["site_name"].map(_key)
    dated = days.notna()
    latest_by_site = days[dated].groupby(sites[dated]).max()
    latest = sites.map(latest_by_site)
    return ~dated | latest.isna() | (days == latest)


# Protección de la desactivación (Fase 2b del step 3).
# Una corrida parcial (ej: 53 de 395 productos) haría creer que el resto "ya no existe"
# y lo desactivaría. Si la corrida trae menos de MIN_RATIO de los productos hoy activos
# de la tienda, se omite la desactivación de esa tienda.
DEACTIVATION_MIN_RATIO = 0.5
DEACTIVATION_MIN_BASE = 10   # tiendas con menos activos que esto: sin protección (muestra chica)


def should_skip_deactivation(seen_count, active_count,
                             min_ratio=DEACTIVATION_MIN_RATIO,
                             min_base=DEACTIVATION_MIN_BASE):
    """
    True si NO se debe desactivar a la tienda en este run.

    seen_count:   URLs distintas de la tienda que llegaron en esta corrida.
    active_count: productos hoy activos (is_active = true) de esa tienda en la BD.
    """
    if active_count < min_base:
        return False
    return seen_count < active_count * min_ratio


def _is_low_priority(row):
    subcat = _key(str(row.get("subcategory", "")))
    return any(kw in subcat for kw in LOW_PRIORITY_SUBCATS)


def dedup_links(rows):
    """
    Un enlace por (producto, tienda) y una sola fila ganadora por (tienda, URL).

    rows: iterable de dicts con id_producto, id_tienda, url_link, subcategory,
          descripcion (en orden de aparición en el CSV).
    Retorna (links_dedup, url_winner, conflict_count):
      links_dedup  — {(id_producto, id_tienda): row}
      url_winner   — {(id_tienda, url_normalizada): id_producto ganador}
      conflict_count — filas con URL repetida y distinto producto
    """
    links_dedup = {}
    url_winner = {}
    conflicts = 0

    for row in rows:
        pid, tid = row["id_producto"], row["id_tienda"]
        url_key = (tid, _key(row["url_link"]))

        existing_pid = url_winner.get(url_key)
        if existing_pid is not None and existing_pid != pid:
            conflicts += 1
            subcat = _key(str(row.get("subcategory", "")))
            if any(kw in subcat for kw in LOW_PRIORITY_SUBCATS):
                continue  # fila de baja prioridad: gana la anterior
            links_dedup.pop((existing_pid, tid), None)  # la específica reemplaza

        # Un producto puede traer dos filas para la misma tienda (misma identidad en dos
        # subcategorías, ver resolve_products): no deja que una fila de baja prioridad
        # (Ofertas/Packs) reemplace a una específica.
        prev = links_dedup.get((pid, tid))
        if prev is not None and _is_low_priority(row) and not _is_low_priority(prev):
            url_winner[url_key] = pid
            continue

        url_winner[url_key] = pid
        links_dedup[(pid, tid)] = row

    return links_dedup, url_winner, conflicts


def dedup_prices(price_rows, lowest_price_sites):
    """
    Un precio por (enlace, día).

    price_rows: iterable de dicts {id_producto_tienda, precio, fecha_precio, site_key}.
    - Tiendas en lowest_price_sites (URLs duplicadas con precios distintos):
      gana el precio menor del día (empate: la captura más reciente).
    - Resto: gana la captura más reciente del día (empate: la última en el input).
    Retorna lista de dicts; el orden no está garantizado.
    """
    best = {}
    for p in price_rows:
        key = (p["id_producto_tienda"], _day(p["fecha_precio"]))
        cur = best.get(key)
        if cur is None:
            best[key] = p
            continue

        fecha_new, fecha_cur = str(p["fecha_precio"]), str(cur["fecha_precio"])
        if p["site_key"] in lowest_price_sites:
            replace = p["precio"] < cur["precio"] or (
                p["precio"] == cur["precio"] and fecha_new >= fecha_cur
            )
        else:
            replace = fecha_new >= fecha_cur
        if replace:
            best[key] = p
    return list(best.values())


# ---------------------------------------------------------------------------
# Identidad de producto: (nombre normalizado, marca), SIN subcategoría.
#
# Antes la clave era (nombre, marca, subcategoría): el mismo producto clasificado en dos
# subcategorías nacía como dos filas y el step 4 las fusionaba (y borraba) en cada corrida.
# Ahora se decide ANTES de insertar: si la identidad ya existe se reutiliza su id.
# El puntaje del maestro es el mismo del step 4.
# ---------------------------------------------------------------------------
LOW_PRIORITY_CATEGORY_KEYWORDS = [
    "oferta", "pack", "promo", "bundle", "especial",
    "outlet", "liquidacion", "cyber", "black",
]


def identity_key(name):
    """Nombre sin tildes, con espacios colapsados y en minúsculas ('Energética  X' == 'energetica x')."""
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.split()).lower()


def product_score(cat_name, prod_name, known_categories):
    """Mismo criterio que get_score del step 4 (categoría, no subcategoría)."""
    score = 0
    cat_lower = (cat_name or "").lower()
    if cat_name in known_categories:
        score += 10
    if any(kw in cat_lower for kw in LOW_PRIORITY_CATEGORY_KEYWORDS):
        score -= 100
    if prod_name and not prod_name.islower():
        score += 20
    return score


def master_index(existing, known_categories):
    """
    {(identity_key, id_marca): fila maestra} entre los productos ya existentes.
    existing: iterable de dicts {id, nombre, id_marca, id_subcategoria, cat_name}.
    Mayor puntaje gana; en empate, el id menor (más antiguo).
    """
    best = {}
    for r in existing:
        ident = (identity_key(r["nombre"]), r["id_marca"])
        score = product_score(r.get("cat_name"), r["nombre"], known_categories)
        cur = best.get(ident)
        if cur is None or (-score, r["id"]) < (-cur[0], cur[1]["id"]):
            best[ident] = (score, r)
    return {k: v[1] for k, v in best.items()}


def resolve_products(batch, existing, known_categories):
    """
    Decide qué hacer con cada identidad (nombre normalizado, marca) del lote.

    batch:    dicts {nombre, id_marca, id_subcategoria, cat_name, url_imagen, url_thumb_imagen}
    existing: dicts {id, nombre, id_marca, id_subcategoria, cat_name} (productos ya en BD)
    Retorna dict con:
      insert          — filas nuevas (la mejor subcategoría del lote, una por identidad)
      touch           — maestro existente + imágenes del lote (se pasa por el mismo upsert
                        para refrescar imágenes sin crear filas)
      subcat_updates  — [(id_producto, nueva_subcategoria)] solo si el lote trae una
                        categoría con puntaje ESTRICTAMENTE mayor (ej: sale de Packs/Ofertas)
      collapsed       — filas del lote absorbidas por otra de la misma identidad
    """
    masters = master_index(existing, known_categories)

    groups = {}
    for b in batch:
        groups.setdefault((identity_key(b["nombre"]), b["id_marca"]), []).append(b)

    insert, touch, subcat_updates, collapsed = [], [], [], 0
    for ident, rows in groups.items():
        collapsed += len(rows) - 1
        best = rows[0]
        best_score = product_score(best.get("cat_name"), best["nombre"], known_categories)
        for r in rows[1:]:
            sc = product_score(r.get("cat_name"), r["nombre"], known_categories)
            if sc > best_score:          # empate: gana la primera fila del lote
                best, best_score = r, sc

        master = masters.get(ident)
        if master is None:
            insert.append(best)
            continue

        touch.append({
            "id": master["id"], "nombre": master["nombre"], "id_marca": master["id_marca"],
            "id_subcategoria": master["id_subcategoria"],
            "url_imagen": best.get("url_imagen"), "url_thumb_imagen": best.get("url_thumb_imagen"),
        })
        master_score = product_score(master.get("cat_name"), master["nombre"], known_categories)
        if best_score > master_score and best["id_subcategoria"] != master["id_subcategoria"]:
            subcat_updates.append((master["id"], best["id_subcategoria"]))

    return {"insert": insert, "touch": touch, "subcat_updates": subcat_updates, "collapsed": collapsed}
