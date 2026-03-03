"""
step6_tag_keywords.py
---------------------
Asigna keywords (tags) a productos usando LLM.

Lógica:
  - Fetch de productos desde la BD Local (sin tags, a menos que --forzar)
  - Por cada producto, toma las keywords disponibles del diccionario JSON
    usando nombre_categoria como clave directa (coincide exactamente con la BD)
  - Llama a GPT-4o-mini para que elija máximo 5 keywords del listado
  - Guarda el array en productos.tags (text[] en PostgreSQL) en Local y Producción
  - Genera siempre un reporte CSV con muestra de 50 productos

Uso:
    python local_processing_testing/step6_tag_keywords.py
    python local_processing_testing/step6_tag_keywords.py --dry-run
    python local_processing_testing/step6_tag_keywords.py --forzar
    python local_processing_testing/step6_tag_keywords.py --concurrencia 15

Flags:
    --dry-run         Muestra prompts/respuestas sin escribir en BD
    --forzar          Regenera tags incluso para productos que ya los tienen
    --concurrencia N  Requests paralelos a OpenAI (default: 10)
"""

import argparse
import asyncio
import csv
import json
import os
import random
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from openai import AsyncOpenAI

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

OPENAI_API_KEY    = os.getenv("CHATGPT_MINI4")
MODEL             = "gpt-4o-mini"
MAX_OUTPUT_TOKENS = 80
TEMPERATURE       = 0.1   # Baja temperatura para clasificación determinista
RETRY_ATTEMPTS    = 3
RETRY_DELAY       = 5

DICT_PATH         = Path(__file__).parent / "dictionaries" / "category_tag_keywords.json"
REPORT_DIR        = Path(__file__).parent / "data" / "reports"
REPORT_SAMPLE_SIZE = 50

SYSTEM_PROMPT = """\
Eres un experto en nutrición deportiva y suplementación con amplio conocimiento del mercado \
chileno. Tu tarea es asignar keywords descriptivas a productos de suplementos deportivos \
para un comparador de precios.

OBJETIVO: Seleccionar las keywords que mejor describan los ATRIBUTOS y CARACTERÍSTICAS del \
producto — lo que lo diferencia dentro de su categoría y ayuda al usuario a encontrarlo \
mediante filtros o búsqueda.

CRITERIOS DE SELECCIÓN (aplica en este orden de prioridad):
1. Formato del producto: polvo, cápsulas, líquido, gomas, barra, sobres, etc.
2. Objetivo o beneficio específico: masa muscular, definición, hidratación, energía, etc.
3. Atributos especiales: vegano, sin lactosa, keto, sin cafeína, sin gluten, orgánico, etc.
4. Momento de uso: pre entreno, intra entreno, post entreno, nocturno, diario, etc.
5. Público objetivo: hombre, mujer, principiante, atleta avanzado, etc.

REGLAS ESTRICTAS:
- Responde SOLO con un JSON array de strings. Sin texto adicional, sin markdown, sin explicaciones.
- Elige entre 1 y 3 keywords (máximo 3). Prefiere 1-2 bien elegidas antes que 3 genéricas.
- Usa las keywords EXACTAMENTE tal como aparecen en la lista proporcionada, sin modificarlas.
- NUNCA elijas una keyword que ya esté expresada en el nombre del producto, la categoría \
o la subcategoría del producto. Eso sería información redundante.
- Prioriza lo ESPECÍFICO sobre lo general: "sin lactosa" es mejor que "saludable".
- Si una característica es claramente visible en el nombre o descripción (ej: "polvo", \
"sin cafeína"), inclúyela. Si no estás seguro de una característica, NO la incluyas.

EJEMPLO DE BUENA CLASIFICACIÓN:
Producto: "Gold Standard 100% Whey Vanilla" | Categoría: Proteinas | Subcategoría: Proteína de Whey
Lista disponible: ["polvo", "saborizada", "sin lactosa"]
  ❌ Mal: ["whey", "proteina", "masa muscular"] → "whey" ya está en nombre y subcategoría
  ✅ Bien: ["polvo", "saborizada", "baja en grasa"]

Responde únicamente con el JSON array. Ejemplo: ["polvo", "vegana", "post entreno"]
"""


# ---------------------------------------------------------------------------
# Diccionario de keywords
# ---------------------------------------------------------------------------
def load_keyword_dict() -> dict:
    with open(DICT_PATH, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------
def build_user_prompt(row: dict, available_keywords: list[str]) -> str:
    lines = [
        f"Nombre: {row['nombre_producto']}",
        f"Marca: {row['nombre_marca']}",
        f"Categoría: {row['nombre_categoria']}",
        f"Subcategoría: {row['nombre_subcategoria']}",
    ]

    if row.get("descripcion_llm"):
        lines.append(f"Descripción: {row['descripcion_llm'][:300]}")

    descs = row.get("descs") or []
    seen, unique_descs = set(), []
    for d in descs:
        if not d:
            continue
        key = " ".join(d.lower().split())[:100]
        if key not in seen:
            seen.add(key)
            unique_descs.append(d[:300].strip())

    if unique_descs:
        lines.append("\nDescripciones de tiendas:")
        for i, d in enumerate(unique_descs[:3], 1):
            lines.append(f"[{i}] {d}")

    lines.append(f"\nKeywords disponibles para \"{row['nombre_categoria']}\":")
    lines.append(", ".join(available_keywords))
    lines.append("\nSelecciona las 1-5 más relevantes para este producto específico.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn(url: str) -> psycopg2.extensions.connection:
    return psycopg2.connect(url)


def fetch_products(conn, *, force: bool) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # array_length('{}', 1) retorna NULL en PG, cubre tanto NULL como array vacío
    where = "" if force else "WHERE (p.tags IS NULL OR array_length(p.tags, 1) IS NULL)"
    cur.execute(f"""
        SELECT
            p.id_producto,
            p.nombre_producto,
            m.nombre_marca,
            c.nombre_categoria,
            s.nombre_subcategoria,
            p.descripcion_llm,
            array_agg(pt.descripcion ORDER BY LENGTH(pt.descripcion) DESC NULLS LAST) AS descs
        FROM productos p
        JOIN marcas        m  ON p.id_marca        = m.id_marca
        JOIN subcategorias s  ON p.id_subcategoria  = s.id_subcategoria
        JOIN categorias    c  ON s.id_categoria     = c.id_categoria
        LEFT JOIN producto_tienda pt ON p.id_producto = pt.id_producto
        {where}
        GROUP BY p.id_producto, p.nombre_producto, m.nombre_marca,
                 c.nombre_categoria, s.nombre_subcategoria, p.descripcion_llm
        ORDER BY p.id_producto
    """)
    return [dict(r) for r in cur.fetchall()]


def save_tags(targets: list[dict], producto_id: int, tags: list[str]):
    """Escribe los tags en todos los targets (Local + Producción)."""
    for target in targets:
        try:
            conn = get_conn(target["url"])
            cur = conn.cursor()
            cur.execute(
                "UPDATE productos SET tags = %s WHERE id_producto = %s",
                (tags, producto_id),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"\n  [WARN] Error guardando tags en {target['name']} "
                  f"(id={producto_id}): {e}")


# ---------------------------------------------------------------------------
# LLM call (async, con retry + backoff exponencial)
# ---------------------------------------------------------------------------
async def generate_tags(
    client: AsyncOpenAI,
    row: dict,
    available_keywords: list[str],
    semaphore: asyncio.Semaphore,
    dry_run: bool,
) -> tuple[int, Optional[list[str]], Optional[str]]:
    """Retorna (id_producto, tags_list, error_msg)."""
    pid = row["id_producto"]
    user_prompt = build_user_prompt(row, available_keywords)

    if dry_run:
        print(f"\n{'='*60}")
        print(f"PRODUCTO {pid}: {row['nombre_producto']} ({row['nombre_marca']})")
        print(f"{'─'*60}")
        print(user_prompt)
        print(f"{'─'*60}")

    async with semaphore:
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                response = await client.chat.completions.create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": user_prompt},
                    ],
                    max_tokens=MAX_OUTPUT_TOKENS,
                    temperature=TEMPERATURE,
                )
                raw = response.choices[0].message.content.strip()

                # Parsear JSON array
                tags = json.loads(raw)
                if not isinstance(tags, list):
                    raise ValueError(f"Respuesta no es un array: {raw}")

                # Filtrar solo keywords permitidas y limitar a 5
                allowed_set = set(available_keywords)
                tags = [t for t in tags if t in allowed_set][:5]

                if dry_run:
                    print(f"TAGS ASIGNADOS: {tags}")

                return (pid, tags, None)

            except Exception as e:
                if attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                else:
                    return (pid, None, str(e))

    return (pid, None, "semaphore never released")  # unreachable


# ---------------------------------------------------------------------------
# CSV report
# ---------------------------------------------------------------------------
def write_csv_report(all_results: list[dict], *, is_muestra: bool = False) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    if is_muestra:
        # En modo muestra ya se tomó N por categoría — escribir todo
        rows_to_write = sorted(all_results, key=lambda x: (x["nombre_categoria"], x["id_producto"]))
        suffix = "muestra"
    else:
        rows_to_write = random.sample(all_results, min(REPORT_SAMPLE_SIZE, len(all_results)))
        rows_to_write.sort(key=lambda x: x["id_producto"])
        suffix = "sample"

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filepath  = REPORT_DIR / f"step7_tags_{suffix}_{timestamp}.csv"

    fieldnames = [
        "id_producto", "nombre_producto", "nombre_marca",
        "nombre_categoria", "nombre_subcategoria",
        "tags_asignados", "tags_disponibles",
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_to_write:
            writer.writerow({
                "id_producto":         row["id_producto"],
                "nombre_producto":     row["nombre_producto"],
                "nombre_marca":        row["nombre_marca"],
                "nombre_categoria":    row["nombre_categoria"],
                "nombre_subcategoria": row["nombre_subcategoria"],
                "tags_asignados":      json.dumps(row["tags"], ensure_ascii=False),
                "tags_disponibles":    json.dumps(row["available_keywords"], ensure_ascii=False),
            })

    print(f"\n  Reporte CSV guardado: {filepath}")
    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def run(args):
    if not OPENAI_API_KEY:
        print("ERROR: Variable de entorno CHATGPT_MINI4 no encontrada.")
        sys.exit(1)

    from shared.db_multiconnect import get_targets
    targets = get_targets()
    if not targets:
        print("ERROR: No se encontró ninguna BD configurada en .env")
        sys.exit(1)

    keyword_dict = load_keyword_dict()

    # Leer siempre desde Local (targets[0])
    conn_source = get_conn(targets[0]["url"])
    rows = fetch_products(conn_source, force=args.forzar)
    conn_source.close()

    if not rows:
        print("Paso 6 — Tags: todos los productos ya tienen tags asignados. Nada que hacer.")
        return

    # Solo procesar productos cuya categoría esté en el diccionario
    rows_with_keywords = [
        (r, keyword_dict[r["nombre_categoria"]])
        for r in rows
        if r["nombre_categoria"] in keyword_dict
    ]
    skipped = len(rows) - len(rows_with_keywords)

    # Sampling por categoría si se pasa --muestra N
    if args.muestra:
        by_cat: dict[str, list] = defaultdict(list)
        for r, kws in rows_with_keywords:
            by_cat[r["nombre_categoria"]].append((r, kws))
        sampled = []
        for cat_items in by_cat.values():
            random.shuffle(cat_items)
            sampled.extend(cat_items[:args.muestra])
        rows_with_keywords = sampled

    total = len(rows_with_keywords)

    target_names = " + ".join(t["name"] for t in targets)
    modo = "DRY-RUN (sin escritura)" if args.dry_run else f"ESCRITURA en BD ({target_names})"
    print(f"\n--- Paso 6: Asignación de Keywords/Tags ({MODEL}) ---")
    print(f"Productos sin tags             : {len(rows)}")
    print(f"Con categoría en diccionario   : {total}")
    if args.muestra:
        print(f"Modo muestra                   : {args.muestra} por categoría")
    if skipped:
        print(f"Omitidos (categoría no mapeada): {skipped}")
    print(f"Modo                           : {modo}")
    print(f"Concurrencia                   : {args.concurrencia}")
    print()

    if total == 0:
        print("  Nada que procesar (ningún producto tiene categoría mapeada).")
        return

    client    = AsyncOpenAI(api_key=OPENAI_API_KEY)
    semaphore = asyncio.Semaphore(args.concurrencia)

    tasks = [
        generate_tags(client, row, kws, semaphore, dry_run=args.dry_run)
        for row, kws in rows_with_keywords
    ]

    # Lookup para recuperar datos originales al procesar resultados
    row_lookup = {r["id_producto"]: (r, kws) for r, kws in rows_with_keywords}

    ok_count    = 0
    error_count = 0
    errors      = []
    all_results = []   # Para el reporte CSV
    t_start     = time.time()

    for i, coro in enumerate(asyncio.as_completed(tasks), 1):
        pid, tags, err = await coro
        elapsed = time.time() - t_start
        row, kws = row_lookup[pid]

        if err:
            error_count += 1
            errors.append((pid, err))
            status = "ERROR"
        else:
            ok_count += 1
            status = "OK   "
            if not args.dry_run:
                save_tags(targets, pid, tags)
            all_results.append({
                **row,
                "tags":               tags,
                "available_keywords": kws,
            })

        rps = i / elapsed if elapsed > 0 else 0
        eta = (total - i) / rps if rps > 0 else 0
        print(
            f"  [{i:>4}/{total}] [{status}] id={pid:<6} "
            f"({elapsed:.0f}s elapsed, ETA {eta:.0f}s)",
            end="\r" if not args.dry_run else "\n",
        )

    elapsed_total = time.time() - t_start
    print()
    print(f"\n{'='*50}")
    print(f"  Asignados OK : {ok_count}")
    print(f"  Errores      : {error_count}")
    print(f"  Tiempo total : {elapsed_total:.1f}s")
    if total > 0:
        print(f"  Tiempo/prod  : {elapsed_total / total:.2f}s")

    if errors:
        print(f"\n  IDs con error:")
        for pid, err in errors:
            print(f"    id={pid}: {err[:120]}")

    # Reporte CSV siempre que haya resultados
    if all_results:
        write_csv_report(all_results, is_muestra=bool(args.muestra))

    if error_count > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Paso 7: asigna keywords/tags a productos usando LLM."
    )
    parser.add_argument("--dry-run",      action="store_true",
                        help="Muestra prompts y respuestas sin escribir en BD")
    parser.add_argument("--forzar",       action="store_true",
                        help="Regenera tags incluso para productos que ya los tienen")
    parser.add_argument("--concurrencia", type=int, default=10,
                        help="Requests paralelos a OpenAI (default: 10)")
    parser.add_argument("--muestra",      type=int, default=None, metavar="N",
                        help="Procesa solo N productos por categoría y guarda el resultado en CSV (ej: --muestra 50)")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
