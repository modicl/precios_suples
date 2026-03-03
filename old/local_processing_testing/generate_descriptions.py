"""
generate_descriptions.py
------------------------
Genera descripciones de productos usando GPT-4o mini y las guarda
en la columna `productos.descripcion_llm`.

Uso:
    python local_processing_testing/generate_descriptions.py
    python local_processing_testing/generate_descriptions.py --dry-run --limite 3
    python local_processing_testing/generate_descriptions.py --producto-id 414
    python local_processing_testing/generate_descriptions.py --forzar
    python local_processing_testing/generate_descriptions.py --concurrencia 15

Flags:
    --dry-run          Muestra prompt y respuesta sin escribir en BD
    --forzar           Regenera productos que ya tienen descripcion_llm
    --limite N         Procesa solo los primeros N productos
    --producto-id ID   Procesa solo ese producto
    --concurrencia N   Requests paralelos a OpenAI (default: 10)
"""

import argparse
import asyncio
import os
import sys
import time
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from openai import AsyncOpenAI

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("CHATGPT_MINI4")
MODEL          = "gpt-4o-mini"
MAX_OUTPUT_TOKENS = 200
TEMPERATURE       = 0.3     # bajo para consistencia
RETRY_ATTEMPTS    = 3
RETRY_DELAY       = 5       # segundos entre reintentos

SYSTEM_PROMPT = """\
Eres un redactor de fichas técnicas de suplementos deportivos para un comparador \
de precios chileno. Tu tarea es escribir UNA descripción de 2 a 3 oraciones en \
español, con tono informativo y directo, sin frases de marketing vacías.

La descripción debe incluir:
1. Qué es el producto y su tipo (ej: proteína aislada hidrolizada, creatina monohidratada, etc.)
2. Su beneficio o uso principal
3. Dato clave si está disponible: proteína por porción, cantidad de servings, tamaño del envase, \
ingrediente destacado o público objetivo

Reglas:
- No menciones nombres de tiendas ni precios
- No uses superlativos vacíos ("el mejor", "increíble", "revolucionario")
- Si el producto es vegano o para mujeres y eso es relevante, inclúyelo
- Responde SOLO con la descripción, sin títulos ni bullet points
- Máximo 60 palabras
"""


def build_user_prompt(row: dict) -> str:
    """Construye el prompt de usuario con los atributos del producto."""
    lines = [
        f"Nombre: {row['nombre_producto']}",
        f"Marca: {row['nombre_marca']}",
        f"Categoría: {row['nombre_categoria']}",
        f"Subcategoría: {row['nombre_subcategoria']}",
    ]

    extras = []
    if row.get("is_vegan"):
        extras.append("vegano")
    if row.get("is_women"):
        extras.append("orientado a mujeres")
    if extras:
        lines.append(f"Características: {', '.join(extras)}")

    # Descripciones de tiendas — deduplicar y limpiar
    descs = row.get("descs") or []
    seen, unique_descs = set(), []
    for d in descs:
        if not d:
            continue
        key = " ".join(d.lower().split())[:120]
        if key not in seen:
            seen.add(key)
            # Truncar descripción muy larga para no desperdiciar tokens
            unique_descs.append(d[:600].strip())

    if unique_descs:
        lines.append("\nDescripciones de tiendas:")
        for i, d in enumerate(unique_descs, 1):
            lines.append(f"[{i}] {d}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    from tools.db_multiconnect import get_targets
    targets = get_targets()
    local = next((t for t in targets if t["name"] == "Local"), None)
    if not local:
        print("ERROR: No se encontró BD local en .env")
        sys.exit(1)
    return psycopg2.connect(local["url"])


def fetch_products(conn, *, force: bool, producto_id: Optional[int], limite: Optional[int]) -> list[dict]:
    """Recupera los productos a procesar con todas sus descripciones de tiendas."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    where_clauses = []
    params = []

    if producto_id is not None:
        where_clauses.append("p.id_producto = %s")
        params.append(producto_id)
    elif not force:
        where_clauses.append("p.descripcion_llm IS NULL")

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    limit = f"LIMIT {limite}" if limite else ""

    cur.execute(f"""
        SELECT
            p.id_producto,
            p.nombre_producto,
            m.nombre_marca,
            c.nombre_categoria,
            s.nombre_subcategoria,
            p.is_vegan,
            p.is_women,
            array_agg(pt.descripcion ORDER BY LENGTH(pt.descripcion) DESC) AS descs
        FROM productos p
        JOIN marcas        m  ON p.id_marca        = m.id_marca
        JOIN subcategorias s  ON p.id_subcategoria  = s.id_subcategoria
        JOIN categorias    c  ON s.id_categoria     = c.id_categoria
        LEFT JOIN producto_tienda pt ON p.id_producto = pt.id_producto
        {where}
        GROUP BY p.id_producto, p.nombre_producto, m.nombre_marca,
                 c.nombre_categoria, s.nombre_subcategoria,
                 p.is_vegan, p.is_women
        ORDER BY p.id_producto
        {limit}
    """, params)

    return [dict(r) for r in cur.fetchall()]


def save_description(conn, producto_id: int, descripcion: str):
    cur = conn.cursor()
    cur.execute(
        "UPDATE productos SET descripcion_llm = %s WHERE id_producto = %s",
        (descripcion, producto_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------
async def generate_one(
    client: AsyncOpenAI,
    row: dict,
    semaphore: asyncio.Semaphore,
    dry_run: bool,
) -> tuple[int, Optional[str], Optional[str]]:
    """
    Retorna (id_producto, descripcion, error_msg).
    descripcion es None si hubo error o dry_run.
    """
    pid = row["id_producto"]
    user_prompt = build_user_prompt(row)

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
                desc = response.choices[0].message.content.strip()

                if dry_run:
                    print(f"RESPUESTA:\n{desc}")

                return (pid, desc, None)

            except Exception as e:
                err = str(e)
                if attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                else:
                    return (pid, None, err)

    return (pid, None, "semaphore never released")  # unreachable


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def run(args):
    if not OPENAI_API_KEY:
        print("ERROR: Variable de entorno CHATGPT_MINI4 no encontrada.")
        sys.exit(1)

    conn   = get_conn()
    rows   = fetch_products(
        conn,
        force=args.forzar,
        producto_id=args.producto_id,
        limite=args.limite,
    )

    if not rows:
        print("No hay productos para procesar.")
        conn.close()
        return

    total = len(rows)
    print(f"\n--- Generación de Descripciones LLM ({MODEL}) ---")
    print(f"Productos a procesar : {total}")
    print(f"Modo                 : {'DRY-RUN (sin escritura)' if args.dry_run else 'ESCRITURA en BD'}")
    print(f"Concurrencia         : {args.concurrencia}")
    print()

    client    = AsyncOpenAI(api_key=OPENAI_API_KEY)
    semaphore = asyncio.Semaphore(args.concurrencia)

    tasks = [
        generate_one(client, row, semaphore, dry_run=args.dry_run)
        for row in rows
    ]

    ok_count    = 0
    error_count = 0
    errors      = []
    t_start     = time.time()

    # Procesar con barra de progreso simple
    for i, coro in enumerate(asyncio.as_completed(tasks), 1):
        pid, desc, err = await coro

        if err:
            error_count += 1
            errors.append((pid, err))
            status = "ERROR"
        else:
            ok_count += 1
            status = "OK"
            if not args.dry_run:
                save_description(conn, pid, desc)

        elapsed = time.time() - t_start
        rps     = i / elapsed if elapsed > 0 else 0
        eta     = (total - i) / rps if rps > 0 else 0
        print(
            f"  [{i:>4}/{total}] [{status:5}] id={pid:<6} "
            f"({elapsed:.0f}s elapsed, ETA {eta:.0f}s)",
            end="\r" if not args.dry_run else "\n",
        )

    elapsed_total = time.time() - t_start
    print()
    print(f"\n{'='*50}")
    print(f"  Generados OK : {ok_count}")
    print(f"  Errores      : {error_count}")
    print(f"  Tiempo total : {elapsed_total:.1f}s")
    if total > 0:
        print(f"  Tiempo/prod  : {elapsed_total/total:.2f}s")

    if errors:
        print(f"\n  IDs con error:")
        for pid, err in errors:
            print(f"    id={pid}: {err[:100]}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Genera descripciones de productos con GPT-4o mini."
    )
    parser.add_argument("--dry-run",       action="store_true",
                        help="Muestra prompts y respuestas sin escribir en BD")
    parser.add_argument("--forzar",        action="store_true",
                        help="Regenera productos que ya tienen descripcion_llm")
    parser.add_argument("--limite",        type=int, default=None,
                        help="Procesa solo los primeros N productos")
    parser.add_argument("--producto-id",   type=int, default=None,
                        help="Procesa solo este id_producto")
    parser.add_argument("--concurrencia",  type=int, default=10,
                        help="Requests paralelos a OpenAI (default: 10)")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
