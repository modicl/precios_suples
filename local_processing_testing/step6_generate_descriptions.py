"""
step6_generate_descriptions.py
-------------------------------
Genera descripciones LLM para productos que aún no tienen `descripcion_llm`.
Usa GPT-4o mini y escribe en `productos.descripcion_llm`.

Diseñado para correr después del pipeline (steps 1-5): solo procesa productos
nuevos o sin descripción, por lo que es rápido en ejecuciones incrementales.

Uso directo:
    python local_processing_testing/step6_generate_descriptions.py
    python local_processing_testing/step6_generate_descriptions.py --dry-run
    python local_processing_testing/step6_generate_descriptions.py --forzar
    python local_processing_testing/step6_generate_descriptions.py --concurrencia 15

Flags:
    --dry-run         Muestra prompts y respuestas sin escribir en BD
    --forzar          Regenera también productos que ya tienen descripcion_llm
    --concurrencia N  Requests paralelos a OpenAI (default: 10)
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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

OPENAI_API_KEY    = os.getenv("CHATGPT_MINI4")
MODEL             = "gpt-4o-mini"
MAX_OUTPUT_TOKENS = 200
TEMPERATURE       = 0.3
RETRY_ATTEMPTS    = 3
RETRY_DELAY       = 5      # segundos base entre reintentos (backoff exponencial)

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


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------
def build_user_prompt(row: dict) -> str:
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

    descs = row.get("descs") or []
    seen, unique_descs = set(), []
    for d in descs:
        if not d:
            continue
        key = " ".join(d.lower().split())[:120]
        if key not in seen:
            seen.add(key)
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


def fetch_products(conn, *, force: bool) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    where = "" if force else "WHERE p.descripcion_llm IS NULL"

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
    """)

    return [dict(r) for r in cur.fetchall()]


def save_description(conn, producto_id: int, descripcion: str):
    cur = conn.cursor()
    cur.execute(
        "UPDATE productos SET descripcion_llm = %s WHERE id_producto = %s",
        (descripcion, producto_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# LLM call (async, with retry + exponential backoff)
# ---------------------------------------------------------------------------
async def generate_one(
    client: AsyncOpenAI,
    row: dict,
    semaphore: asyncio.Semaphore,
    dry_run: bool,
) -> tuple[int, Optional[str], Optional[str]]:
    """Retorna (id_producto, descripcion, error_msg)."""
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
                if attempt < RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY * attempt)
                else:
                    return (pid, None, str(e))

    return (pid, None, "semaphore never released")  # unreachable


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def run(args):
    if not OPENAI_API_KEY:
        print("ERROR: Variable de entorno CHATGPT_MINI4 no encontrada.")
        sys.exit(1)

    conn = get_conn()
    rows = fetch_products(conn, force=args.forzar)

    if not rows:
        print("Paso 6 — Descripciones LLM: todos los productos ya tienen descripcion_llm. Nada que hacer.")
        conn.close()
        return

    total = len(rows)
    modo  = "DRY-RUN (sin escritura)" if args.dry_run else "ESCRITURA en BD"
    print(f"\n--- Paso 6: Generación de Descripciones LLM ({MODEL}) ---")
    print(f"Productos sin descripcion_llm : {total}")
    print(f"Modo                          : {modo}")
    print(f"Concurrencia                  : {args.concurrencia}")
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

    for i, coro in enumerate(asyncio.as_completed(tasks), 1):
        pid, desc, err = await coro
        elapsed = time.time() - t_start

        if err:
            error_count += 1
            errors.append((pid, err))
            status = "ERROR"
        else:
            ok_count += 1
            status = "OK   "
            if not args.dry_run:
                save_description(conn, pid, desc)

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
    print(f"  Generados OK : {ok_count}")
    print(f"  Errores      : {error_count}")
    print(f"  Tiempo total : {elapsed_total:.1f}s")
    if total > 0:
        print(f"  Tiempo/prod  : {elapsed_total / total:.2f}s")

    if errors:
        print(f"\n  IDs con error:")
        for pid, err in errors:
            print(f"    id={pid}: {err[:120]}")

    conn.close()

    # Salir con código de error si algún producto falló, para que el .bat lo detecte
    if error_count > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Paso 6: genera descripcion_llm para productos nuevos o sin descripción."
    )
    parser.add_argument("--dry-run",      action="store_true",
                        help="Muestra prompts y respuestas sin escribir en BD")
    parser.add_argument("--forzar",       action="store_true",
                        help="Regenera también productos que ya tienen descripcion_llm")
    parser.add_argument("--concurrencia", type=int, default=10,
                        help="Requests paralelos a OpenAI (default: 10)")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
