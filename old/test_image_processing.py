"""
test_image_processing.py
------------------------
Descarga imágenes RGBA de ChileSuplementos desde S3 y compara distintas
variantes de procesamiento. Genera un index.html para revisar los resultados.

Uso:
    python img_debugging/test_image_processing.py
    python img_debugging/test_image_processing.py --n 15
    python img_debugging/test_image_processing.py --source-dir ruta/local/
    python img_debugging/test_image_processing.py --size 250

Flags:
    --n N              Cuántas imágenes RGBA bajar desde S3 (default: 10)
    --size PX          Tamaño máx de thumbnails en el HTML (default: 220)
    --source-dir PATH  Usar directorio local en vez de S3
    --out-dir PATH     Carpeta de salida (default: img_debugging/output/)
"""

import argparse
import io
import os
import sys
import textwrap
from pathlib import Path

from PIL import Image
import numpy as np

# ---------------------------------------------------------------------------
# Variantes de procesamiento
# ---------------------------------------------------------------------------
VARIANTS = [
    {
        "id":   "A_original",
        "name": "Original (sobre gris)",
        "desc": "RGBA original sin modificar,\ncompositeado sobre gris medio\npara hacer visible la transparencia.",
        "bg":   (180, 180, 180),  # gris medio
    },
    {
        "id":   "B_current_lambda",
        "name": "Lambda actual (paste)",
        "desc": "Lo que hace el Lambda:\npaste(img, mask=alpha)\nsobre fondo blanco RGB.",
        "bg":   (255, 255, 255),
    },
    {
        "id":   "C_alpha_composite",
        "name": "alpha_composite",
        "desc": "Image.alpha_composite(grey_rgba, img)\nMatemáticamente más correcto\npara WebP con semi-transparencia.",
        "bg":   (245, 245, 245),
    },
    {
        "id":   "D_resize_rgba_first",
        "name": "Resize RGBA → composite",
        "desc": "Resize/thumbnail en modo RGBA,\nluego composite sobre gris.\nEvita LANCZOS bleeding en borde.",
        "bg":   (245, 245, 245),
    },
    {
        "id":   "E_premult_unpack",
        "name": "Unpremultiply alpha",
        "desc": "Des-premultiplicar alpha antes\nde composite. Corrige artefactos\nen WebP con premultiplied alpha.",
        "bg":   (245, 245, 245),
    },
]

THUMB_SIZE = (200, 200)   # tamaño del resize/thumbnail


def apply_variant(img_rgba: Image.Image, variant_id: str, bg_color: tuple) -> Image.Image:
    """Aplica una variante de procesamiento. Siempre retorna imagen RGB."""
    img = img_rgba.copy()

    if variant_id == "A_original":
        # Solo composite sobre gris para visualizar transparencia
        bg = Image.new("RGB", img.size, bg_color)
        bg.paste(img, mask=img.split()[3])
        return bg

    if variant_id == "B_current_lambda":
        # Igual al Lambda actual
        bg = Image.new("RGB", img.size, bg_color)
        bg.paste(img, mask=img.split()[3])
        result = bg.copy()
        if result.width > 600:
            wp = 600 / result.width
            result = result.resize((600, int(result.height * wp)), Image.Resampling.LANCZOS)
        thumb = result.copy()
        thumb.thumbnail(THUMB_SIZE, Image.Resampling.LANCZOS)
        return thumb

    if variant_id == "C_alpha_composite":
        # Composite matemáticamente correcto
        bg = Image.new("RGBA", img.size, bg_color + (255,))
        composited = Image.alpha_composite(bg, img)
        result = composited.convert("RGB")
        if result.width > 600:
            wp = 600 / result.width
            result = result.resize((600, int(result.height * wp)), Image.Resampling.LANCZOS)
        thumb = result.copy()
        thumb.thumbnail(THUMB_SIZE, Image.Resampling.LANCZOS)
        return thumb

    if variant_id == "D_resize_rgba_first":
        # Resize en RGBA primero, composite después
        if img.width > 600:
            wp = 600 / img.width
            img = img.resize((600, int(img.height * wp)), Image.Resampling.LANCZOS)
        thumb = img.copy()
        thumb.thumbnail(THUMB_SIZE, Image.Resampling.LANCZOS)
        bg = Image.new("RGBA", thumb.size, bg_color + (255,))
        composited = Image.alpha_composite(bg, thumb)
        return composited.convert("RGB")

    if variant_id == "E_premult_unpack":
        # Des-premultiplicar alpha antes del composite
        arr = np.array(img, dtype=np.float32)
        rgb   = arr[:, :, :3]
        alpha = arr[:, :, 3:4] / 255.0
        # Evitar división por cero en zonas totalmente transparentes
        safe_alpha = np.where(alpha > 0, alpha, 1.0)
        unpremult = np.clip(rgb / safe_alpha, 0, 255).astype(np.uint8)
        alpha_u8  = arr[:, :, 3].astype(np.uint8)
        unpremult_img = Image.fromarray(
            np.dstack([unpremult, alpha_u8[:, :, np.newaxis]]), "RGBA"
        )
        if unpremult_img.width > 600:
            wp = 600 / unpremult_img.width
            unpremult_img = unpremult_img.resize(
                (600, int(unpremult_img.height * wp)), Image.Resampling.LANCZOS
            )
        thumb = unpremult_img.copy()
        thumb.thumbnail(THUMB_SIZE, Image.Resampling.LANCZOS)
        bg = Image.new("RGBA", thumb.size, bg_color + (255,))
        composited = Image.alpha_composite(bg, thumb)
        return composited.convert("RGB")

    raise ValueError(f"Variante desconocida: {variant_id}")


def image_stats(img_rgba: Image.Image) -> dict:
    arr   = np.array(img_rgba)
    alpha = arr[:, :, 3]
    return {
        "size":        img_rgba.size,
        "mode":        img_rgba.mode,
        "transparent": int((alpha == 0).sum()),
        "opaque":      int((alpha == 255).sum()),
        "semitrans":   int(((alpha > 0) & (alpha < 255)).sum()),
        "premult_pixels": int(
            ((alpha == 0) & (arr[:, :, :3].sum(axis=2) > 0)).sum()
        ),
    }


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def load_from_s3(n: int) -> list[tuple[str, Image.Image]]:
    import boto3
    from dotenv import load_dotenv

    sys.path.insert(0, str(Path(__file__).parent.parent))
    load_dotenv()

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-2"),
    )
    bucket = os.getenv("AWS_BUCKET_NAME", "suplescrapper-images")

    print(f"Listando imágenes en S3 originals/chilesuplementos/ ...")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(
        Bucket=bucket, Prefix="assets/img/originals/chilesuplementos/"
    ):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    print(f"Total en S3: {len(keys)} archivos. Descargando hasta {n} RGBA ...")

    results = []
    for key in keys:
        if len(results) >= n:
            break
        buf = io.BytesIO()
        s3.download_fileobj(bucket, key, buf)
        buf.seek(0)
        try:
            img = Image.open(buf)
            img.load()
            if img.mode != "RGBA":
                continue   # solo nos interesan las problemáticas
            results.append((os.path.basename(key), img))
            print(f"  [{len(results):2d}/{n}] {os.path.basename(key)} {img.size}")
        except Exception as e:
            print(f"  SKIP {key}: {e}")

    return results


def load_from_dir(source_dir: str, n: int) -> list[tuple[str, Image.Image]]:
    results = []
    for fname in sorted(os.listdir(source_dir)):
        if len(results) >= n:
            break
        fpath = os.path.join(source_dir, fname)
        try:
            img = Image.open(fpath)
            img.load()
            if img.mode != "RGBA":
                continue
            results.append((fname, img))
            print(f"  [{len(results):2d}/{n}] {fname} {img.size}")
        except Exception:
            continue
    return results


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------
HTML_TEMPLATE = textwrap.dedent("""\
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Image Processing Debug — ChileSuplementos</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: system-ui, sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 16px; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 4px; color: #fff; }}
  .subtitle {{ color: #888; font-size: 0.85rem; margin-bottom: 20px; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 24px; }}
  .legend-item {{ background: #16213e; border: 1px solid #0f3460; border-radius: 6px; padding: 8px 12px; font-size: 0.78rem; }}
  .legend-item strong {{ color: #e94560; display: block; margin-bottom: 2px; }}
  .legend-item pre {{ color: #a0c4ff; white-space: pre-wrap; font-size: 0.75rem; font-family: inherit; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th {{ background: #16213e; color: #a0c4ff; font-size: 0.8rem; padding: 8px 10px; text-align: center; position: sticky; top: 0; z-index: 10; border: 1px solid #0f3460; }}
  th.filename {{ text-align: left; }}
  td {{ border: 1px solid #0f3460; padding: 6px; vertical-align: top; text-align: center; }}
  td.filename {{ text-align: left; font-size: 0.72rem; color: #888; vertical-align: middle; min-width: 80px; max-width: 120px; word-break: break-all; }}
  .stats {{ font-size: 0.68rem; color: #888; margin-top: 4px; line-height: 1.4; }}
  .stats .warn {{ color: #e94560; }}
  img.thumb {{ display: block; margin: 0 auto; max-width: {size}px; max-height: {size}px; width: auto; height: auto; border: 1px solid #333; background: repeating-conic-gradient(#3a3a4a 0% 25%, #2a2a3a 0% 50%) 0 0 / 12px 12px; }}
  .badge {{ display: inline-block; font-size: 0.65rem; padding: 1px 5px; border-radius: 3px; margin-bottom: 3px; }}
  .badge-rgba {{ background: #e94560; color: #fff; }}
  .badge-rgb  {{ background: #4caf50; color: #fff; }}
</style>
</head>
<body>
<h1>Image Processing Debug — ChileSuplementos</h1>
<p class="subtitle">{n_images} imágenes RGBA · {n_variants} variantes · thumbnail target {thumb_size}px</p>

<div class="legend">
{legend_html}
</div>

<table>
<thead>
<tr>
  <th class="filename">Archivo</th>
  {header_cells}
</tr>
</thead>
<tbody>
{rows_html}
</tbody>
</table>
</body>
</html>
""")


def build_html(
    images: list[tuple[str, Image.Image]],
    results: list[list[tuple[str, Image.Image]]],  # results[img_idx][variant_idx] = (rel_path, processed_img)
    out_dir: Path,
    thumb_display_size: int,
) -> str:
    # Legend
    legend_items = []
    for v in VARIANTS:
        desc_html = v["desc"].replace("\n", "<br>")
        legend_items.append(
            f'<div class="legend-item"><strong>{v["id"]} — {v["name"]}</strong>'
            f'<pre>{desc_html}</pre></div>'
        )
    legend_html = "\n".join(legend_items)

    # Header
    header_cells = "\n  ".join(
        f'<th>{v["id"]}<br><span style="font-weight:normal;color:#ccc">{v["name"]}</span></th>'
        for v in VARIANTS
    )

    # Rows
    rows = []
    for img_idx, (fname, img_rgba) in enumerate(images):
        stats = image_stats(img_rgba)
        warn_premult = f'<span class="warn">⚠ {stats["premult_pixels"]} premult px</span>' if stats["premult_pixels"] > 0 else ""
        fname_cell = (
            f'<td class="filename">'
            f'<span class="badge badge-rgba">RGBA</span><br>'
            f'{fname}<br>'
            f'<span class="stats">'
            f'{stats["size"][0]}×{stats["size"][1]}<br>'
            f'transp: {stats["transparent"]}<br>'
            f'semitrans: {stats["semitrans"]}<br>'
            f'{warn_premult}'
            f'</span>'
            f'</td>'
        )

        cells = [fname_cell]
        for v_idx, v in enumerate(VARIANTS):
            rel_path, proc_img = results[img_idx][v_idx]
            proc_stats = f'{proc_img.size[0]}×{proc_img.size[1]}'
            badge = f'<span class="badge badge-rgb">RGB</span>' if proc_img.mode == "RGB" else f'<span class="badge badge-rgba">RGBA</span>'
            cells.append(
                f'<td>'
                f'{badge}<br>'
                f'<img class="thumb" src="{rel_path}" loading="lazy">'
                f'<div class="stats">{proc_stats}</div>'
                f'</td>'
            )

        rows.append(f'<tr>{"".join(cells)}</tr>')

    return HTML_TEMPLATE.format(
        n_images=len(images),
        n_variants=len(VARIANTS),
        thumb_size=THUMB_SIZE[0],
        legend_html=legend_html,
        header_cells=header_cells,
        rows_html="\n".join(rows),
        size=thumb_display_size,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Compara variantes de procesamiento de imágenes RGBA de ChileSuplementos."
    )
    parser.add_argument("--n",          type=int,  default=10,  help="Nº de imágenes a procesar (default: 10)")
    parser.add_argument("--size",       type=int,  default=220, help="Tamaño display de thumbs en HTML px (default: 220)")
    parser.add_argument("--source-dir", type=str,  default=None, help="Directorio local en vez de S3")
    parser.add_argument("--out-dir",    type=str,  default=None, help="Carpeta de salida (default: img_debugging/output/)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    out_dir = Path(args.out_dir) if args.out_dir else script_dir / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    imgs_dir = out_dir / "imgs"
    imgs_dir.mkdir(exist_ok=True)

    # --- Cargar imágenes ---
    if args.source_dir:
        print(f"Cargando desde directorio local: {args.source_dir}")
        images = load_from_dir(args.source_dir, args.n)
    else:
        images = load_from_s3(args.n)

    if not images:
        print("No se encontraron imágenes RGBA. Abortando.")
        sys.exit(1)

    print(f"\nProcesando {len(images)} imágenes × {len(VARIANTS)} variantes ...")

    # --- Procesar variantes ---
    results = []  # results[img_idx][variant_idx] = (rel_path_str, processed_Image)
    for img_idx, (fname, img_rgba) in enumerate(images):
        name_root = Path(fname).stem
        row = []
        for v in VARIANTS:
            try:
                processed = apply_variant(img_rgba, v["id"], v["bg"])
            except Exception as e:
                print(f"  ERROR {fname} / {v['id']}: {e}")
                processed = Image.new("RGB", THUMB_SIZE, (80, 0, 0))

            out_fname = f"{name_root}_{v['id']}.png"
            out_path  = imgs_dir / out_fname
            processed.save(out_path, format="PNG")

            rel_path = f"imgs/{out_fname}"
            row.append((rel_path, processed))
            print(f"  [{img_idx+1:2d}/{len(images)}] {fname} → {v['id']} {processed.size}", end="\r")

        results.append(row)
        print()  # newline after each image

    # --- Generar HTML ---
    html = build_html(images, results, out_dir, args.size)
    html_path = out_dir / "index.html"
    html_path.write_text(html, encoding="utf-8")

    print(f"\nListo. Abre en el browser:")
    print(f"  {html_path.resolve()}")


if __name__ == "__main__":
    main()
