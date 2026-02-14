# Script para clasificar productos como veganos o para mujeres a datos de la base de datos
import os
import sys
import re
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
from rich.console import Console

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from tools.db_multiconnect import get_targets

console = Console()

# 1. Definición de Keywords (Regex)
KEYWORDS_VEGAN = [
    r'\bvegan', r'\bvegano', r'\bvegetariano', 
    r'\bplant\s*based', r'\bplant-based', 
    r'\bvegetal', r'\bsoya\b', r'\bsoy\b', 
    r'\bguisante', r'\barveja', r'\bpea\s*protein',
    r'\bvegana'
]

KEYWORDS_WOMEN = [
    r'\bwomen', r'\bwoman', r'\bmujer', 
    r'\bfemenin[oa]', r'\bfemale', 
    r'\bhers\b', r'\bella\b', r'\bpara\s*ellas\b',
    r'\bwomen\'s', r'\bwomens'
]

def classify_text(text_content):
    if not text_content:
        return False, False
    
    text_lower = text_content.lower()
    
    is_vegan = any(re.search(p, text_lower) for p in KEYWORDS_VEGAN)
    is_women = any(re.search(p, text_lower) for p in KEYWORDS_WOMEN)
    
    return is_vegan, is_women

def classify_products_db(engine, db_name):
    console.print(f"\n[bold]--- Classifying Products for: {db_name} ---[/bold]")
    
    # 1. Fetch products
    products = []
    try:
        with engine.connect() as conn:
            query = text("SELECT id_producto, nombre_producto, descripcion FROM productos")
            products = conn.execute(query).fetchall()
    except Exception as e:
        console.print(f"[red][{db_name}] Error fetching products: {e}[/red]")
        return
    
    console.print(f"[{db_name}] Analizando {len(products)} productos...")
    
    updates = []
    vegan_count = 0
    women_count = 0
    
    for p in products:
        # Concatenate name + desc
        name = p.nombre_producto if p.nombre_producto else ""
        desc = p.descripcion if p.descripcion else ""
        full_text = f"{name} {desc}"
        
        is_vegan, is_women = classify_text(full_text)
        
        if is_vegan: vegan_count += 1
        if is_women: women_count += 1
        
        updates.append({
            'id': p.id_producto,
            'is_vegan': is_vegan,
            'is_women': is_women
        })
        
    console.print(f"[{db_name}] Detectados: [green]Veganos: {vegan_count}[/green], [magenta]Mujer: {women_count}[/magenta]")
    
    if not updates:
        console.print(f"[{db_name}] No hay productos para actualizar.")
        return

    # 2. Bulk Update
    BATCH_SIZE = 1000
    total_updated = 0
    
    console.print(f"[bold blue][{db_name}] Ejecutando actualización en BD...[/bold blue]")
    
    try:
        with engine.begin() as conn:
            for i in range(0, len(updates), BATCH_SIZE):
                batch = updates[i:i+BATCH_SIZE]
                
                # Construct VALUES string: (id, vegan, women)
                values_str = ",".join(
                    f"({item['id']}, {str(item['is_vegan']).lower()}, {str(item['is_women']).lower()})" 
                    for item in batch
                )
                
                # Use a cleaner way to update from values list
                # Note: Postgres specific syntax is usually fine here given the project stack
                sql = f"""
                UPDATE productos AS p 
                SET is_vegan = v.is_vegan, 
                    is_women = v.is_women
                FROM (VALUES {values_str}) AS v(id, is_vegan, is_women) 
                WHERE p.id_producto = v.id;
                """
                
                conn.execute(text(sql))
                total_updated += len(batch)
                console.print(f"  -> [{db_name}] Lote procesado: {total_updated}/{len(updates)}")
        
        console.print(f"[bold green][{db_name}] Actualización completada exitosamente. Total: {total_updated}[/bold green]")
        
    except Exception as e:
        console.print(f"[bold red][{db_name}] Error actualizando BD: {e}[/bold red]")

def main():
    load_dotenv()
    
    targets = get_targets()
    if not targets:
        console.print("[red]Error: No database targets found.[/red]")
        return

    console.print(f"Found database targets: {[t['name'] for t in targets]}")

    for target in targets:
        db_name = target["name"]
        db_url = target["url"]
        
        try:
            print(f"\nConnecting to {db_name}...")
            engine = sa.create_engine(db_url)
            classify_products_db(engine, db_name)
        except Exception as e:
            console.print(f"[red]Error connecting/processing {db_name}: {e}[/red]")

if __name__ == "__main__":
    main()
