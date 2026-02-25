import os
import argparse
import sqlalchemy as sa
from sqlalchemy.sql import text
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "suplementos")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "password")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

console = Console()

def get_db_connection():
    return sa.create_engine(DATABASE_URL)

def check_duplicates(conn, table_name, group_cols, id_col="id"):
    """
    Checks for exact duplicates in a table based on grouping columns.
    Returns (total_rows, unique_rows, duplicate_rows).
    """
    console.print(f"[bold blue]Analizando tabla: {table_name}...[/bold blue]")
    
    # 1. Count Total Rows
    total_query = text(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = conn.execute(total_query).scalar()
    
    # 2. Count Unique Rows (based on group_cols)
    # We use COUNT(DISTINCT ...) but specifically on the combination.
    # Postgres supports COUNT(DISTINCT (col1, col2...))
    cols_str = ", ".join(group_cols)
    
    # Alternative: Count rows in the grouped subquery
    unique_query = text(f"""
        SELECT COUNT(*) FROM (
            SELECT 1 
            FROM {table_name} 
            GROUP BY {cols_str}
        ) as sub
    """)
    unique_rows = conn.execute(unique_query).scalar()
    
    duplicate_rows = total_rows - unique_rows
    
    return total_rows, unique_rows, duplicate_rows

def clean_duplicates(conn, table_name, group_cols, id_col, execute=False):
    total, unique, duplicates = check_duplicates(conn, table_name, group_cols, id_col)
    
    table = Table(title=f"Reporte de Duplicados: {table_name}")
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", style="magenta")
    
    table.add_row("Total Filas", str(total))
    table.add_row("Filas Únicas (Combinación)", str(unique))
    table.add_row("Duplicados Exactos", f"[red]{duplicates}[/red]" if duplicates > 0 else "[green]0[/green]")
    
    console.print(table)
    
    if duplicates == 0:
        console.print(f"[green]No se encontraron duplicados en {table_name}.[/green]")
        return
        
    if not execute:
        console.print(f"[yellow]Modo Dry-Run: Se encontrarían y eliminarían {duplicates} filas.[/yellow]")
        console.print("Usa --execute para aplicar los cambios.")
        return

    # Execute Deletion
    console.print(f"[bold red]Eliminando {duplicates} filas duplicadas en {table_name}...[/bold red]")
    
    cols_str = ", ".join(group_cols)
    
    # Delete Logic: Keep the row with MIN(id)
    delete_query = text(f"""
        DELETE FROM {table_name}
        WHERE {id_col} NOT IN (
            SELECT MIN({id_col})
            FROM {table_name}
            GROUP BY {cols_str}
        )
    """)
    
    result = conn.execute(delete_query)
    console.print(f"[green]Eliminadas {result.rowcount} filas.[/green]")
    
    # Verify
    new_total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    console.print(f"Nuevo total de filas: {new_total}")

def main():
    parser = argparse.ArgumentParser(description="Limpiar duplicados exactos en la base de datos.")
    parser.add_argument("--execute", action="store_true", help="Ejecutar eliminación (por defecto es solo reporte).")
    args = parser.parse_args()
    
    engine = get_db_connection()
    
    try:
        with engine.connect() as conn:
            # We need autocommit for VACUUM if we were to run it, but usually standard delete is transactional.
            # However, VACUUM cannot run inside a transaction block.
            # We'll handle VACUUM separately if needed, or just print message.
            
            # Start transaction for DELETEs
            trans = conn.begin()
            try:
                # 1. Historia Precios
                # Columns defining uniqueness: id_producto_tienda, fecha_precio, precio
                clean_duplicates(
                    conn, 
                    "historia_precios", 
                    ["id_producto_tienda", "fecha_precio", "precio"], 
                    "id_historia_precio",
                    execute=args.execute
                )
                
                # Check other tables if requested, but plan focuses on history.
                # Let's check 'producto_tienda' just in case unique constraint was missing/violated
                # clean_duplicates(conn, "producto_tienda", ["id_producto", "id_tienda"], "id_producto_tienda", execute=args.execute)
                
                if args.execute:
                    trans.commit()
                    console.print("[bold green]Cambios aplicados exitosamente.[/bold green]")
                else:
                    trans.rollback()
                    console.print("[yellow]Dry-Run finalizado (Rollback).[/yellow]")
                    
            except Exception as e:
                trans.rollback()
                console.print(f"[bold red]Error durante la ejecución: {e}[/bold red]")
                raise e
        
        # Post-execution: Suggest VACUUM
        if args.execute:
            console.print("\n[bold blue]Sugerencia:[/bold blue] Para recuperar el espacio en disco, ejecuta manualmente:")
            console.print(f"[white]VACUUM FULL historia_precios;[/white]")
            console.print("(Esto requiere conexión fuera de transacción y puede bloquear la tabla momentáneamente)")

    except Exception as e:
        console.print(f"[red]Error de conexión: {e}[/red]")

if __name__ == "__main__":
    main()
