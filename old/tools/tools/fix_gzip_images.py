import os
import gzip
import shutil

def is_gzip(filepath):
    """Verifica si el archivo tiene los bytes mágicos de GZIP."""
    try:
        with open(filepath, 'rb') as f:
            return f.read(2) == b'\x1f\x8b'
    except Exception:
        return False

def decompress_file(filepath):
    """Descomprime un archivo GZIP in-place."""
    print(f"Reparando (Descomprimiendo) archivo: {filepath}")
    temp_gz_path = filepath + ".gz"
    
    try:
        # Renombrar a .gz
        os.rename(filepath, temp_gz_path)
        
        with gzip.open(temp_gz_path, 'rb') as f_in:
            with open(filepath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Eliminar temporal
        os.remove(temp_gz_path)
        print("  -> Éxito")
        
    except Exception as e:
        print(f"  -> Error al descomprimir {filepath}: {e}")
        # Restaurar si falló
        if os.path.exists(temp_gz_path):
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass
            os.rename(temp_gz_path, filepath)

def scan_and_fix(root_dir="assets/img"):
    """Recorre el directorio buscando y reparando archivos GZIP."""
    if not os.path.exists(root_dir):
        print(f"Directorio {root_dir} no existe.")
        return

    print(f"Escaneando {root_dir}...")
    count = 0
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            path = os.path.join(root, file)
            # Ignoramos archivos que ya son .gz explícitamente (aunque el script busca magic bytes)
            if file.endswith('.gz'):
                continue
                
            if is_gzip(path):
                decompress_file(path)
                count += 1
    
    print(f"Proceso finalizado. Total archivos reparados: {count}")

if __name__ == "__main__":
    scan_and_fix()
