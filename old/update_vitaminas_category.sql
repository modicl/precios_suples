DO $$
DECLARE
    id_vitaminas INT;
    id_nueva INT;
BEGIN
    -- 1. Buscar ID de la categoría actual 'Vitaminas'
    SELECT id_categoria INTO id_vitaminas FROM categorias WHERE nombre_categoria = 'Vitaminas';

    -- 2. Buscar ID de la categoría destino 'Vitaminas y Minerales' (por si ya existe)
    SELECT id_categoria INTO id_nueva FROM categorias WHERE nombre_categoria = 'Vitaminas y Minerales';

    -- Verificar si encontramos 'Vitaminas'
    IF id_vitaminas IS NOT NULL THEN
        
        IF id_nueva IS NOT NULL THEN
            -- CASO A: Ambas categorías existen.
            -- Estrategia: Mover todas las subcategorías de 'Vitaminas' a 'Vitaminas y Minerales' y borrar la antigua.
            RAISE NOTICE 'Se encontraron ambas categorías (Vitaminas: %, Vitaminas y Minerales: %). Fusionando...', id_vitaminas, id_nueva;
            
            -- Mover subcategorías
            UPDATE subcategorias 
            SET id_categoria = id_nueva 
            WHERE id_categoria = id_vitaminas;
            
            -- Eliminar la categoría vacía 'Vitaminas'
            DELETE FROM categorias WHERE id_categoria = id_vitaminas;
            
            RAISE NOTICE 'Fusión completada. Categoría Vitaminas eliminada.';
            
        ELSE
            -- CASO B: Solo existe 'Vitaminas'.
            -- Estrategia: Simplemente renombrar la categoría.
            RAISE NOTICE 'Solo existe la categoría Vitaminas (ID: %). Renombrando...', id_vitaminas;
            
            UPDATE categorias 
            SET nombre_categoria = 'Vitaminas y Minerales' 
            WHERE id_categoria = id_vitaminas;
            
            RAISE NOTICE 'Renombre completado exitosamente.';
        END IF;

    ELSE
        RAISE NOTICE 'No se encontró la categoría "Vitaminas" en la base de datos. No se realizaron cambios.';
    END IF;
END $$;
