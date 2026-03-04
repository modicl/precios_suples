# Schema de Base de Datos

Definido en `bd/prisma/schema.prisma`. Motor: **PostgreSQL** (Neon en producción).

---

## Diagrama de Relaciones

```
categorias
  └── subcategorias (1:N, id_categoria)
       └── productos (N:1, id_subcategoria)

marcas
  └── productos (1:N, id_marca)

productos
  └── producto_tienda (1:N, id_producto)
       └── historia_precios (1:N, id_producto_tienda)

tiendas
  └── producto_tienda (1:N, id_tienda)

usuarios
  ├── favoritos (1:N) → productos
  └── alertas (1:N)  → productos
```

---

## Tablas Core

### `productos`
Catálogo maestro. Unique constraint: `(nombre_producto, id_marca, id_subcategoria)`.

| Campo | Tipo | Notas |
|-------|------|-------|
| `id_producto` | Int PK | |
| `nombre_producto` | VarChar(255) | Nombre normalizado (post Step 2) |
| `url_imagen` | VarChar(500)? | URL S3 versión resized |
| `url_thumb_imagen` | VarChar(500)? | URL S3 thumbnail |
| `descripcion` | String? | Descripción scrapeada |
| `descripcion_llm` | String? | Generada por GPT-4o-mini (Step 5) |
| `tags` | String[] | Tags por Step 6 |
| `id_marca` | Int? | FK → marcas |
| `id_subcategoria` | Int? | FK → subcategorias |
| `is_vegan` | Boolean | Default false |
| `is_women` | Boolean | Default false |

### `producto_tienda`
Junction table. Unique: `(id_producto, id_tienda)`. También unique implícito en `url_link` por tienda (constraint de negocio, no en schema).

| Campo | Tipo | Notas |
|-------|------|-------|
| `id_producto_tienda` | Int PK | |
| `id_producto` | Int | FK → productos |
| `id_tienda` | Int | FK → tiendas |
| `url_link` | String? | URL del producto en la tienda |
| `descripcion` | String? | |
| `is_active` | Boolean | false si no apareció en el último scrape |
| `fecha_ultima_vista` | DateTime? | Actualizado en cada run |

### `historia_precios`
Serie de tiempo de precios. Sin dedup — una fila por inserción.

```sql
INDEX: (id_producto_tienda, fecha_precio DESC, precio)
-- índice covering para queries de precio actual / historial
```

### `marcas` / `categorias` / `subcategorias`
Taxonomía. `subcategorias` tiene un fallback "Otros" por categoría que se usa en Step 3 cuando una subcategoría no existe en la BD.

---

## Constraints Relevantes

```sql
-- productos: no puede haber dos productos con mismo nombre, marca y subcategoría
UNIQUE (nombre_producto, id_marca, id_subcategoria)

-- producto_tienda: un producto aparece una sola vez por tienda
UNIQUE (id_producto, id_tienda)

-- subcategorias: subcategoría única dentro de su categoría
UNIQUE (nombre_subcategoria, id_categoria)
```

---

## Tablas de Usuarios (Frontend)

| Tabla | Descripción |
|-------|-------------|
| `usuarios` | Auth, email, región/comuna |
| `favoritos` | Productos guardados por usuario |
| `alertas` | Alerta de precio con umbral (`precio_umbral`) |
| `click_analytics` | Click por producto con timestamp |
| `search_analytics` | Keywords buscados con timestamp |

---

## Referencias

- [[arquitectura/Flujo de Datos]]
- [[etl/Step 3 - Insercion Bulk]]
