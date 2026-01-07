-- Creación de tablas
CREATE TABLE tiendas (
  id_tienda SERIAL PRIMARY KEY,
  nombre_tienda VARCHAR(255) NOT NULL,
  url_tienda VARCHAR(500)
);

CREATE TABLE marcas (
  id_marca SERIAL PRIMARY KEY,
  nombre_marca VARCHAR(255) NOT NULL
);

CREATE TABLE categorias (
  id_categoria SERIAL PRIMARY KEY,
  nombre_categoria VARCHAR(255) NOT NULL
);

CREATE TABLE subcategorias (
  id_subcategoria SERIAL PRIMARY KEY,
  nombre_subcategoria VARCHAR(255) NOT NULL,
  id_categoria INTEGER NOT NULL
);

CREATE TABLE productos (
  id_producto SERIAL PRIMARY KEY,
  nombre_producto VARCHAR(255) NOT NULL,
  url_imagen VARCHAR(500),
  url_thumb_imagen VARCHAR(500),
  descripcion TEXT,
  id_marca INTEGER,
  id_subcategoria INTEGER
);

CREATE TABLE producto_tienda (
  id_producto_tienda SERIAL PRIMARY KEY,
  id_producto INTEGER NOT NULL,
  id_tienda INTEGER NOT NULL
);

CREATE TABLE historia_precios (
  id_historia_precio SERIAL PRIMARY KEY,
  id_producto_tienda INTEGER NOT NULL,
  precio INTEGER NOT NULL,
  fecha_precio DATE DEFAULT CURRENT_DATE
);

-- Relaciones (Foreign Keys)
ALTER TABLE subcategorias ADD CONSTRAINT fk_categoria 
    FOREIGN KEY (id_categoria) REFERENCES categorias (id_categoria);

ALTER TABLE productos ADD CONSTRAINT fk_subcategoria 
    FOREIGN KEY (id_subcategoria) REFERENCES subcategorias (id_subcategoria);

ALTER TABLE productos ADD CONSTRAINT fk_marca 
    FOREIGN KEY (id_marca) REFERENCES marcas (id_marca);

ALTER TABLE producto_tienda ADD CONSTRAINT fk_producto 
    FOREIGN KEY (id_producto) REFERENCES productos (id_producto);

ALTER TABLE producto_tienda ADD CONSTRAINT fk_tienda 
    FOREIGN KEY (id_tienda) REFERENCES tiendas (id_tienda);

ALTER TABLE historia_precios ADD CONSTRAINT fk_producto_tienda 
    FOREIGN KEY (id_producto_tienda) REFERENCES producto_tienda (id_producto_tienda);
