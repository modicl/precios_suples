"""
health_check_config.py
======================
Configuración centralizada para el DOM Health Check.
Cada entrada define la URL de prueba y los selectores CSS críticos
que deben encontrar elementos para que el scraper sea considerado funcional.

Usar: check_dom_health.py (en la raíz del proyecto)

Columnas de cada store:
    name          → nombre display (debe coincidir con site_name del scraper)
    test_url      → URL de una categoría con productos garantizados
    selectors     → dict CSS selector → descripción
                    (product_card es OBLIGATORIO; es el indicador principal de salud)
    min_products  → mínimo de product_cards esperados para considerar OK
    note          → (opcional) descripción del mecanismo de extracción
"""

STORES = [
    {
        "name": "ChileSuplementos",
        "test_url": "https://www.chilesuplementos.cl/categoria/productos/tipo-de-proteina/whey-protein/",
        "selectors": {
            "product_card": ".archive-products .porto-tb-item",
            "name":         ".post-title",
            "price":        ".price",
            "link":         ".post-title a",
        },
        "min_products": 5,
    },
    {
        "name": "AllNutrition",
        "test_url": "https://allnutrition.cl/collections/whey-protein",
        "selectors": {
            "product_card": ".c-card-product",
            "name":         ".c-card-producto__title h6",
            "price":        ".c-card-product__price",
            "link":         "a.link--not-decoration",
        },
        "min_products": 5,
    },
    {
        "name": "BYON",
        "test_url": "https://www.byon.cl/collections/proteina-de-suero-de-leche",
        "selectors": {
            "product_card": ".card-wrapper",
            "name":         ".card__heading .full-unstyled-link",
            "price":        ".price-item--regular",
            "link":         ".card__heading .full-unstyled-link",
        },
        "min_products": 3,
    },
    {
        "name": "SportNutriShop",
        "test_url": "https://www.sportnutrishop.cl/collections/proteinas-y-ganadores",
        "selectors": {
            "product_card": ".card-wrapper",
            "name":         ".card__heading a",
            "price":        ".price-item--sale, .price-item--regular",
            "link":         ".card__heading a",
        },
        "min_products": 5,
    },
    {
        "name": "Suples.cl",
        "test_url": "https://www2.suples.cl/collections/proteina-whey",
        "selectors": {
            "product_card": ".product-item",
            "name":         ".product-item__title",
            "price":        ".price",
            "link":         "a.product-item__title",
        },
        "min_products": 5,
    },
    {
        "name": "Farmacia Knopp",
        "test_url": "https://www.farmaciasknop.com/types/proteinas",
        "selectors": {
            "product_card": "div.product-item",
            "name":         "div.product-name a.product-title-link",
            "price":        "span.bootic-price",
            "link":         "a.product-link",
        },
        "min_products": 3,
    },
    {
        "name": "Cruz Verde",
        "test_url": "https://www.cruzverde.cl/vitaminas-y-suplementos/nutricion-deportiva/proteinas/",
        "selectors": {
            "product_card": "ml-new-card-product",
            "name":         "h2 a.new-ellipsis",
            "link":         "at-image a",
        },
        "min_products": 3,
    },
    {
        "name": "Decathlon",
        "test_url": "https://www.decathlon.cl/5270-proteinas",
        "selectors": {
            "product_card": ".product-miniature",
        },
        "min_products": 3,
        "note": "Extracción vía JSON embebido en página. product_card es indicativo.",
    },
    {
        "name": "Dr Simi",
        "test_url": "https://www.drsimi.cl/pronutrition",
        "selectors": {
            "product_card": ".vtex-product-summary-2-x-container",
            "name":         ".vtex-product-summary-2-x-brandName",
            "price":        ".vtex-product-price-1-x-sellingPriceValue--summary",
            "link":         "a.vtex-product-summary-2-x-clearLink",
        },
        "min_products": 3,
    },
    {
        "name": "KoteSport",
        "test_url": "https://kotesport.cl/categoria-producto/proteinas/whey/",
        "selectors": {
            "product_card": ".product-grid-item:not(.wd-hover-small)",
            "name":         ".wd-entities-title a",
            "link":         ".wd-entities-title a",
        },
        "min_products": 3,
    },
    {
        "name": "MuscleFactory",
        "test_url": "https://www.musclefactory.cl/proteinas",
        "selectors": {
            "product_card": ".product-block",
            "name":         ".product-block__name",
            "price":        ".product-block__price",
            "link":         ".product-block__anchor",
        },
        "min_products": 3,
    },
    {
        "name": "OneNutrition",
        "test_url": "https://onenutrition.cl/tienda/proteinas",
        "selectors": {
            "product_card": "#js-product-list .product-miniature",
            "name":         ".product-title a",
            "price":        ".price",
            "link":         ".product-title a",
        },
        "min_products": 3,
    },
    {
        "name": "Strongest",
        "test_url": "https://www.strongest.cl/collection/proteinas",
        "selectors": {
            "product_card": ".bs-collection__product",
            "name":         ".bs-collection__product-title",
            "price":        ".bs-collection__product-final-price",
            "link":         "a.bs-collection__product-info",
        },
        "min_products": 3,
    },
    {
        "name": "SupleStore",
        "test_url": "https://www.suplestore.cl/collection/proteinas",
        "selectors": {
            "product_card": ".bs-product",
            "name":         ".bs-product-info h6",
            "price":        ".bs-product-final-price",
            "link":         ".bs-product-info a",
        },
        "min_products": 3,
    },
    {
        "name": "SupleTech",
        "test_url": "https://www.supletech.cl/suplementos-alimenticios/proteinas/whey-protein/concentradas",
        "selectors": {
            "product_card": ".vtex-product-summary-2-x-container",
            "name":         "span.vtex-product-summary-2-x-productBrand",
            "price":        ".vtex-product-price-1-x-currencyContainer",
            "link":         "a.vtex-product-summary-2-x-clearLink",
        },
        "min_products": 3,
    },
    {
        "name": "SuplementosBullChile",
        "test_url": "https://www.suplementosbullchile.cl/proteinas",
        "selectors": {
            "product_card": ".product-block",
            "name":         ".product-block__name",
            "price":        ".product-block__price",
            "link":         ".product-block__anchor",
        },
        "min_products": 3,
    },
    {
        "name": "SuplementosMayoristas",
        "test_url": "https://www.suplementosmayoristas.cl/proteinas/whey-protein",
        "selectors": {
            "product_card": "section.vtex-product-summary-2-x-container",
            "name":         ".vtex-product-summary-2-x-productBrand",
            "price":        ".vtex-product-price-1-x-sellingPriceValue",
            "link":         "a.vtex-product-summary-2-x-clearLink",
        },
        "min_products": 3,
    },
    {
        "name": "Wild Foods",
        "test_url": "https://thewildfoods.com/collections/whey-protein",
        "selectors": {
            "product_card": ".product-item",
            "name":         ".product-item__title",
            "price":        ".product-price--original, .price-item--sale, .price-item--regular",
            "link":         ".product-item__title",
        },
        "min_products": 3,
    },
    {
        "name": "Winkler Nutrition",
        "test_url": "https://winklernutrition.cl/categoria-producto/proteinas-wk/",
        "selectors": {
            "product_card": "article.product",
            "name":         ".woocommerce-loop-product__title",
            "link":         "a.entry-link-mask",
        },
        "min_products": 3,
    },
    {
        "name": "FitMarketChile",
        "test_url": "https://fitmarketchile.cl/categoria-producto/proteinas",
        "selectors": {
            "product_card": ".product-grid-item",
            "name":         ".wd-entities-title a",
            "price":        ".price",
            "link":         ".product-image-link",
        },
        "min_products": 3,
    },
]
