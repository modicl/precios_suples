# Scraper para la pagina web Suples

category_urls = {
            "Proteinas": [
                "https://www2.suples.cl/collections/proteina-whey",
                "https://www2.suples.cl/collections/proteina-isolate",
                "https://www2.suples.cl/collections/proteinas-hidrolizadas",
                "https://www2.suples.cl/collections/proteinas-caseinas",
                "https://www2.suples.cl/collections/proteinas-de-carne",
                "https://www2.suples.cl/collections/proteinas-veganas",
                "https://www2.suples.cl/collections/proteinas-liquidas"
            ],
            "Creatinas": [
                "https://www2.suples.cl/collections/creatinas",
            ],
            "Vitaminas": [
                "https://www2.suples.cl/collections/multivitaminicos",
                "https://www2.suples.cl/collections/vitamina-b",
                "https://www2.suples.cl/collections/vitamina-c",
                "https://www2.suples.cl/collections/vitamina-d",
                "https://www2.suples.cl/collections/vitamina-e",
                "https://www2.suples.cl/collections/magnesio",
                "https://www2.suples.cl/collections/calcio",
                "https://www2.suples.cl/collections/omega-y-acidos-grasos-1" # Ignorar el 1 para subcategoria
                "https://www2.suples.cl/collections/magnesio-y-minerales-1",# Ignorar el 1 para subcategoria
                "https://www2.suples.cl/collections/sistema-digestivo-y-probioticos",
                "https://www2.suples.cl/collections/colageno-y-articulaciones",
                "https://www2.suples.cl/collections/antimicrobianos-naturales-y-acido-caprilico",
                "https://www2.suples.cl/collections/equilibrante-natural-adaptogenos-y-bienestar-general",
                "https://www2.suples.cl/collections/aminoacidos-y-nutrientes-esenciales",
                "https://www2.suples.cl/collections/sistemas-nervioso-y-cognitivo",
                "https://www2.suples.cl/collections/bienestar-natural-y-salud-integral",
                "https://www2.suples.cl/collections/arginina",
                "https://www2.suples.cl/collections/antioxidantes",
                "https://www2.suples.cl/collections/colagenos-1",
                "https://www2.suples.cl/collections/hmb",
                "https://www2.suples.cl/collections/omega-3",
                "https://www2.suples.cl/collections/probioticos",
                "https://www2.suples.cl/collections/zma"
            ],
            "Pre Entrenos": [
                "https://www2.suples.cl/collections/pre-workout"
            ],
            "Ganadores de Peso": [
                "https://www2.suples.cl/collections/ganadores-de-masa"
            ],
            "Aminoacidos y BCAA": [
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/bcaa",
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/eaa",
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/hmb-y-zma",
                "https://www.supletech.cl/suplementos-alimenticios/aminoacidos/especificos"
            ],
            "Perdida de Grasa": [
                "https://www2.suples.cl/collections/cafeina",
                "https://www2.suples.cl/collections/quemadores-termogenicos",
                "https://www2.suples.cl/collections/quemadores-liquidos",
                "https://www2.suples.cl/collections/quemadores-naturales",
                "https://www2.suples.cl/collections/eliminadores-de-retencion",
                "https://www2.suples.cl/collections/quemadores-localizados",
                "https://www2.suples.cl/collections/cremas-reductoras"
            ],
            "Snacks y Comida": [
                "https://www2.suples.cl/collections/barritas-y-snacks-proteicas",
                "https://www2.suples.cl/collections/alimentos-outdoor"
            ]
        }
        
# Por implementar
selectors = {
    "product_grid": "#gallery-layout-container", 
    'product_card': '.vtex-product-summary-2-x-container', 
    'product_name': 'span.vtex-product-summary-2-x-productBrand',
    'brand': 'span.vtex-store-components-3-x-productBrandName', 
    'price': '.vtex-product-price-1-x-currencyContainer', 
    'link': 'a.vtex-product-summary-2-x-clearLink', 
    'rating': '.vtex-reviews-and-ratings-3-x-stars', 
    'active_discount': '', # Si hay dos precios, si hay una barrita,etc
    'next_button': '.vtex-search-result-3-x-buttonShowMore :where(button, a)' # Botón o Link "Mostrar más"
}