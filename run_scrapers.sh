#!/bin/bash

# Activar venv si es necesario (opcional, asumiendo que ya se corre dentro de un entorno activado en git bash)
# source venv/Scripts/activate

echo "Iniciando scrapers en paralelo..."

# Ejecutar cada scraper en segundo plano
python AllNutritionScraper.py &
python ChileSuplementosScraper.py &
python OneNutritionScraper.py &
python SupleStoreScraper.py &
python SupleTechScraper.py &
python SuplesScraper.py &
python StrongestScraper.py &

# Esperar a que todos terminen
wait

echo "Todos los scrapers han finalizado."
