import os
from dotenv import load_dotenv

def get_targets():
    """
    Returns a list of database configurations to target.
    Each config is a dict with 'name' and 'url'.
    """
    load_dotenv()
    
    targets = []
    
    # 1. Local Database
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    
    # Fallback/Safety for local
    if all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        # Check standard port if not set
        port = DB_PORT if DB_PORT else "5432"
        local_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{port}/{DB_NAME}"
        targets.append({"name": "Local", "url": local_url})
    else:
        # Try generic DATABASE_URL if atomic vars fail
        local_url_generic = os.getenv("DATABASE_URL")
        if local_url_generic:
            targets.append({"name": "Local", "url": local_url_generic})
            
    # 2. Production Database
    # Expected format: postgresql://user:pass@host:port/dbname?options
    prod_url = os.getenv("DB_HOST_PROD")
    if prod_url:
        targets.append({"name": "Production", "url": prod_url})
        
    return targets
