import sys
import os
import sqlalchemy as sa
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.db_multiconnect import get_targets

def test_connectivity():
    targets = get_targets()
    print(f"Found {len(targets)} database targets configured.\n")
    
    success_count = 0
    
    for target in targets:
        name = target['name']
        url = target['url']
        
        # Mask password for printing
        safe_url = url
        if "@" in url:
            try:
                # Basic masking logic
                prefix, suffix = url.split("@", 1)
                # handle schema://user:pass
                if ":" in prefix:
                     schema_user, _ = prefix.rsplit(":", 1)
                     safe_url = f"{schema_user}:*****@{suffix}"
            except:
                pass
                
        print(f"Testing Connection to [{name}]...")
        # print(f"  URL: {safe_url}") 
        
        try:
            engine = sa.create_engine(url)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                if result == 1:
                    print(f"  [OK] Connection Successful! Response: {result}")
                    success_count += 1
                else:
                    print(f"  [FAIL] Query executed but returned unexpected result: {result}")
        except Exception as e:
            print(f"  [ERROR] Connection Failed: {e}")
            
        print("-" * 30)
        
    if success_count == len(targets) and len(targets) > 0:
        print("\nAll targets verifiable!")
    else:
        print(f"\nVerification incomplete. ({success_count}/{len(targets)} success)")

if __name__ == "__main__":
    test_connectivity()
