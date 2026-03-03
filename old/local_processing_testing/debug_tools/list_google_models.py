import os
import requests
import sys

# Load env from root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
# But loading env is manual here or we rely on .env file if python-dotenv installed?
# Or just read the .env file manually.

def get_api_key():
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))
    with open(env_path, 'r') as f:
        for line in f:
            if line.startswith("GOOGLE_API_KEY="):
                return line.strip().split("=", 1)[1]
    return None

def list_models():
    api_key = get_api_key()
    if not api_key:
        print("API Key not found in .env")
        return

    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            models = response.json().get('models', [])
            print(f"Found {len(models)} models:")
            for m in models:
                name = m.get('name') # usually "models/model-name"
                methods = m.get('supportedGenerationMethods', [])
                if 'generateContent' in methods:
                    print(f"  - {name} (Supports generateContent)")
                else:
                    print(f"  - {name} (NO generateContent)")
        else:
            print(f"Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    list_models()
