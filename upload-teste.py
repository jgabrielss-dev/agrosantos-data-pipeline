import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Teste de upload simples
with open("foto_teste.jpg", "rb") as f:
    supabase.storage.from_("catalog-images").upload("teste.jpg", f)