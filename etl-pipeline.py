import os
import pandas as pd
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

# --- CONFIGURAÇÃO DE AMBIENTE ---
# Carrega as chaves do .env ignorado pelo git
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("Chaves do Supabase não encontradas no .env!")

# Inicializa o cliente do Supabase
supabase: Client = create_client(url, key)

# --- DEFINIÇÃO DE CAMINHOS ABSOLUTOS SEGUROS ---
# Caminhos relativos ao local deste script
BASE_DIR = Path(__file__).resolve().parent
# Onde está a planilha bruta
CAMINHO_PLANILHA = BASE_DIR / "planilha.xlsx"
# Onde estão as imagens locais (baseado na sua estrutura antiga)
PASTA_IMAGENS_LOCAL = BASE_DIR / "img" 

NOME_BUCKET = "catalog-images"

def executar_etl():
    print("Iniciando Pipeline de Ingestão de Dados (ETL)...")
    
    if not CAMINHO_PLANILHA.exists():
        print(f"ERRO: Planilha não encontrada em {CAMINHO_PLANILHA}")
        return

    # --- 1. EXTRAÇÃO (Extract) ---
    print("\n1. Extraindo dados do ERP...")
    # skiprows=1 assume que o cabeçalho real começa na segunda linha, como no seu código antigo
    df = pd.read_excel(CAMINHO_PLANILHA, skiprows=1) 
    
    # Remove linhas vazias baseadas nas duas primeiras colunas (ID e Nome)
    df.dropna(subset=[df.columns[0], df.columns[1]], inplace=True)
    
    # --- 2. TRANSFORMAÇÃO (Transform) ---
    print("2. Tratando e limpando dados...")
    
    produtos_processados = []
    
    for _, row in df.iterrows():
        try:
            # Pega o ID bruto e limpa qualquer vestígio de .0
            id_bruto = str(row.iloc[0]).replace('.0', '').strip()
            
            produto = {
                "codigo_erp": id_bruto,
                "nome": str(row.iloc[1]).strip(),
                "categoria": str(row.iloc[2]).strip(),
                # Garante que o preço seja um float numérico
                "preco": float(row.iloc[3]), 
                "url_imagem": None # Será preenchido no próximo passo
            }
            produtos_processados.append(produto)
        except Exception as e:
            # Ignora linhas mal formatadas (ex: cabeçalhos repetidos ou rodapé)
            continue
            
    print(f"Total de produtos após limpeza: {len(produtos_processados)}")

    # --- 3. CARGA NO BUCKET & BANCO (Load) ---
    print("\n3. Iniciando Carga no Storage e Banco de Dados...")
    
    for produto in produtos_processados:
        codigo = produto["codigo_erp"]
        nome_arquivo = f"{codigo}.jpg"
        caminho_foto_local = PASTA_IMAGENS_LOCAL / nome_arquivo
        
        # 3.1 Upload da Imagem para o Bucket (Se existir localmente)
        url_publica = None
        if caminho_foto_local.exists():
            print(f"[{codigo}] Fazendo upload da imagem...")
            try:
                # Faz o upload (se a imagem já existir no bucket com esse nome, 
                # a configuração padrão do Supabase falha. O ideal no futuro é verificar antes ou forçar overwrite)
                with open(caminho_foto_local, "rb") as f:
                    supabase.storage.from_(NOME_BUCKET).upload(nome_arquivo, f)
                    
                # Captura a URL pública
                url_publica = supabase.storage.from_(NOME_BUCKET).get_public_url(nome_arquivo)
                
            except Exception as e:
                print(f"[{codigo}] Aviso de Storage: {e} (Talvez a imagem já exista no bucket?)")
                # Se já existir, podemos apenas pegar a URL
                url_publica = supabase.storage.from_(NOME_BUCKET).get_public_url(nome_arquivo)
        else:
             print(f"[{codigo}] AVISO: Imagem {nome_arquivo} não encontrada no disco local.")

        produto["url_imagem"] = url_publica

        # 3.2 Inserção/Atualização (Upsert) na Tabela do Supabase
        # Exige que 'codigo_erp' seja uma coluna UNIQUE na tabela do banco
        try:
            print(f"[{codigo}] Sincronizando dados na tabela 'produtos'...")
            response = supabase.table("produtos").upsert(produto, on_conflict="codigo_erp").execute()
        except Exception as e:
             print(f"[{codigo}] ERRO DE BANCO: Falha ao inserir {produto['nome']} - {e}")

    print("\nPipeline Finalizado!")

if __name__ == "__main__":
    executar_etl()