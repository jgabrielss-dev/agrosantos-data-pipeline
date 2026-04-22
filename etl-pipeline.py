import os
import pandas as pd
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import time
import glob
import shutil
from icrawler.builtin import BingImageCrawler

# --- CONFIGURAÇÃO DE AMBIENTE ---
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("Chaves do Supabase não encontradas no .env!")

supabase: Client = create_client(url, key)

# --- CONFIGURAÇÕES E CAMINHOS ---
BASE_DIR = Path(__file__).resolve().parent
CAMINHO_PLANILHA = input("Arraste a planilha ou digite o caminho completo: ").strip()

# Limpeza agressiva para lidar com o drag-and-drop do PowerShell
CAMINHO_PLANILHA = CAMINHO_PLANILHA.removeprefix("& ").strip() # Remove o operador do Windows
CAMINHO_PLANILHA = CAMINHO_PLANILHA.strip("'").strip('"')      # Remove as aspas residuais

NOME_BUCKET = "catalog-images"

# --- REGRAS DE NEGÓCIO HARDCODED ---
IDS_BLOQUEADOS = ['1560', '576', '2435', '749', '942', '2613', '2803', '2899', '2900', '2902', '3005', '3037', '309', '5', '1470', '2733', '958', '588', '6', '702', '2901', '2635', '2709', '2760', '2537', '2542', '2546', '2584', '2497', '1895', '1963', '2202', '2225', '2303', '1916', '1965', '1935', '1951', '1956', '1658', '1771', '1695', '1660', '1451', '1523', '1059', '1294', '1032', '2592', '1958', '1959', '2809', '2812', '2813', '2073', '2419', '2466', '2763', '2769', '2778', '2801', '2496', '2572', '2690', '2147', '2159', '2160', '2151', '2231', '2260', '2278', '1960', '1962', '1966', '1974', '1984', '1985', '1986', '1988', '2021', '2022', '2023', '1731', '2370', '2864', '2079', '2344', '2919', '2428', '2429', '2734', '2264', '2263', '1659', '927', '3292', '1486', '1487', '1485', '2991', '1489', '1488', '1493', '1492', '1798', '1500', '1503', '1501', '1484']
CATEGORIAS_BLOQUEADAS = ['INSUMO', 'SELARIA']
TERMOS_BLOQUEADOS = [t for t in ['CINTO', 'INTERRUPT', 'TOMADA', 'PADRON', 'FLAMBADOR', 'CHAMAS', 'VASSOURA', 'FACHOLI', 'PURUCA', 'VASSOURAO', 'INSUMO', 'COLEIRA', 'MULTISHOW', 'GLYPHOTAL', 'ROUNDUP', 'ATRAZINA', 'GLIFOSATO', 'SIMPARIC', 'TECH MASTER', 'CRIADORES', 'CANTONINHO', 'ALCON CLUB', 'DIMY', 'GRANEL', 'SAAD', 'FINOTRATO', 'CHURU', 'cocho', 'COCHO', 'UNICOCHO'] if t]

def executar_etl():
    print("Iniciando Pipeline de Ingestão de Dados (ETL)...")
    
    caminho_obj = Path(CAMINHO_PLANILHA)
    if not caminho_obj.exists():
        print(f"ERRO: Planilha não encontrada em {CAMINHO_PLANILHA}")
        return

    # --- 1. EXTRAÇÃO ---
    print("\n[1/3] Lendo o ERP...")
    planilha = pd.read_excel(caminho_obj, skiprows=1) 
    
    col_id = planilha.columns[0]
    col_nome = planilha.columns[1]
    col_cat = planilha.columns[2]
    
    planilha.dropna(subset=[col_id, col_nome], inplace=True)

    # --- 2. TRANSFORMAÇÃO VETORIZADA (Limpeza antes do loop) ---
    print("[2/3] Filtrando lixo e aplicando regras de negócio...")
    
    # Formata a coluna ID para string pura
    planilha[col_id] = planilha[col_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Aplica os filtros de bloqueio
    planilha = planilha[~planilha[col_id].isin(IDS_BLOQUEADOS)]
    planilha = planilha[~planilha[col_cat].astype(str).str.strip().str.upper().isin(CATEGORIAS_BLOQUEADAS)]
    
    padrao_termos = '|'.join(TERMOS_BLOQUEADOS)
    planilha = planilha[~planilha[col_nome].astype(str).str.upper().str.contains(padrao_termos, na=False, regex=True)]

    print(f"Total de produtos após filtros: {len(planilha)}")

    # --- 3. EXECUÇÃO (Loop, Scraping e Upsert) ---
    print("\n[3/3] Iniciando sincronização com Supabase (Banco e Bucket)...")
    
    for _, row in planilha.iterrows():
        id_prod = str(row.iloc[0]).replace('.0', '').strip()
        nome_prod = str(row.iloc[1]).strip()
        categoria_prod = str(row.iloc[2]).strip()
        
        try:
            preco_prod = round(float(row.iloc[3]), 2)
        except ValueError:
            print(f"[{id_prod}] Ignorado: Preço inválido.")
            continue

        url_imagem = f"{url}/storage/v1/object/public/{NOME_BUCKET}/{id_prod}.jpg"
        nome_arquivo = f"{id_prod}.jpg"

        # TENTA LISTAR O ARQUIVO NO SUPABASE PARA VER SE ELE JÁ EXISTE
        arquivos_existentes = supabase.storage.from_(NOME_BUCKET).list(path="", options={"search": nome_arquivo})
        imagem_ja_no_bucket = any(arq['name'] == nome_arquivo for arq in arquivos_existentes)

        if not imagem_ja_no_bucket:
            print(f"[{id_prod}] Sem foto. Raspando Bing: '{nome_prod}'...")
            pasta_temp = BASE_DIR / f"temp_{id_prod}"
            os.makedirs(pasta_temp, exist_ok=True)
            
            try:
                crawler = BingImageCrawler(storage={'root_dir': str(pasta_temp)}, log_level=50)
                # Filtra para evitar fotos quebradas
                crawler.crawl(keyword=nome_prod, max_num=1, filters={'type': 'photo'})
                
                arquivos_baixados = glob.glob(f"{pasta_temp}/*")
                if arquivos_baixados:
                    caminho_imagem_baixada = arquivos_baixados[0]
                    with open(caminho_imagem_baixada, "rb") as f:
                        # Upload para o Supabase
                        supabase.storage.from_(NOME_BUCKET).upload(nome_arquivo, f)
                    print(f"[{id_prod}] Upload de imagem concluído.")
                else:
                    print(f"[{id_prod}] Bing não encontrou imagens. Seguindo sem foto.")
                    url_imagem = None # Define nulo se não achou, respeitando o contrato Next.js
            
            except Exception as e:
                print(f"[{id_prod}] Falha no scraping/upload: {e}")
                url_imagem = None
            
            finally:
                if os.path.exists(pasta_temp):
                    shutil.rmtree(pasta_temp)
                time.sleep(1) # Delay de respeito à rede
        else:
            print(f"[{id_prod}] Imagem já no bucket. Atualizando apenas texto...")

        # UPSERT NO BANCO DE DADOS (Cria ou Atualiza)
        try:
            dados_insercao = {
                "id":        id_prod,
                "nome":      nome_prod,
                "preco":     preco_prod,
                "categoria": categoria_prod,
                "url_imagem": url_imagem
            }
            # O .execute() é inegociável na API Python do Supabase
            supabase.table("produtos").upsert(dados_insercao).execute()
        except Exception as e:
            print(f"[{id_prod}] ERRO DE BANCO DE DADOS: {e}")

    print("\nPipeline Finalizado! O Supabase está sincronizado com o ERP.")

if __name__ == "__main__":
    executar_etl()