import streamlit as st
import pandas as pd
import os
import glob
import shutil
import tempfile
import time
from supabase import create_client, Client
from icrawler.builtin import BingImageCrawler

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Motor ETL - Agrosantos", page_icon="⚙️", layout="centered")
st.title("⚙️ Motor de Sincronização ERP")

# --- CONEXÃO COM SUPABASE (Lida com Nuvem e Local) ---
@st.cache_resource
def get_supabase_client():
    # O Streamlit Cloud usa st.secrets. Localmente, pode usar os.environ (se rodar com dotenv)
    url = st.secrets.get("SUPABASE_URL") or os.environ.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_KEY") or os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        st.error("ERRO FATAL: Chaves do Supabase não encontradas nos Secrets do Streamlit.")
        st.stop()
    return create_client(url, key)

supabase = get_supabase_client()
NOME_BUCKET = "catalog-images"

# --- INTERFACE DE USUÁRIO (O Painel do Tio) ---
st.markdown("### 1. Regras de Filtragem (Opcional)")
st.info("Deixe em branco ou cole os itens separados por linha (Enter).")

col1, col2, col3 = st.columns(3)
with col1:
    ids_raw = st.text_area("IDs Bloqueados", height=150, placeholder="Ex:\n302\n505\n991")
with col2:
    cats_raw = st.text_area("Categorias Bloqueadas", height=150, placeholder="Ex:\nFERRAMENTAS\nVENENO")
with col3:
    termos_raw = st.text_area("Termos Bloqueados", height=150, placeholder="Ex:\nDEFEITO\nTESTE\nUSADO")

st.markdown("### 2. Upload do Catálogo")
arquivo_planilha = st.file_uploader("Arraste a planilha (.xlsx) do ERP aqui", type=["xlsx"])

# --- MOTOR DE EXECUÇÃO ---
if st.button("🚀 INICIAR SINCRONIZAÇÃO", use_container_width=True, type="primary"):
    if not arquivo_planilha:
        st.warning("Você precisa fazer o upload da planilha antes de iniciar.")
        st.stop()

    # Processa as strings do painel em listas limpas
    ids_bloqueados = [x.strip() for x in ids_raw.split('\n') if x.strip()]
    categorias_bloqueadas = [x.strip().upper() for x in cats_raw.split('\n') if x.strip()]
    termos_bloqueados = [x.strip().upper() for x in termos_raw.split('\n') if x.strip()]

    # UI de Progresso
    status_text = st.empty()
    progress_bar = st.progress(0)
    console_log = st.empty()
    logs = []

    def log(mensagem):
        logs.append(mensagem)
        # Mantém apenas as últimas 5 mensagens na tela para não estourar a memória do navegador
        console_log.code('\n'.join(logs[-5:]), language="bash")

    status_text.text("[1/3] Lendo arquivo Excel...")
    
    # O Pandas consegue ler direto do arquivo em memória do Streamlit
    planilha = pd.read_excel(arquivo_planilha, skiprows=1) 
    
    col_id = planilha.columns[0]
    col_nome = planilha.columns[1]
    col_cat = planilha.columns[2]
    
    planilha.dropna(subset=[col_id, col_nome], inplace=True)

    status_text.text("[2/3] Aplicando filtros de negócio...")
    planilha[col_id] = planilha[col_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    if ids_bloqueados:
        planilha = planilha[~planilha[col_id].isin(ids_bloqueados)]
    if categorias_bloqueadas:
        planilha = planilha[~planilha[col_cat].astype(str).str.strip().str.upper().isin(categorias_bloqueadas)]
    if termos_bloqueados:
        padrao_termos = '|'.join(termos_bloqueados)
        planilha = planilha[~planilha[col_nome].astype(str).str.upper().str.contains(padrao_termos, na=False, regex=True)]

    total_produtos = len(planilha)
    status_text.text(f"[3/3] Sincronizando {total_produtos} produtos com a Nuvem...")
    log(f"Iniciando carga de {total_produtos} itens.")

    for i, row in enumerate(planilha.iterrows()):
        _, row_data = row
        id_prod = str(row_data.iloc[0]).replace('.0', '').strip()
        nome_prod = str(row_data.iloc[1]).strip()
        categoria_prod = str(row_data.iloc[2]).strip()
        
        try:
            preco_prod = round(float(row_data.iloc[3]), 2)
        except ValueError:
            log(f"[{id_prod}] Ignorado: Preço inválido.")
            continue

        url_imagem = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{id_prod}.jpg"
        nome_arquivo = f"{id_prod}.jpg"

        # Tenta listar o arquivo no Supabase
        try:
            arquivos_existentes = supabase.storage.from_(NOME_BUCKET).list(path="", options={"search": nome_arquivo})
            imagem_ja_no_bucket = any(arq['name'] == nome_arquivo for arq in arquivos_existentes)
        except Exception as e:
            imagem_ja_no_bucket = False

        if not imagem_ja_no_bucket:
            log(f"[{id_prod}] Sem foto. Raspando Bing: '{nome_prod}'...")
            
            # Usando diretório temporário blindado para nuvem Linux
            pasta_temp = tempfile.mkdtemp()
            
            try:
                crawler = BingImageCrawler(storage={'root_dir': pasta_temp}, log_level=50)
                crawler.crawl(keyword=nome_prod, max_num=1, filters={'type': 'photo'})
                
                arquivos_baixados = glob.glob(f"{pasta_temp}/*")
                if arquivos_baixados:
                    caminho_imagem_baixada = arquivos_baixados[0]
                    with open(caminho_imagem_baixada, "rb") as f:
                        supabase.storage.from_(NOME_BUCKET).upload(nome_arquivo, f)
                    log(f"[{id_prod}] Upload de imagem concluído.")
                else:
                    url_imagem = None
            except Exception as e:
                log(f"[{id_prod}] Falha no scraping: {e}")
                url_imagem = None
            finally:
                shutil.rmtree(pasta_temp)
                time.sleep(1) # Respeito à rede
        else:
            log(f"[{id_prod}] Imagem já no bucket. Atualizando texto...")

        # UPSERT NO BANCO
        try:
            dados_insercao = {
                "id": id_prod,
                "nome": nome_prod,
                "preco": preco_prod,
                "categoria": categoria_prod,
                "url_imagem": url_imagem
            }
            supabase.table("produtos").upsert(dados_insercao).execute()
        except Exception as e:
            log(f"[{id_prod}] ERRO DB: {e}")

        # Atualiza a barra de progresso visual
        progresso = (i + 1) / total_produtos
        progress_bar.progress(progresso)

    status_text.success("✅ Sincronização concluída com sucesso!")
    st.balloons()