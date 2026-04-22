import streamlit as st
import pandas as pd
import os
import tempfile
import time
import glob
from supabase import create_client, Client
from icrawler.builtin import BingImageCrawler

st.set_page_config(page_title="Motor ETL Synthesis", page_icon="⚙️", layout="wide")

# --- CONEXÃO ---
@st.cache_resource
def get_supabase_client():
    url = st.secrets.get("SUPABASE_URL") or os.environ.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_KEY") or os.environ.get("SUPABASE_KEY")
    return create_client(url, key)

supabase = get_supabase_client()
NOME_BUCKET = "catalog-images"

# --- LÓGICA DE MEMÓRIA (Persistence) ---
def carregar_config():
    res = supabase.table("etl_config").select("*").eq("id", "default").maybe_single().execute()
    return res.data if res.data else {"ids_bloqueados": "", "categorias_bloqueadas": "", "termos_bloqueados": ""}

def salvar_config(ids, cats, termos):
    supabase.table("etl_config").upsert({
        "id": "default",
        "ids_bloqueados": ids,
        "categorias_bloqueadas": cats,
        "termos_bloqueados": termos,
        "ultima_execucao": "now()"
    }).execute()
    st.toast("Configurações salvas no banco!", icon="💾")

config_inicial = carregar_config()

# --- INTERFACE ---
st.title("⚙️ Gerenciador de Sincronização")

with st.sidebar:
    st.header("💾 Memória do Sistema")
    if st.button("SALVAR REGRAS ATUAIS"):
        salvar_config(st.session_state.ids, st.session_state.cats, st.session_state.termos)
    st.caption(f"Última atualização: {config_inicial.get('ultima_execucao', 'Nunca')}")

st.markdown("### 1. Regras de Filtragem Ativas")
col1, col2, col3 = st.columns(3)
with col1:
    ids_input = st.text_area("IDs Bloqueados", value=config_inicial['ids_bloqueados'], key="ids", height=150)
with col2:
    cats_input = st.text_area("Categorias Bloqueadas", value=config_inicial['categorias_bloqueadas'], key="cats", height=150)
with col3:
    termos_input = st.text_area("Termos Bloqueados", value=config_inicial['termos_bloqueados'], key="termos", height=150)

st.markdown("### 2. Upload e Execução")
arquivo = st.file_uploader("Selecione a planilha do ERP (.xlsx)", type=["xlsx"])

if st.button("🚀 INICIAR PIPELINE", use_container_width=True, type="primary"):
    if not arquivo:
        st.error("Erro: Nenhuma planilha detectada.")
        st.stop()

    ids_blq = [x.strip() for x in ids_input.split('\n') if x.strip()]
    cats_blq = [x.strip().upper() for x in cats_input.split('\n') if x.strip()]
    termos_blq = [x.strip().upper() for x in termos_input.split('\n') if x.strip()]

    status_box = st.info("Executando leitura e sincronização...")
    
    # --- ETL ---
    df = pd.read_excel(arquivo, skiprows=1)
    df.dropna(subset=[df.columns[0], df.columns[1]], inplace=True)
    
    c_id, c_nome, c_cat, c_preco = df.columns[0], df.columns[1], df.columns[2], df.columns[3]
    df[c_id] = df[c_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    df = df[~df[c_id].isin(ids_blq)]
    df = df[~df[c_cat].astype(str).str.strip().str.upper().isin(cats_blq)]
    if termos_blq:
        df = df[~df[c_nome].astype(str).str.upper().str.contains('|'.join(termos_blq), na=False, regex=True)]

    # --- VARREDURA PAGINADA DO BUCKET (Silenciosa) ---
    arquivos_no_bucket = set()
    offset_atual = 0
    
    try:
        while True:
            blocos = supabase.storage.from_(NOME_BUCKET).list(path="", options={"limit": 1000, "offset": offset_atual})
            if not blocos:
                break
                
            for f in blocos:
                nome_arq = f.get('name')
                if nome_arq and nome_arq != '.emptyFolderPlaceholder':
                    arquivos_no_bucket.add(nome_arq.lower())
            
            if len(blocos) < 1000:
                break
            offset_atual += 1000
    except Exception as e:
        status_box.empty()
        st.error(f"Erro fatal ao ler o Storage: {e}")
        st.stop()

    lote_upsert = [] 
    produtos_baixados_lista = []
    MAX_SCRAPE_POR_SESSAO = 30 
    fotos_raspadas_agora = 0

    for _, row_data in df.iterrows():
        id_p, nome_p, cat_p, preco_p = str(row_data.iloc[0]), str(row_data.iloc[1]), str(row_data.iloc[2]), float(row_data.iloc[3])
        
        img_name = f"{id_p}.jpg"
        img_name_lower = img_name.lower()
        url_img = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{img_name}"
        
        if img_name_lower not in arquivos_no_bucket:
            if fotos_raspadas_agora < MAX_SCRAPE_POR_SESSAO:
                pasta_temp = tempfile.mkdtemp()
                try:
                    crawler = BingImageCrawler(storage={'root_dir': pasta_temp}, log_level=50)
                    crawler.crawl(keyword=nome_p, max_num=1, filters={'type': 'photo'})
                    
                    arquivos_baixados = glob.glob(f"{pasta_temp}/*")
                    if arquivos_baixados:
                        with open(arquivos_baixados[0], "rb") as f:
                            supabase.storage.from_(NOME_BUCKET).upload(img_name, f, file_options={"upsert": "true"})
                        fotos_raspadas_agora += 1
                        produtos_baixados_lista.append(nome_p)
                    else:
                        url_img = None
                except Exception:
                    url_img = None
                finally:
                    import shutil
                    if os.path.exists(pasta_temp):
                        shutil.rmtree(pasta_temp)
                    time.sleep(1) 
            else:
                url_img = None
        else:
            url_img = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{img_name}"

        lote_upsert.append({
            "id": id_p, "nome": nome_p, "preco": preco_p, "categoria": cat_p, "url_imagem": url_img
        })

    # --- GRAVAÇÃO ÚNICA ---
    try:
        supabase.table("produtos").upsert(lote_upsert).execute()
    except Exception as e:
        status_box.empty()
        st.error(f"Erro fatal de gravação no banco: {e}")
        st.stop()

    # --- FEEDBACK FINAL ---
    status_box.empty() 
    st.success("✅ Concluído.")
    
    if produtos_baixados_lista:
        st.write(f"**Fotos baixadas nesta execução ({len(produtos_baixados_lista)}):**")
        for nome_produto in produtos_baixados_lista:
            st.markdown(f"- {nome_produto}")
    else:
        st.write("**Nenhuma foto nova foi baixada nesta sessão.**")