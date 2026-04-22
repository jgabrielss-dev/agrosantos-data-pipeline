import streamlit as st
import pandas as pd
import os
import tempfile
import time
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

# Carrega os dados iniciais do banco
config_inicial = carregar_config()

# --- INTERFACE ---
st.title("⚙️ Gerenciador de Sincronização")

with st.sidebar:
    st.header("💾 Memória do Sistema")
    if st.button("SALVAR REGRAS ATUAIS"):
        # As variáveis serão pegas do estado dos text_areas
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

    # Preparação das listas
    ids_blq = [x.strip() for x in ids_input.split('\n') if x.strip()]
    cats_blq = [x.strip().upper() for x in cats_input.split('\n') if x.strip()]
    termos_blq = [x.strip().upper() for x in termos_input.split('\n') if x.strip()]

    # Métricas de Feedback
    stats = {"novos": 0, "atualizados": 0, "fotos_novas": 0, "erros": 0}
    
    prog_bar = st.progress(0)
    status = st.empty()
    
    # --- ETL ---
    df = pd.read_excel(arquivo, skiprows=1)
    df.dropna(subset=[df.columns[0], df.columns[1]], inplace=True)
    
    c_id, c_nome, c_cat, c_preco = df.columns[0], df.columns[1], df.columns[2], df.columns[3]
    df[c_id] = df[c_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # Filtros
    df = df[~df[c_id].isin(ids_blq)]
    df = df[~df[c_cat].astype(str).str.strip().str.upper().isin(cats_blq)]
    if termos_blq:
        df = df[~df[c_nome].astype(str).str.upper().str.contains('|'.join(termos_blq), na=False, regex=True)]

    total = len(df)
    
    for i, row in enumerate(df.iterrows()):
        _, data = row
        id_p, nome_p, cat_p, preco_p = str(data.iloc[0]), str(data.iloc[1]), str(data.iloc[2]), float(data.iloc[3])
        
        status.text(f"Processando {i+1}/{total}: {nome_p[:30]}...")
        
        # Lógica de Imagem (Simplificada para o exemplo)
        img_name = f"{id_p}.jpg"
        url_img = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{img_name}"
        
        # Verifica se já existe (Feedback visual)
        try:
            check = supabase.storage.from_(NOME_BUCKET).list(path="", options={"search": img_name})
            if not any(f['name'] == img_name for f in check):
                # Raspagem (Omitida aqui por brevidade, use a lógica do seu script anterior)
                stats["fotos_novas"] += 1
        except: pass

        # Upsert
        try:
            res = supabase.table("produtos").upsert({
                "id": id_p, "nome": nome_p, "preco": preco_p, "categoria": cat_p, "url_imagem": url_img
            }).execute()
            stats["atualizados"] += 1
        except:
            stats["erros"] += 1

        prog_bar.progress((i + 1) / total)

    # --- FEEDBACK FINAL (O Dashboard) ---
    st.success("Sincronização Finalizada!")
    c1, c2, c3 = st.columns(3)
    c1.metric("Produtos Processados", total)
    c2.metric("Imagens Novas", stats["fotos_novas"])
    c3.metric("Erros de Banco", stats["erros"])
    st.balloons()