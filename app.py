import streamlit as st
import pandas as pd
import os
import tempfile
import glob
import shutil
from supabase import create_client, Client
from icrawler.builtin import BingImageCrawler

st.set_page_config(page_title="Motor ETL", page_icon="⚙️")

@st.cache_resource
def get_supabase_client():
    url = st.secrets.get("SUPABASE_URL") or os.environ.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_KEY") or os.environ.get("SUPABASE_KEY")
    return create_client(url, key)

supabase = get_supabase_client()
NOME_BUCKET = "catalog-images"

# --- INTERFACE ---
st.title("Sincronizador Agrosantos")

# Recupera regras do banco silenciosamente
res_config = supabase.table("etl_config").select("*").eq("id", "default").maybe_single().execute()
config = res_config.data or {"ids_bloqueados": "", "categorias_bloqueadas": "", "termos_bloqueados": ""}

ids_blq = [x.strip() for x in config['ids_bloqueados'].split('\n') if x.strip()]
cats_blq = [x.strip().upper() for x in config['categorias_bloqueadas'].split('\n') if x.strip()]
termos_blq = [x.strip().upper() for x in config['termos_bloqueados'].split('\n') if x.strip()]

arquivo = st.file_uploader("Upload da Planilha", type=["xlsx"])

if st.button("SINCRONIZAR"):
    if not arquivo:
        st.stop()
    
    msg_status = st.empty()
    msg_status.info("Executando...")

    # 1. Carregamento e Filtros (Memória)
    df = pd.read_excel(arquivo, skiprows=1)
    df.dropna(subset=[df.columns[0], df.columns[1]], inplace=True)
    c_id, c_nome, c_cat, c_preco = df.columns[0], df.columns[1], df.columns[2], df.columns[3]
    df[c_id] = df[c_id].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    df = df[~df[c_id].isin(ids_blq)]
    df = df[~df[c_cat].astype(str).str.strip().str.upper().isin(cats_blq)]
    if termos_blq:
        df = df[~df[c_nome].astype(str).str.upper().str.contains('|'.join(termos_blq), na=False, regex=True)]

    # 2. Check de Imagens Existentes (Rede - Chamada Única)
    res_banco = supabase.table("produtos").select("id, url_imagem").execute()
    produtos_com_foto = {str(p['id']) for p in res_banco.data if p.get('url_imagem')}

    lote_upsert = []
    fotos_baixadas = 0

    # 3. Loop de Processamento (Silencioso)
    for _, row_data in df.iterrows():
        id_p, nome_p, cat_p, preco_p = str(row_data.iloc[0]), str(row_data.iloc[1]), str(row_data.iloc[2]), float(row_data.iloc[3])
        
        img_name = f"{id_p}.jpg"
        url_img = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{img_name}"

        if id_p not in produtos_com_foto:
            pasta_temp = tempfile.mkdtemp()
            try:
                crawler = BingImageCrawler(storage={'root_dir': pasta_temp}, log_level=50)
                crawler.crawl(keyword=nome_p, max_num=1, filters={'type': 'photo'})
                baixados = glob.glob(f"{pasta_temp}/*")
                if baixados:
                    with open(baixados[0], "rb") as f:
                        supabase.storage.from_(NOME_BUCKET).upload(img_name, f, file_options={"upsert": "true"})
                    fotos_baixadas += 1
                else:
                    url_img = None
            except:
                url_img = None
            finally:
                if os.path.exists(pasta_temp):
                    shutil.rmtree(pasta_temp)
        
        lote_upsert.append({
            "id": id_p, "nome": nome_p, "preco": preco_p, "categoria": cat_p, "url_imagem": url_img
        })

    # 4. Upload em Lote (Bulk Upsert)
    # Quebra em pedaços de 1000 para garantir que o payload não seja rejeitado pelo Supabase
    for i in range(0, len(lote_upsert), 1000):
        supabase.table("produtos").upsert(lote_upsert[i:i+1000]).execute()

    msg_status.empty()
    st.success("Concluído.")
    st.write(f"Sincronização finalizada. Imagens baixadas nesta sessão: {fotos_baixadas}")