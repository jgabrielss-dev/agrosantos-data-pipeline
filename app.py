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
    
    status.text("Verificando imagens existentes no banco (Bulk Read)...")
    try:
        res_banco = supabase.table("produtos").select("id, url_imagem").execute()
        produtos_com_foto = {str(p['id']) for p in res_banco.data if p.get('url_imagem')}
    except Exception as e:
        st.error(f"Erro ao ler banco: {e}")
        produtos_com_foto = set()

    status.text(f"Preparando lote de {total} itens...")
    
    lote_upsert = [] # O "caminhão" que vai levar os dados de uma vez só
    MAX_SCRAPE_POR_SESSAO = 30 # Proteção contra Timeout do Streamlit
    fotos_raspadas_agora = 0
    fotos_adiadas = 0

    for i, row in enumerate(df.iterrows()):
        _, data = row
        id_p, nome_p, cat_p, preco_p = str(data.iloc[0]), str(data.iloc[1]), str(data.iloc[2]), float(data.iloc[3])
        
        img_name = f"{id_p}.jpg"
        url_img = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{img_name}"
        
        imagem_ja_no_bucket = id_p in produtos_com_foto

        if not imagem_ja_no_bucket:
            # Só raspa a imagem se ainda não estourou o limite seguro da nuvem
            if fotos_raspadas_agora < MAX_SCRAPE_POR_SESSAO:
                pasta_temp = tempfile.mkdtemp()
                try:
                    crawler = BingImageCrawler(storage={'root_dir': pasta_temp}, log_level=50)
                    crawler.crawl(keyword=nome_p, max_num=1, filters={'type': 'photo'})
                    
                    arquivos_baixados = glob.glob(f"{pasta_temp}/*")
                    if arquivos_baixados:
                        with open(arquivos_baixados[0], "rb") as f:
                            supabase.storage.from_(NOME_BUCKET).upload(img_name, f, file_options={"upsert": "true"})
                        stats["fotos_novas"] += 1
                        fotos_raspadas_agora += 1
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
                # Limite estourado: Pula o scraping para proteger a execução, mas salva o texto.
                url_img = None
                fotos_adiadas += 1
        else:
            url_img = f"{supabase.supabase_url}/storage/v1/object/public/{NOME_BUCKET}/{img_name}"

        # Guarda o produto no "caminhão" em vez de enviar pro banco agora
        lote_upsert.append({
            "id": id_p, "nome": nome_p, "preco": preco_p, "categoria": cat_p, "url_imagem": url_img
        })

        if i % 50 == 0 or i == total - 1:
            status.text(f"Processando textos e fotos: {i+1}/{total}...")
            prog_bar.progress(0.8 * ((i + 1) / total)) # Vai até 80% da barra na fase de processamento

    # --- GRAVAÇÃO EM LOTE (BULK UPSERT) ---
    status.text("Gravando catálogo no banco de dados (Operação em Lote)...")
    tamanho_lote = 500
    
    for i in range(0, len(lote_upsert), tamanho_lote):
        lote_atual = lote_upsert[i:i + tamanho_lote]
        try:
            supabase.table("produtos").upsert(lote_atual).execute()
            stats["atualizados"] += len(lote_atual)
            prog_bar.progress(0.8 + (0.2 * ((i + tamanho_lote) / len(lote_upsert))))
        except Exception as e:
            st.error(f"Erro ao gravar lote {i}: {e}")
            stats["erros"] += len(lote_atual)

    # --- FEEDBACK FINAL (O Dashboard) ---
    status.empty() 
    prog_bar.progress(1.0)
    st.success("✅ Sincronização Finalizada!")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Processados", total)
    c2.metric("Imagens Baixadas", stats["fotos_novas"])
    c3.metric("Sem Imagem (Adiadas)", fotos_adiadas, help="Rode novamente depois para baixar o resto.")
    c4.metric("Erros de Banco", stats["erros"])
    st.balloons()