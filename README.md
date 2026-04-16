# Agrosantos Data Pipeline

## 🚀 Missão
Este repositório isola a inteligência de dados da Agrosantos. O objetivo é atuar como uma ponte automatizada (ETL) entre o sistema legado (ERP via exportação de planilhas) e uma infraestrutura moderna em nuvem (Supabase), eliminando processos manuais de atualização de catálogo.

## 🛠️ Stack Técnica
- **Linguagem:** Python 3.12+
- **Processamento de Dados:** Pandas (Extração e Limpeza)
- **Infraestrutura Cloud:** Supabase (PostgreSQL + Storage)
- **Segurança:** Dotenv (Gestão de segredos e chaves de API)

## 🏗️ Arquitetura do Pipeline (ETL)

O script principal `etl_pipeline.py` executa o ciclo completo de sincronização:

1. **Extract (Extração):** Leitura bruta da planilha `.xlsx` exportada pelo ERP, ignorando cabeçalhos redundantes e tratando inconsistências de tipos.
2. **Transform (Transformação):** - Limpeza de IDs (remoção de decimais de Excel).
   - Normalização de preços para formato numérico `float`.
   - Mapeamento de categorias e sanitização de strings.
3. **Load (Carga):**
   - **Storage:** Upload inteligente de imagens para o Supabase Storage (ignora arquivos já existentes para economizar banda).
   - **Database:** Operação de `UPSERT` na tabela `produtos` utilizando o `codigo_erp` como chave única, garantindo que o catálogo web reflita sempre o preço e estoque mais recentes sem duplicar registros.

## 🔐 Configuração e Segurança

O projeto utiliza um arquivo `.env` (ignorado pelo Git) para proteger as credenciais de infraestrutura.

**Variáveis necessárias:**
- `SUPABASE_URL`: Endpoint do projeto.
- `SUPABASE_KEY`: Service Role Key (acesso administrativo para escrita).

## 📊 Status do Projeto
- [x] Sprint 0: Setup de ambiente e infraestrutura básica.
- [x] Sprint 1: Provisionamento de Buckets e Tabelas.
- [x] Sprint 2: Implementação do motor de ingestão Python/Pandas.
- [ ] Sprint 3: Consumo da API pelo Front-end (Next.js).

---
**Desenvolvido por João Gabriel** *Engenharia de Software | Agrosantos Pipeline*
