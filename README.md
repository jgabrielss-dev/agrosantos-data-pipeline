```
# Agrosantos Data Pipeline: Automated ETL & Image Enrichment

This repository contains the Data Ingestion Engine (ETL) and Image Enrichment pipeline developed for Agrosantos, an agricultural e-commerce operation. It serves as the automated bridge between a legacy on-premise ERP system and a modern cloud infrastructure (Supabase/Next.js), ensuring the digital catalog is constantly synchronized and visually enriched without manual data entry.

## 🏗️ Architecture & Execution Flow

The pipeline is designed to be **strictly idempotent**. Whether it is executed once or a thousand times, the final state of the database remains consistent, with zero record duplication.

The operation is divided into four non-negotiable phases:

1. **Extract:**
   - Reads the raw `.xlsx` spreadsheet exported directly from the local ERP system.
2. **Transform:**
   - Sanitizes text strings, standardizes categories, and handles whitespace anomalies.
   - Parses and formats financial values into strict numeric types.
   - Handles null data and ERP input inconsistencies.
3. **Load (Supabase PostgreSQL):**
   - Executes `UPSERT` operations to synchronize the catalog.
   - Utilizes the ERP's native ID as the **Natural Primary Key**, guaranteeing that existing products are updated (price/stock/name) and new products are seamlessly inserted while preserving relational integrity.
4. **Enrich (Bing API & Cloud Storage):**
   - Scans the database for products lacking an assigned image URL.
   - Queries the **Bing Image Search API** using sanitized product nomenclature.
   - Downloads the optimal image match directly into memory.
   - Uploads the asset to **Supabase Storage** (public bucket).
   - Updates the database row with the final CDN image URL.

## ⚙️ Environment Variables (Secrets)

To run this pipeline locally or deploy it to the cloud (e.g., Streamlit Cloud), the following variables must be configured in a `.env` file or the host's Secrets manager:

```env
# Database & Storage
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_service_role_key

# Image Search Engine
BING_API_KEY=your_bing_search_api_key
(Warning: This script requires the Supabase service_role_key to bypass RLS policies for bulk backend operations. Never expose this key in client-side applications).

🚀 How to Run (Local Environment)
Clone the repository.

Create and activate a virtual environment (python -m venv venv).

Install the strict dependencies:

Bash
pip install pandas supabase requests python-dotenv openpyxl
Place the raw ERP spreadsheet in the root directory (matching the expected filename in the script).

Execute the engine:

Bash
python etl_agrosantos.py
☁️ Cloud Deployment Strategy
This engine is architected to run as an internal tool via Streamlit Cloud for non-technical operators.

The store manager uploads the daily spreadsheet via the web interface.

The Python script executes the heavy lifting in the background.

All secrets are securely managed through Streamlit's advanced configuration panel.

Architected and developed by João Gabriel.
```

2. **Create and activate virtual environment:**

   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # source venv/bin/activate  # Linux/Mac
   ```
3. **Install dependencies:**

   ```bash
   pip install pandas supabase requests python-dotenv openpyxl icrawler
   ```
4. **Configure environment variables** (create `.env` file):

   ```env
   SUPABASE_URL=your_supabase_project_url
   SUPABASE_KEY=your_supabase_service_role_key
   BING_API_KEY=your_bing_search_api_key
   ```
5. **Run the pipeline:**

   ```bash
   python etl-pipeline.py
   ```

## ☁️ Cloud Deployment Strategy

This engine is architected to run as an internal tool via **Streamlit Cloud** for non-technical operators.

- **Store manager** uploads the daily spreadsheet via web interface.
- **Python script** executes heavy lifting in the background.
- **Secrets** are securely managed through Streamlit's configuration panel.

## 🔧 Configuration

### 📝 Environment Variables (Secrets)

```env
# 🗄️ Database & Storage
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_service_role_key_here

# 🔍 Image Search Engine
BING_API_KEY=your_bing_search_api_key
```

> ⚠️ **Warning:** This script requires the Supabase `service_role_key` to bypass RLS policies for bulk backend operations. Never expose this key in client-side applications.

### 📁 Project Structure

```
agrosantos-data-pipeline/
├── etl-pipeline.py          # Main ETL script
├── upload-teste.py          # Upload test script
├── img/                     # Local images (backup)
├── README.md                # This documentation
└── .env                     # Environment variables (don't commit)
```

## 🤝 Contributing

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

**Architected and developed with ❤️ by [João Gabriel](https://github.com/your-username).**
