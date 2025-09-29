## SNFL Data nxt — Governance, Glossary, and Lineage Toolkit

This project provides a Streamlit-based UI to explore Snowflake objects, generate a business glossary using AI, and design data lineage diagrams. It also includes Snowflake SQL to provision a demo environment with roles, warehouses, schemas, raw tables, stages, and harmonized/analytics/semantic layer views.

### Repository layout
- `streamlit_app.py`: Streamlit UI (modules: Data Object Explorer, Business Glossary Generator, Lineage Studio)
- `backend.py`: Unified backend (Snowflake utilities and AI services)
- `setup.sql`: End-to-end Snowflake setup: roles, warehouses, stages, schemas, raw tables and loads, harmonized/analytics/semantic layer views
- `requirements.txt`: Python dependencies

### Prerequisites
- Snowflake account and credentials with permissions per `setup.sql`
- Python 3.9+ (recommended 3.11)
- Access to OpenAI API key for AI-powered features

### Quick start
1) Clone and install
```bash
git clone <your-repo-url>.git
cd ey_data_next
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

2) Provision Snowflake (optional demo environment)
- Open `setup.sql` in Snowflake Worksheets and run. This creates:
  - Roles and warehouses
  - Stages and file formats
  - Schemas `tb_101.*` (raw_pos, raw_customer, raw_support, harmonized, analytics, governance, semantic_layer)
  - Raw tables and sample loads from S3 stages
  - Harmonized, analytics, and semantic layer views

3) Run the UI
```bash
streamlit run streamlit_app.py
```

### Using the app

1) Snowflake Connection (sidebar)
- Provide account, username, PAT (as password), and optional role/warehouse/database/schema.
- Click Connect.

2) Data Object Explorer
- Browse databases → schemas → objects.
- Expand tables/views to see columns; other objects show names and scope.

3) Business Glossary Generator
- Enter your OpenAI API key in the sidebar.
- Upload a semantic YAML and click “Generate Business Glossary”.
- View/download a CSV of column-level definitions and synonyms. Raw JSON is available for inspection.

4) Lineage Studio
- Enter your OpenAI API key in the sidebar.
- Upload a lineage CSV (relationships) and optional code files (SQL/Python/Java/Scala).
- Configure target, hops, theme, detail level; generate a Graphviz lineage diagram and download DOT.

 

### Environment variables
- OpenAI key: set in the app sidebar. Alternatively set `OPENAI_API_KEY` in your shell and wire it in as needed.

### Troubleshooting
- Pre-commit missing: If `git commit` fails with a pre-commit error locally, commit with `--no-verify` or install `pre-commit`.
- Streamlit import errors after refactor: restart Streamlit to load `backend.py` updates.
- Snowflake permissions: ensure roles and grants from `setup.sql` are applied.

### Contributing
- Use feature branches and PRs. Keep UI logic in `streamlit_app.py` and all service logic in `backend.py`.

### License
Proprietary / Internal.
