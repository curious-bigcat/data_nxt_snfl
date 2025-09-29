## EY Data Next — Governance, Glossary, and Lineage Toolkit

This project provides a Streamlit-based UI to explore Snowflake objects, generate a business glossary using AI, and design data lineage diagrams. It also includes Snowflake SQL to provision a demo environment and automate a new Sales Insights pipeline (facts, KPIs, DQ checks, anomalies) using UDFs, stored procedures, and task DAGs.

### Repository layout
- `streamlit_app.py`: Streamlit UI (modules: Data Object Explorer, Business Glossary Generator, Lineage Studio)
- `backend.py`: Unified backend (Snowflake utilities and AI services)
- `setup.sql`: End-to-end Snowflake setup: roles, warehouses, schemas, raw/harmonized/analytics views, and the Sales Insights pipeline (UDFs, procedures, tasks)
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
  - Schemas `tb_101.*` (raw, harmonized, analytics, governance, semantic_layer, analytics_mart, governance_logs)
  - Raw tables and sample loads
  - Harmonized/analytics views
  - New Sales Insights pipeline: UDFs, stored procedures, and tasks

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

### Sales Insights pipeline (Snowflake)
Defined in `setup.sql` under “New Pipeline: Sales Insights”.

- UDFs (SQL, immutable, memoizable):
  - `governance.SAFE_DIVIDE`, `governance.ZSCORE`, `governance.ROBUST_ZSCORE`, `governance.COALESCE_ZERO`
- Tables:
  - `analytics_mart.fact_daily_item_sales`, `analytics_mart.kpi_daily_brand`, `analytics_mart.sales_anomalies`, `governance_logs.dq_results`
- Procedures:
  - `sp_build_fact_daily_item_sales(days_back)`
  - `sp_build_kpi_daily_brand(days_back)`
  - `sp_run_dq_checks(days_back)`
  - `sp_detect_sales_anomalies(days_back)`
  - Orchestrator: `sp_run_new_pipeline(days_back)`
- Tasks (DAG):
  - `t_np_build_fact_daily` → `t_np_build_kpi` → `t_np_dq_checks` → `t_np_anomaly`

Manual run examples:
```sql
CALL tb_101.governance.sp_run_new_pipeline(7);
EXECUTE TASK tb_101.governance.t_np_build_fact_daily;
```

### Environment variables
- OpenAI key: set in the app sidebar. Alternatively set `OPENAI_API_KEY` in your shell and wire it in as needed.

### Troubleshooting
- Pre-commit missing: If `git commit` fails with a pre-commit error locally, commit with `--no-verify` or install `pre-commit`.
- Streamlit import errors after refactor: restart Streamlit to load `backend.py` updates.
- Snowflake permissions: ensure roles and grants from `setup.sql` are applied, especially for tasks and procedures.

### Contributing
- Use feature branches and PRs. Keep UI logic in `streamlit_app.py` and all service logic in `backend.py`.

### License
Proprietary / Internal.
