import streamlit as st
from backend import connect_to_snowflake, list_data_objects, get_schema_objects, get_table_or_view_columns, list_stages, list_files_in_stage, read_file_from_stage, SnowflakeConnectionError
import yaml
import csv
import io
import re
from backend import generate_business_glossary_from_yaml, generate_lineage_dot

# Page configuration and lightweight theming
st.set_page_config(page_title="SNFL Data nxt | Governance & Lineage", page_icon="📊", layout="wide")
st.markdown(
    """
    <style>
      .ey-app-title {font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem;}
      .ey-app-subtitle {color: #6b7280; font-size: 0.95rem; margin-top: 0;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div class='ey-app-title'>SNFL Data nxt</div>", unsafe_allow_html=True)
st.markdown("<p class='ey-app-subtitle'>Business glossary generation, Snowflake exploration, and lineage design</p>", unsafe_allow_html=True)



st.sidebar.header("Snowflake Connection")
st.sidebar.caption("Provide credentials to explore objects and run features")

account = st.sidebar.text_input("Account Identifier", key="account")
user = st.sidebar.text_input("Username", key="user")
pat = st.sidebar.text_input("Programmatic Access Token (PAT)", type="password", key="pat")
role = st.sidebar.text_input("Role (optional)", key="role")
warehouse = st.sidebar.text_input("Warehouse (optional)", key="warehouse")
database = st.sidebar.text_input("Database (optional)", key="database")
schema = st.sidebar.text_input("Schema (optional)", key="schema")

if 'connected' not in st.session_state:
    st.session_state['connected'] = False
    st.session_state['conn'] = None

if st.sidebar.button("Connect"):
    try:
        conn = connect_to_snowflake(
            user=user,
            password=pat,
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        st.session_state['connected'] = True
        st.session_state['conn'] = conn
        st.success("Connected to Snowflake!")
    except SnowflakeConnectionError as e:
        st.session_state['connected'] = False
        st.session_state['conn'] = None
        st.error(f"Connection failed: {e}")

if st.session_state.get("connected") and st.session_state.get("conn"):
    conn = st.session_state['conn']
    section = st.radio("Modules", ["Data Object Explorer", "Business Glossary", "Lineage Studio"])  # removed setup

    if section == "Data Object Explorer":
        st.header("Data Object Explorer")
        st.caption("Browse databases, schemas, and objects in your Snowflake account")
        try:
            data = list_data_objects(conn)
            db_names = sorted(data.keys())
            selected_db = st.selectbox("Database", db_names, key="explorer_db") if db_names else None
            if not selected_db:
                st.info("No databases available.")
            else:
                schemas = sorted(data[selected_db].keys())
                selected_schemas = st.multiselect("Schemas", schemas, default=schemas, key="explorer_schemas")

                # For each selected schema, fetch objects and render top-level expanders per object
                for schema_name in selected_schemas:
                    try:
                        schema_objects = get_schema_objects(conn, selected_db, schema_name)
                    except Exception as e:
                        st.error(f"Failed to load schema objects for {selected_db}.{schema_name}: {e}")
                        continue

                    counts = {k: len(v) for k, v in schema_objects.items() if isinstance(v, list)}
                    if counts:
                        st.caption(f"{selected_db}.{schema_name} — " + ", ".join([f"{k}: {v}" for k, v in sorted(counts.items())]))

                    # Tables (one expander per table)
                    for tbl in sorted(schema_objects.get('tables', [])):
                        with st.expander(f"Table: {selected_db}.{schema_name}.{tbl}"):
                            btn_key = f"tbl_btn_{selected_db}_{schema_name}_{tbl}"
                            if st.button("Show columns", key=btn_key):
                                try:
                                    cols = get_table_or_view_columns(conn, selected_db, schema_name, tbl, object_type='table')
                                    st.table(cols)
                                except Exception as e:
                                    st.error(str(e))

                    # Views (one expander per view)
                    for vw in sorted(schema_objects.get('views', [])):
                        with st.expander(f"View: {selected_db}.{schema_name}.{vw}"):
                            btn_key = f"vw_btn_{selected_db}_{schema_name}_{vw}"
                            if st.button("Show columns", key=btn_key):
                                try:
                                    cols = get_table_or_view_columns(conn, selected_db, schema_name, vw, object_type='view')
                                    st.table(cols)
                                except Exception as e:
                                    st.error(str(e))

                    # Other object categories (each item expandable)
                    def render_expandable_list(items, label):
                        for name in sorted(items):
                            with st.expander(f"{label}: {selected_db}.{schema_name}.{name}"):
                                st.write(f"Name: `{name}`")

                    render_expandable_list(schema_objects.get('stages', []), 'Stage')
                    render_expandable_list(schema_objects.get('file_formats', []), 'File Format')
                    render_expandable_list(schema_objects.get('sequences', []), 'Sequence')
                    render_expandable_list(schema_objects.get('functions', []), 'Function')
                    render_expandable_list(schema_objects.get('user_functions', []), 'User Function')
                    render_expandable_list(schema_objects.get('procedures', []), 'Procedure')
                    render_expandable_list(schema_objects.get('tasks', []), 'Task')
                    render_expandable_list(schema_objects.get('streams', []), 'Stream')
                    render_expandable_list(schema_objects.get('pipes', []), 'Pipe')
        except Exception as e:
            st.error(str(e))

    elif section == "Business Glossary":
        st.header("Business Glossary Generator")
        st.caption("Upload semantic YAML to generate per-column definitions and synonyms")

        # OpenAI API key input
        with st.sidebar:
            openai_api_key = st.text_input("OpenAI API Key", type="password", key="openai_api_key")

        uploaded_file = st.file_uploader("Upload Semantic YAML", type=["yaml", "yml"], key="sem_yaml_upload")
        if uploaded_file is not None:
            try:
                content = uploaded_file.read().decode("utf-8")
                parsed_yaml = yaml.safe_load(content)
                st.subheader("Uploaded YAML Preview")
                st.code(yaml.dump(parsed_yaml, sort_keys=False), language="yaml")

                if openai_api_key:
                    if st.button("Generate Business Glossary"):
                        try:
                            result = generate_business_glossary_from_yaml(openai_api_key, content)
                            if isinstance(result, dict) and "text" not in result:
                                # Expect column-level glossary
                                columns_glossary = result.get("columns")
                                if columns_glossary is None and isinstance(result.get("business_glossary"), dict):
                                    columns_glossary = result["business_glossary"].get("columns")
                                # Build rows: table, column, definition, synonyms
                                rows = []
                                if isinstance(columns_glossary, list):
                                    for c in columns_glossary:
                                        if not isinstance(c, dict):
                                            continue
                                        table_name = c.get("table") or ""
                                        column_name = c.get("column") or ""
                                        definition = c.get("definition") or c.get("description") or ""
                                        synonyms = c.get("synonyms") or []
                                        if isinstance(synonyms, list):
                                            synonyms = ", ".join([str(x) for x in synonyms])
                                        rows.append({
                                            "Table": table_name,
                                            "Column": column_name,
                                            "Definition": definition,
                                            "Synonyms": synonyms,
                                        })

                                st.subheader("Column Glossary")
                                if rows:
                                    st.dataframe(rows, use_container_width=True)
                                    # Download as CSV
                                    buf = io.StringIO()
                                    writer = csv.DictWriter(buf, fieldnames=["Table", "Column", "Definition", "Synonyms"])
                                    writer.writeheader()
                                    writer.writerows(rows)
                                    st.download_button(
                                        "Download Column Glossary CSV",
                                        data=buf.getvalue(),
                                        file_name="column_business_glossary.csv",
                                        mime="text/csv",
                                    )
                                else:
                                    st.info("No column glossary found. Showing raw JSON.")
                                    st.json(result)
                                # Optional: also show terms if provided
                                terms = result.get("terms")
                                if isinstance(terms, list) and terms:
                                    term_rows = []
                                    for t in terms:
                                        if not isinstance(t, dict):
                                            continue
                                        term_rows.append({
                                            "Term": t.get("term") or t.get("name") or "",
                                            "Definition": t.get("definition") or t.get("description") or "",
                                        })
                                    with st.expander("Business Terms (optional)"):
                                        st.dataframe(term_rows, use_container_width=True)
                                with st.expander("Raw JSON response"):
                                    st.json(result)
                            else:
                                st.subheader("Business Glossary (Text)")
                                st.write(result.get("text", ""))
                        except Exception as e:
                            st.error(str(e))
                else:
                    st.info("Enter your OpenAI API key in the sidebar to generate a glossary.")
            except Exception as e:
                st.error(f"Failed to parse uploaded YAML: {e}")
        else:
            st.info("Upload a semantic YAML file to begin.")

    elif section == "Lineage Studio":
        st.header("Lineage Studio")
        st.caption("Generate an interactive lineage diagram from CSV relationships and code context")

        # OpenAI API key input (shared key name to reuse value if already set)
        with st.sidebar:
            openai_api_key = st.text_input("OpenAI API Key", type="password", key="openai_api_key")

        lineage_csv = st.file_uploader("Upload lineage relationships (CSV)", type=["csv"], key="lineage_csv_upload")
        code_files = st.file_uploader(
            "Upload Pipeline Code Files (SQL/Python/Java/Scala)",
            type=["sql", "py", "java", "scala"],
            accept_multiple_files=True,
            key="lineage_code_upload",
        )

        csv_rows = []
        if lineage_csv is not None:
            try:
                csv_text = lineage_csv.read().decode("utf-8")
                reader = csv.DictReader(csv_text.splitlines())
                csv_rows = list(reader)
                st.subheader("Lineage CSV Preview")
                st.dataframe(csv_rows)
            except Exception as e:
                st.error(f"Failed to parse CSV: {e}")

        code_blobs = []
        if code_files:
            st.subheader("Uploaded Code Files")
            for f in code_files:
                try:
                    content = f.read().decode("utf-8", errors="replace")
                    code_blobs.append({"name": f.name, "content": content})
                    st.markdown(f"- `{f.name}` ({len(content)} chars)")
                except Exception as e:
                    st.markdown(f"- `{f.name}` (error reading: {e})")

        target = st.text_input("Target object (table/view) for focused lineage", key="lineage_target", help="Optional; centers the diagram on this node")
        max_hops = st.slider("Max hops from target (both directions)", min_value=1, max_value=5, value=2, key="lineage_hops")
        theme = st.selectbox("Diagram theme", options=["vibrant", "muted", "monochrome"], index=0, key="lineage_theme")
        detail_level = st.selectbox("Detail level", options=["low", "medium", "high"], index=2, key="lineage_detail")
        include_sql_snippets = st.checkbox("Include SQL snippet excerpts", value=False, key="lineage_snippets")
        show_edge_labels = st.checkbox("Show edge operation labels (joins, aggregation, filters)", value=True, key="lineage_edge_labels")
        show_node_tooltips = st.checkbox("Show node tooltips (summaries)", value=True, key="lineage_tooltips")
        include_ctes = st.checkbox("Include CTEs as nodes", value=True, key="lineage_ctes")
        include_column_lineage = st.checkbox("Include column-level lineage hints", value=True, key="lineage_col_lineage")
        include_file_stage_sources = st.checkbox("Include file/stage sources (COPY INTO, stages)", value=True, key="lineage_file_stage")

        additional_instructions = st.text_area(
            "Optional: Additional instructions/context for diagram generation",
            help="E.g., describe schema naming conventions, important transformations, or grouping rules."
        )

        if st.button("Generate Lineage Diagram"):
            if not openai_api_key:
                st.error("Enter your OpenAI API key in the sidebar.")
            elif lineage_csv is None and not code_blobs:
                st.error("Upload at least a lineage CSV or one code file.")
            else:
                try:
                    dot_text = generate_lineage_dot(
                        openai_api_key=openai_api_key,
                        lineage_rows=csv_rows,
                        code_blobs=code_blobs,
                        additional_instructions=additional_instructions,
                        target=target.strip() if target else None,
                        max_hops=int(max_hops),
                        theme=theme,
                        detail_level=detail_level,
                        include_sql_snippets=include_sql_snippets,
                        snippet_max_chars=180,
                        show_edge_labels=show_edge_labels,
                            show_node_tooltips=show_node_tooltips,
                            include_ctes=include_ctes,
                            include_column_lineage=include_column_lineage,
                            include_file_and_stage_sources=include_file_stage_sources,
                    )

                    if not dot_text.lower().startswith("digraph"):
                        st.warning("The model did not return DOT text as expected. Showing raw output.")
                        st.write(dot_text)
                    else:
                        st.subheader("Lineage Diagram")
                        st.graphviz_chart(dot_text)
                        st.subheader("DOT Source")
                        st.code(dot_text, language="dot")
                        st.download_button("Download DOT", data=dot_text, file_name="lineage.dot", mime="text/vnd.graphviz")

                except Exception as e:
                    st.error(str(e))
