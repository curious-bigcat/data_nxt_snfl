import streamlit as st
from snowflake_utils import connect_to_snowflake, list_data_objects, get_schema_objects, get_table_or_view_columns, list_stages, list_files_in_stage, read_file_from_stage, SnowflakeConnectionError
import yaml
import csv
import re
from ai_services import generate_business_glossary_from_yaml, generate_lineage_dot

st.title("Data Explorer")

st.sidebar.header("Snowflake Connection")

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
    section = st.radio("Choose a module:", ["Data Object Explorer", "Data Explorer", "Lineage Designer"])  # added lineage

    if section == "Data Object Explorer":
        st.header("Data Object Explorer")
        try:
            data = list_data_objects(conn)
            db_names = list(data.keys())
            selected_db = st.selectbox("Select Database", db_names, key="db_obj")
            schema_names = list(data[selected_db].keys())
            selected_schema = st.selectbox("Select Schema", schema_names, key="schema_obj")
            st.write("Tables:", data[selected_db][selected_schema]['tables'])
            st.write("Views:", data[selected_db][selected_schema]['views'])

            # Show all schema objects
            st.subheader("All Schema Objects (Tables, Views, Stages, File Formats, Sequences, Functions, Procedures, Tasks, Streams, Pipes)")
            schema_objects = get_schema_objects(conn, selected_db, selected_schema)
            st.json(schema_objects)

            # Table or View columns and data types
            st.subheader("Inspect Table/View Columns and Data Types")
            all_objects = schema_objects.get('tables', []) + schema_objects.get('views', [])
            object_type_map = {name: 'table' for name in schema_objects.get('tables', [])}
            object_type_map.update({name: 'view' for name in schema_objects.get('views', [])})
            selected_object = st.selectbox("Select Table or View", all_objects, key="obj_col")
            if selected_object:
                obj_type = object_type_map[selected_object]
                st.markdown(f"**{obj_type.title()} Columns for `{selected_object}`**")
                columns = get_table_or_view_columns(conn, selected_db, selected_schema, selected_object, object_type=obj_type)
                st.table(columns)
        except Exception as e:
            st.error(str(e))

    elif section == "Data Explorer":
        st.header("Data Explorer")
        st.caption("Upload a semantic YAML file to generate a business glossary.")

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
                                st.subheader("Business Glossary (JSON)")
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

    elif section == "Lineage Designer":
        st.header("Lineage Designer")
        st.caption("Upload lineage CSV and related SQL/Python code to generate a lineage diagram.")

        # OpenAI API key input (shared key name to reuse value if already set)
        with st.sidebar:
            openai_api_key = st.text_input("OpenAI API Key", type="password", key="openai_api_key")

        lineage_csv = st.file_uploader("Upload Lineage CSV", type=["csv"], key="lineage_csv_upload")
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

        target = st.text_input("Target table/view for focused lineage (optional)", key="lineage_target")
        max_hops = st.slider("Max hops from target (upstream/downstream)", min_value=1, max_value=5, value=2, key="lineage_hops")
        theme = st.selectbox("Diagram theme", options=["vibrant", "muted", "monochrome"], index=0, key="lineage_theme")
        detail_level = st.selectbox("Detail level", options=["low", "medium", "high"], index=2, key="lineage_detail")
        include_sql_snippets = st.checkbox("Include SQL snippet excerpts", value=False, key="lineage_snippets")
        show_edge_labels = st.checkbox("Show edge operation labels (joins, agg, filter)", value=True, key="lineage_edge_labels")
        show_node_tooltips = st.checkbox("Show node tooltips (summaries)", value=True, key="lineage_tooltips")

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
