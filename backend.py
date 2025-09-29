import json
import re
from typing import Any, Dict, List, Optional

import requests
import snowflake.connector
import sqlparse
import yaml

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


class SnowflakeConnectionError(Exception):
    pass


class OpenAIClientNotConfigured(Exception):
    pass


def connect_to_snowflake(user, password, account, role=None, warehouse=None, database=None, schema=None):
    try:
        conn_params = {
            'user': user,
            'password': password,  # PAT used as password
            'account': account
        }
        if role:
            conn_params['role'] = role
        if warehouse:
            conn_params['warehouse'] = warehouse
        if database:
            conn_params['database'] = database
        if schema:
            conn_params['schema'] = schema
        conn = snowflake.connector.connect(**conn_params)
        return conn
    except Exception as e:
        raise SnowflakeConnectionError(str(e))


def list_data_objects(conn):
    try:
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        dbs = cur.fetchall()
        db_names = [db[1] for db in dbs]
        data = {}
        for db in db_names:
            cur.execute(f"SHOW SCHEMAS IN DATABASE {db}")
            schemas = cur.fetchall()
            schema_names = [s[1] for s in schemas]
            data[db] = {}
            for schema in schema_names:
                cur.execute(f"SHOW TABLES IN {db}.{schema}")
                tables = cur.fetchall()
                table_names = [t[1] for t in tables]
                cur.execute(f"SHOW VIEWS IN {db}.{schema}")
                views = cur.fetchall()
                view_names = [v[1] for v in views]
                data[db][schema] = {
                    'tables': table_names,
                    'views': view_names
                }
        cur.close()
        return data
    except Exception as e:
        raise Exception(f"Error fetching data objects: {e}")


def get_schema_objects(conn, database, schema):
    try:
        cur = conn.cursor()
        objects = {}
        cur.execute(f"SHOW TABLES IN {database}.{schema}")
        objects['tables'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW VIEWS IN {database}.{schema}")
        objects['views'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW STAGES IN {database}.{schema}")
        objects['stages'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW FILE FORMATS IN {database}.{schema}")
        objects['file_formats'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW SEQUENCES IN {database}.{schema}")
        objects['sequences'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW USER FUNCTIONS IN {database}.{schema}")
        objects['user_functions'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW FUNCTIONS IN {database}.{schema}")
        objects['functions'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW PROCEDURES IN {database}.{schema}")
        objects['procedures'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW TASKS IN {database}.{schema}")
        objects['tasks'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW STREAMS IN {database}.{schema}")
        objects['streams'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW PIPES IN {database}.{schema}")
        objects['pipes'] = [row[1] for row in cur.fetchall()]
        cur.close()
        return objects
    except Exception as e:
        raise Exception(f"Error fetching schema objects: {e}")


def get_table_or_view_columns(conn, database, schema, object_name, object_type='table'):
    try:
        cur = conn.cursor()
        if object_type == 'table':
            cur.execute(f"SHOW COLUMNS IN TABLE {database}.{schema}.{object_name}")
        elif object_type == 'view':
            cur.execute(f"SHOW COLUMNS IN VIEW {database}.{schema}.{object_name}")
        else:
            raise Exception("object_type must be 'table' or 'view'")
        columns = [
            {
                'name': row[2],
                'type': row[3],
                'nullable': row[6],
                'default': row[7],
                'kind': row[8]
            }
            for row in cur.fetchall()
        ]
        cur.close()
        return columns
    except Exception as e:
        raise Exception(f"Error fetching columns for {object_type} {object_name}: {e}")


def list_stages(conn, database):
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW STAGES IN DATABASE {database}")
        stages = [(row[3], row[1]) for row in cur.fetchall()]  # (schema_name, stage_name)
        cur.close()
        return stages
    except Exception as e:
        raise Exception(f"Error fetching stages: {e}")


def list_files_in_stage(conn, stage_full_name, database=None):
    try:
        cur = conn.cursor()
        if database:
            cur.execute(f'USE DATABASE {database}')
        cur.execute(f"LIST @{stage_full_name}")
        files = [row[0] for row in cur.fetchall()]
        cur.close()
        return files
    except Exception as e:
        raise Exception(f"Error listing files in stage {stage_full_name}: {e}")


def get_presigned_url(conn, stage_full_name, file_name):
    try:
        cur = conn.cursor()
        cur.execute("SELECT GET_PRESIGNED_URL(%s, %s)", (f"@{stage_full_name}", file_name))
        url = cur.fetchone()[0]
        cur.close()
        return url
    except Exception as e:
        raise Exception(f"Error generating presigned URL for {file_name} in stage {stage_full_name}: {e}")


def fetch_file_from_url(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        raise Exception(f"Error fetching file from presigned URL: {e}")


def read_file_from_stage(conn, stage_full_name, file_name):
    try:
        url = get_presigned_url(conn, stage_full_name, file_name)
        return fetch_file_from_url(url)
    except Exception as e:
        raise Exception(f"Error reading file {file_name} from stage {stage_full_name}: {e}")


def split_sql_statements(sql_text):
    return [stmt.strip() for stmt in sqlparse.split(sql_text) if stmt and stmt.strip()]


def execute_sql_script(conn, sql_text):
    results = []
    statements = split_sql_statements(sql_text)
    cur = conn.cursor()
    for stmt in statements:
        try:
            cur.execute(stmt)
            try:
                rows = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            except Exception:
                rows = 0
            results.append({'statement': stmt[:2000], 'success': True, 'rows_affected': rows, 'error': ''})
        except Exception as e:
            results.append({'statement': stmt[:2000], 'success': False, 'rows_affected': 0, 'error': str(e)})
    cur.close()
    return results


def execute_sql_file(conn, file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        return execute_sql_script(conn, sql_text)
    except Exception as e:
        raise Exception(f"Error executing SQL file {file_path}: {e}")


def _get_client(openai_api_key: Optional[str]):
    if not openai_api_key:
        raise OpenAIClientNotConfigured("OpenAI API key is required")
    if OpenAI is None:
        raise OpenAIClientNotConfigured("openai package not installed")
    return OpenAI(api_key=openai_api_key)


def generate_business_glossary_from_yaml(openai_api_key: str, yaml_content: str) -> Dict[str, Any]:
    client = _get_client(openai_api_key)
    parsed = yaml.safe_load(yaml_content)
    prompt = (
        "You are a data governance expert. Given the following semantic YAML, produce a business glossary.\n"
        "Requirements:\n"
        "- Return JSON only (no prose).\n"
        "- Include a per-column glossary with definitions and synonyms.\n"
        "- JSON keys: columns (array of {table, column, definition, synonyms}), terms (optional array of {term, definition, related_columns, tables, dq_notes}).\n"
        "- In columns.synonyms, include up to 5 concise, business-friendly synonyms; omit duplicates.\n\n"
    )
    messages = [
        {"role": "system", "content": "You write precise, unambiguous business glossaries."},
        {"role": "user", "content": prompt + yaml.dump(parsed, sort_keys=False)},
    ]
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.2,
    )
    ai_text = response.choices[0].message.content or ""
    try:
        return json.loads(ai_text)
    except Exception:
        return {"text": ai_text}


def generate_lineage_dot(
    openai_api_key: str,
    lineage_rows: List[dict],
    code_blobs: List[Dict[str, str]],
    additional_instructions: str = "",
    target: Optional[str] = None,
    max_hops: int = 2,
    theme: str = "vibrant",
    detail_level: str = "high",
    include_sql_snippets: bool = False,
    snippet_max_chars: int = 180,
    show_edge_labels: bool = True,
    show_node_tooltips: bool = True,
    include_ctes: bool = True,
    include_column_lineage: bool = True,
    include_file_and_stage_sources: bool = True,
) -> str:
    client = _get_client(openai_api_key)

    csv_section = ""
    if lineage_rows:
        preview_rows = lineage_rows[:200]
        csv_headers = list(preview_rows[0].keys()) if preview_rows else []
        csv_lines = ",\n".join([str(r) for r in preview_rows])
        csv_section = (
            f"CSV headers: {csv_headers}\n"
            f"Sample rows (up to 200):\n{csv_lines}\n"
        )

    code_section = ""
    if code_blobs:
        parts = []
        for b in code_blobs:
            name = b.get("name", "unknown")
            content = b.get("content", "")
            lang = ""
            lname = name.lower()
            if lname.endswith(".sql"):
                lang = "SQL"
            elif lname.endswith(".py"):
                lang = "Python"
            elif lname.endswith(".java"):
                lang = "Java"
            elif lname.endswith(".scala"):
                lang = "Scala"
            header = f"FILE: {name} (language: {lang})\n"
            parts.append(header + content)
        code_section = "\n\n".join(parts)

    theme = (theme or "vibrant").lower()
    if theme == "monochrome":
        palette = {
            "graph_bg": "white",
            "edge": "#555555",
            "node_border": "#333333",
            "source_fill": "#DDDDDD",
            "transform_fill": "#BBBBBB",
            "table_fill": "#EEEEEE",
            "view_fill": "#CCCCCC",
            "stage_fill": "#F5F5F5",
            "target_border": "#111111",
            "target_fill": "#FFFFFF",
        }
    elif theme == "muted":
        palette = {
            "graph_bg": "white",
            "edge": "#8E8E8E",
            "node_border": "#6D6D6D",
            "source_fill": "#CFE8FF",
            "transform_fill": "#FFE9C6",
            "table_fill": "#E6F0FA",
            "view_fill": "#E8DDF0",
            "stage_fill": "#F2F4F7",
            "target_border": "#C62828",
            "target_fill": "#FFEBEE",
        }
    else:
        palette = {
            "graph_bg": "white",
            "edge": "#7A7A7A",
            "node_border": "#333333",
            "source_fill": "#A7E3FF",
            "transform_fill": "#FFD59E",
            "table_fill": "#BBDEFB",
            "view_fill": "#D1C4E9",
            "stage_fill": "#ECEFF1",
            "target_border": "#E53935",
            "target_fill": "#FFCDD2",
        }

    focus_clause = (
        f"Focus on the target node named '{target}'. Include only nodes within {max_hops} hops "
        f"upstream and {max_hops} hops downstream of the target. Highlight the target with penwidth=3, "
        f"color=\"{palette['target_border']}\", fillcolor=\"{palette['target_fill']}\".\n"
        if target else ""
    )

    style_parts = [
        "Style requirements (mandatory):",
        f"- graph [bgcolor=\"{palette['graph_bg']}\", rankdir=LR]; edge [color=\"{palette['edge']}\", arrowsize=0.7, penwidth=1];",
        f"- node [style=filled, color=\"{palette['node_border']}\", fontname=\"Helvetica\", fontsize=10];",
        f"- sources/external nodes: shape=cylinder, fillcolor=\"{palette['source_fill']}\";",
        f"- transformation nodes (SQL/Python jobs): shape=box3d, fillcolor=\"{palette['transform_fill']}\";",
        f"- tables: shape=box, fillcolor=\"{palette['table_fill']}\"; views: shape=component, fillcolor=\"{palette['view_fill']}\";",
        f"- staging/temp artifacts: shape=folder, fillcolor=\"{palette['stage_fill']}\";",
        "- cluster by system/schema using subgraph cluster_* with readable labels;",
    ]
    if show_edge_labels:
        style_parts.append("- include edge labels describing operation (e.g., JOIN on id, GROUP BY, FILTER).")
    if show_node_tooltips:
        style_parts.append("- include node tooltips summarizing key transformations/aggregations.")
    style_parts.append("- keep result under 200 nodes; prune minor nodes if needed.\n")
    style_clause = "\n".join(style_parts) + "\n"

    detail_parts = [
        "Details to extract from code and CSV (prioritize accuracy):",
        "- Joins: type (INNER/LEFT/RIGHT/FULL), keys/conditions.",
        "- Aggregations: functions (SUM, COUNT, AVG, MIN, MAX), group-by columns.",
        "- Projections/renames: key selected columns and aliases.",
        "- Filters: WHERE predicates.",
        "- Windows: partition/order and functions.",
        "- Materializations: table/view names; note temp/stage objects.",
    ]
    if include_ctes:
        detail_parts.append("- Common Table Expressions (CTEs): treat named CTEs as transformation nodes; show edges from their inputs.")
    if include_file_and_stage_sources:
        detail_parts.append("- External sources: S3/GCS/Azure stages and COPY INTO; add source nodes for stages and files if referenced.")
    if include_column_lineage:
        detail_parts.append("- Column-level lineage: when clear, add edge labels like colA->colB for key columns.")
    if include_sql_snippets:
        detail_parts.append("- Include concise SQL snippet excerpts per transformation (truncated).")
        detail_parts.append(f"- Truncate any SQL snippet to <= {snippet_max_chars} chars.")
    if show_edge_labels:
        detail_parts.append("- Prefer edge labels for operation types; keep them short.")
    if show_node_tooltips:
        detail_parts.append("- Use node 'tooltip' to hold multi-line summaries; keep UI-friendly.")
    detail_parts.append(f"- Level of detail: {detail_level}.")
    detail_clause = "\n".join(detail_parts) + "\n"

    system_msg = {
        "role": "system",
        "content": (
            "You are an expert data engineer. Given lineage CSV and pipeline code, "
            "produce a styled Graphviz DOT diagram describing end-to-end lineage. Use rankdir=LR, "
            "distinct nodes for sources, transformations, tables, views; edges show data flow. "
            "Output ONLY valid DOT text (no markdown fences, no commentary)."
        ),
    }
    user_msg = {
        "role": "user",
        "content": (
            "Create a lineage diagram in Graphviz DOT format.\n\n"
            + focus_clause
            + style_clause
            + detail_clause
            + ("LINEAGE CSV:\n" + csv_section + "\n\n" if csv_section else "")
            + ("PIPELINE CODE SNIPPETS:\n" + code_section + "\n\n" if code_section else "")
            + ("ADDITIONAL INSTRUCTIONS:\n" + additional_instructions + "\n" if additional_instructions else "")
            + (
                "Constraints:\n"
                "- Return ONLY raw DOT starting with 'digraph' and ending with '}'.\n"
                "- Use rankdir=LR; cluster by system or schema if clear.\n"
                "- Use readable labels; keep graph under 200 nodes if necessary.\n"
            )
        ),
    }

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[system_msg, user_msg],
        temperature=0.1,
    )
    dot_text = response.choices[0].message.content or ""
    dot_text = re.sub(r"^```[a-zA-Z]*\n|\n```$", "", dot_text.strip())
    return dot_text


