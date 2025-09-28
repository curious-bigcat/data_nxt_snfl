import snowflake.connector
import requests
import sqlparse
import yaml

class SnowflakeConnectionError(Exception):
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
        # List all databases
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
    """
    Returns all objects in a schema: tables, views, stages, file formats, sequences, functions, procedures, tasks, streams, pipes, etc.
    """
    try:
        cur = conn.cursor()
        objects = {}
        # Tables
        cur.execute(f"SHOW TABLES IN {database}.{schema}")
        objects['tables'] = [row[1] for row in cur.fetchall()]
        # Views
        cur.execute(f"SHOW VIEWS IN {database}.{schema}")
        objects['views'] = [row[1] for row in cur.fetchall()]
        # Stages
        cur.execute(f"SHOW STAGES IN {database}.{schema}")
        objects['stages'] = [row[1] for row in cur.fetchall()]
        # File formats
        cur.execute(f"SHOW FILE FORMATS IN {database}.{schema}")
        objects['file_formats'] = [row[1] for row in cur.fetchall()]
        # Sequences
        cur.execute(f"SHOW SEQUENCES IN {database}.{schema}")
        objects['sequences'] = [row[1] for row in cur.fetchall()]
        # Functions
        cur.execute(f"SHOW USER FUNCTIONS IN {database}.{schema}")
        objects['user_functions'] = [row[1] for row in cur.fetchall()]
        cur.execute(f"SHOW FUNCTIONS IN {database}.{schema}")
        objects['functions'] = [row[1] for row in cur.fetchall()]
        # Procedures
        cur.execute(f"SHOW PROCEDURES IN {database}.{schema}")
        objects['procedures'] = [row[1] for row in cur.fetchall()]
        # Tasks
        cur.execute(f"SHOW TASKS IN {database}.{schema}")
        objects['tasks'] = [row[1] for row in cur.fetchall()]
        # Streams
        cur.execute(f"SHOW STREAMS IN {database}.{schema}")
        objects['streams'] = [row[1] for row in cur.fetchall()]
        # Pipes
        cur.execute(f"SHOW PIPES IN {database}.{schema}")
        objects['pipes'] = [row[1] for row in cur.fetchall()]
        cur.close()
        return objects
    except Exception as e:
        raise Exception(f"Error fetching schema objects: {e}")

def get_table_or_view_columns(conn, database, schema, object_name, object_type='table'):
    """
    Returns columns and data types for a given table or view.
    object_type: 'table' or 'view'
    """
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

def generate_semantic_model(api_url, api_key, tables, columns):
    """
    Calls the FastGen or orchestrator API to generate a semantic model YAML for the selected tables and columns.
    Returns a dict with keys: semantic_model_yaml, suggestions, warnings, sqls_to_run
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "inputs": {
            "tables": tables,
            "columns": columns
        }
    }
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise Exception(f"Error calling semantic model API: {e}")

def list_stages(conn, database):
    """
    List all stages in the given database (across all schemas).
    Returns a list of (schema_name, stage_name) tuples.
    """
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW STAGES IN DATABASE {database}")
        # Columns: created_on, name (stage_name, idx=1), database_name, schema_name (idx=3), ...
        stages = [(row[3], row[1]) for row in cur.fetchall()]  # (schema_name, stage_name)
        cur.close()
        return stages
    except Exception as e:
        raise Exception(f"Error fetching stages: {e}")

def list_files_in_stage(conn, stage_full_name, database=None):
    """
    List all files in a given stage (stage_full_name = <db>.<schema>.<stage>), always using the selected database context if provided.
    """
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

def list_files_in_stage_raw(conn, stage_full_name, database=None):
    """
    List all files in a given stage (stage_full_name = <db>.<schema>.<stage>), returning all columns from LIST for debugging.
    """
    try:
        cur = conn.cursor()
        if database:
            cur.execute(f'USE DATABASE {database}')
        cur.execute(f"LIST @{stage_full_name}")
        rows = cur.fetchall()
        cur.close()
        return rows
    except Exception as e:
        raise Exception(f"Error listing files in stage {stage_full_name}: {e}")

def get_presigned_url(conn, stage_full_name, file_name):
    """
    Generate a presigned URL for a file in a stage using GET_PRESIGNED_URL.
    """
    try:
        cur = conn.cursor()
        # Use two arguments: stage reference and file path
        cur.execute("SELECT GET_PRESIGNED_URL(%s, %s)", (f"@{stage_full_name}", file_name))
        url = cur.fetchone()[0]
        cur.close()
        return url
    except Exception as e:
        raise Exception(f"Error generating presigned URL for {file_name} in stage {stage_full_name}: {e}")

def fetch_file_from_url(url):
    """
    Fetch the file content from a presigned URL.
    """
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        raise Exception(f"Error fetching file from presigned URL: {e}")

def read_file_from_stage(conn, stage_full_name, file_name):
    """
    Read the contents of a file from a stage using GET_PRESIGNED_URL and HTTP fetch.
    Returns the file as a string (assumes text/yaml).
    """
    try:
        url = get_presigned_url(conn, stage_full_name, file_name)
        return fetch_file_from_url(url)
    except Exception as e:
        raise Exception(f"Error reading file {file_name} from stage {stage_full_name}: {e}")

def get_stage_file_url(conn, stage_full_name, file_name):
    """
    Generate a stage file URL using BUILD_STAGE_FILE_URL for a file in a stage.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT BUILD_STAGE_FILE_URL(%s, %s)", (f"@{stage_full_name}", file_name))
        url = cur.fetchone()[0]
        cur.close()
        return url
    except Exception as e:
        raise Exception(f"Error generating stage file URL for {file_name} in stage {stage_full_name}: {e}")

# ------------------------------------
# SQL execution helpers
# ------------------------------------

def split_sql_statements(sql_text):
    """
    Split SQL script into individual statements using sqlparse.
    """
    return [stmt.strip() for stmt in sqlparse.split(sql_text) if stmt and stmt.strip()]

def execute_sql_script(conn, sql_text):
    """
    Execute a multi-statement SQL script.
    Returns a list of result dicts: { 'statement': str, 'success': bool, 'rows_affected': int, 'error': str }
    """
    results = []
    statements = split_sql_statements(sql_text)
    cur = conn.cursor()
    for stmt in statements:
        try:
            cur.execute(stmt)
            try:
                # For DML/DDL, rowcount may be -1; normalize to 0
                rows = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            except Exception:
                rows = 0
            results.append({
                'statement': stmt[:2000],
                'success': True,
                'rows_affected': rows,
                'error': ''
            })
        except Exception as e:
            results.append({
                'statement': stmt[:2000],
                'success': False,
                'rows_affected': 0,
                'error': str(e)
            })
    cur.close()
    return results

def execute_sql_file(conn, file_path):
    """
    Read a local SQL file and execute it.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        return execute_sql_script(conn, sql_text)
    except Exception as e:
        raise Exception(f"Error executing SQL file {file_path}: {e}")

def execute_sql_from_stage(conn, stage_full_name, file_name):
    """
    Read a SQL file from a Snowflake stage and execute it.
    """
    try:
        sql_text = read_file_from_stage(conn, stage_full_name, file_name)
        return execute_sql_script(conn, sql_text)
    except Exception as e:
        raise Exception(f"Error executing SQL from stage {stage_full_name}/{file_name}: {e}")

def list_and_read_yaml_files_from_stage(conn, stage_full_name, database=None):
    """
    List all .yaml/.yml files in a given stage and return their parsed YAML content.
    Returns a dict: {filename: parsed_yaml_content}
    """
    try:
        files = list_files_in_stage(conn, stage_full_name, database=database)
        yaml_files = [f for f in files if f.lower().endswith(('.yaml', '.yml'))]
        result = {}
        for file_name in yaml_files:
            try:
                content = read_file_from_stage(conn, stage_full_name, file_name)
                parsed = yaml.safe_load(content)
                result[file_name] = parsed
            except Exception as e:
                result[file_name] = f"Error reading/parsing: {e}"
        return result
    except Exception as e:
        raise Exception(f"Error listing/reading YAML files from stage {stage_full_name}: {e}")
