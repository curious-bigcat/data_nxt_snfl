import json
import re
from typing import Any, Dict, List, Optional

import yaml

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


class OpenAIClientNotConfigured(Exception):
    pass


def _get_client(openai_api_key: Optional[str]):
    if not openai_api_key:
        raise OpenAIClientNotConfigured("OpenAI API key is required")
    if OpenAI is None:
        raise OpenAIClientNotConfigured("openai package not installed")
    return OpenAI(api_key=openai_api_key)


def generate_business_glossary_from_yaml(openai_api_key: str, yaml_content: str) -> Dict[str, Any]:
    """
    Call OpenAI to produce a business glossary JSON from the given semantic YAML string.
    Returns a dict, attempting to parse JSON; if parsing fails, returns {'text': <raw>}.
    """
    client = _get_client(openai_api_key)

    parsed = yaml.safe_load(yaml_content)

    prompt = (
        "You are a data governance expert. Given the following semantic YAML, "
        "produce a concise business glossary: list business terms, definitions, "
        "related columns/tables, and any data quality notes. Return JSON with keys: "
        "terms (array of {term, definition, related_columns, tables, dq_notes}).\n\n"
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
) -> str:
    """
    Call OpenAI to produce a Graphviz DOT diagram from lineage CSV rows and code blobs.
    Returns raw DOT text.
    """
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
            # Process full content per request (no truncation)
            parts.append(header + content)
        code_section = "\n\n".join(parts)

    # Theme guidance for consistent styling
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
    else:  # vibrant default
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


