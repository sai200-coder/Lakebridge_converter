# Databricks notebook source
import os
import nbformat
from nbformat.v4 import new_notebook, new_code_cell
from dotenv import load_dotenv
from groq import Groq
from models.modify_model import ModifyNotebookModel
from datetime import datetime


def _clean_sql_output(text: str) -> str:
    """Normalize LLM output to plain SQL text.

    - Strips surrounding triple quotes
    - Removes markdown code fences (```sql ... ``` and ``` ... ```)
    - Removes leading descriptor lines like 'Modified SQL from:' if present
    """
    if not text:
        return ""

    cleaned = text.strip()

    # Remove surrounding triple quotes if present
    if cleaned.startswith('"""') and cleaned.endswith('"""'):
        cleaned = cleaned[3:-3].strip()

    # If content contains fenced code blocks, extract the first fenced block
    # Prefer ```sql fenced block; fall back to generic ```
    lines = cleaned.splitlines()
    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        fence = line.strip()
        if fence.startswith("```sql") or fence == "```":
            start_idx = i + 1
            break

    if start_idx is not None:
        for j in range(start_idx, len(lines)):
            if lines[j].strip() == "```":
                end_idx = j
                break
        if end_idx is not None and end_idx > start_idx:
            cleaned = "\n".join(lines[start_idx:end_idx]).strip()
        else:
            # No closing fence; take the rest
            cleaned = "\n".join(lines[start_idx:]).strip()
    else:
        # No fences; remove any leading descriptor line like '# Modified SQL from: ...'
        if lines and lines[0].lstrip().startswith(("# Modified SQL from:", "Modified SQL from:")):
            cleaned = "\n".join(lines[1:]).strip()

    return cleaned


def _split_sql_statements(sql_text: str) -> list:
    """Split SQL text into individual statements, keeping semicolons.

    This is a simple splitter that does not fully parse SQL, but works well for
    typical scripts generated here. It respects basic string literals to avoid
    splitting on semicolons inside single quotes.
    """
    statements = []
    current = []
    in_single_quote = False
    escape_next = False

    for char in sql_text:
        current.append(char)
        if escape_next:
            escape_next = False
            continue
        if char == "\\":
            escape_next = True
            continue
        if char == "'":
            in_single_quote = not in_single_quote
            continue
        if char == ";" and not in_single_quote:
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

    tail = "".join(current).strip()
    if tail:
        statements.append(tail if tail.endswith(";") else f"{tail};")

    return statements


def _classify_statement(statement: str) -> str:
    """Return one of: 'ddl', 'dml', 'select'."""
    stripped = statement.lstrip()
    # Remove leading SQL comments lines for classification
    while stripped.startswith("--"):
        newline_idx = stripped.find("\n")
        if newline_idx == -1:
            break
        stripped = stripped[newline_idx + 1:].lstrip()

    lowered = stripped.lower()
    if lowered.startswith("create ") or lowered.startswith("drop ") or lowered.startswith("alter "):
        return "ddl"
    if lowered.startswith("insert ") or lowered.startswith("update ") or lowered.startswith("merge ") or lowered.startswith("delete "):
        return "dml"
    if lowered.startswith("select ") or lowered.startswith("with "):
        return "select"
    # Default to DML if it's data-changing keywords appear inside; else select
    if any(k in lowered for k in [" insert ", " update ", " delete ", " merge "]):
        return "dml"
    return "select"


def _organize_sql_blocks(clean_sql: str) -> tuple:
    """Return (ddl_block, dml_block, select_block) as strings.

    Preserves original statement indentation/formatting.
    """
    statements = _split_sql_statements(clean_sql)
    ddl_stmts = []
    dml_stmts = []
    select_stmts = []

    for stmt in statements:
        kind = _classify_statement(stmt)
        if kind == "ddl":
            ddl_stmts.append(stmt.strip())
        elif kind == "dml":
            dml_stmts.append(stmt.strip())
        else:
            select_stmts.append(stmt.strip())

    ddl_block = "\n\n".join(ddl_stmts).strip()
    dml_block = "\n\n".join(dml_stmts).strip()
    select_block = "\n\n".join(select_stmts).strip()
    return ddl_block, dml_block, select_block


def _save_modified_sql_to_notebook(modified_text: str, sql_filename: str, output_dir: str) -> None:
    """Create a notebook with cells inferred from fenced code blocks.

    - ```sql blocks become %sql cells
    - ```python blocks become Python cells
    - If no fences, treat as SQL and split into DDL/DML/SELECT blocks
    """
    notebook = new_notebook()

    text = (modified_text or "").strip()
    cells = []

    # Parse fenced blocks first
    lines = text.splitlines()
    i = 0
    found_fences = False
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("```"):
            lang = line[3:].strip().lower()
            start = i + 1
            j = start
            while j < len(lines) and lines[j].strip() != "```":
                j += 1
            block = "\n".join(lines[start:j]).rstrip()
            if lang in ("sql", ""):
                content = block if block.startswith("%sql") else f"%sql\n{block}"
                cells.append(new_code_cell(content))
            elif lang == "python":
                cells.append(new_code_cell(block))
            found_fences = True
            i = j + 1
            continue
        i += 1

    if not found_fences:
        # Fall back to SQL organization
        clean_sql = _clean_sql_output(text)
        ddl_block, dml_block, select_block = _organize_sql_blocks(clean_sql)
        if ddl_block:
            cells.append(new_code_cell(f"%sql\n{ddl_block}"))
        if dml_block:
            cells.append(new_code_cell(f"%sql\n{dml_block}"))
        if select_block:
            cells.append(new_code_cell(f"%sql\n{select_block}"))

    notebook.cells = cells if cells else [new_code_cell("%sql\n")]
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    notebook_filename = f"modified_{os.path.splitext(sql_filename)[0]}_{current_time}.ipynb"
    notebook_path = os.path.join(output_dir, notebook_filename)

    os.makedirs(output_dir, exist_ok=True)
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(notebook, f)

    print(f"Notebook created: {notebook_path}")


def _save_modified_sql_to_file(modified_sql: str, sql_filename: str, output_dir: str) -> str:
    """Save cleaned SQL to a .sql file and return its path."""
    os.makedirs(output_dir, exist_ok=True)
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_out_filename = f"modified_{os.path.splitext(sql_filename)[0]}_{current_time}.sql"
    sql_out_path = os.path.join(output_dir, sql_out_filename)

    cleaned_sql = _clean_sql_output(modified_sql)
    ddl_block, dml_block, select_block = _organize_sql_blocks(cleaned_sql)

    # Join blocks with a clear blank line separation
    blocks = [b for b in [ddl_block, dml_block, select_block] if b]
    final_text = ("\n\n".join(blocks) + "\n") if blocks else "\n"

    with open(sql_out_path, 'w', encoding='utf-8') as f:
        f.write(final_text)

    print(f"SQL file created: {sql_out_path}")
    return sql_out_path


def run_modify_and_create_notebooks(cfg: ModifyNotebookModel, catalog_name: str, schema_name: str) -> None:

    load_dotenv()
    client = Groq()

    transpiled_dir = cfg.transpiled_dir
    output_dir = cfg.output_dir

    if not os.path.exists(transpiled_dir):
        print(f"Directory '{transpiled_dir}' not found!")
        return

    sql_files = [f for f in os.listdir(transpiled_dir) if f.endswith('.sql')]
    if not sql_files:
        print("No SQL files found in transpiled directory.")
        return

    print(f"Found {len(sql_files)} SQL files to process:")
    for sql_file in sql_files:
        print(f"Processing {sql_file}...")
        try:
            file_path = os.path.join(transpiled_dir, sql_file)
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            
            prompt = f"""
You are an expert in SQL migration to Databricks.

Rewrite the following SQL into a single corrected Databricks SQL script.

Rules:
- Use full three-level table names: catalog.schema.table  
- Defaults if missing:  
  - catalog: {catalog_name}  
  - schema: {schema_name}  
- Convert data types (Snowflake → Databricks SQL):
  NVARCHAR, VARCHAR(n), VARCHAR, TEXT, CHAR(n) → STRING  
  NUMBER(p,s), DECIMAL(p,s), NUMERIC(p,s) → DECIMAL(p,s)  
  FLOAT, FLOAT4, FLOAT8, DOUBLE, REAL → DOUBLE  
  INTEGER, INT, BIGINT, SMALLINT → BIGINT or INT depending on range  
  BOOLEAN → BOOLEAN  
  DATE → DATE  
  TIME → STRING  
  TIMESTAMP_NTZ, TIMESTAMP_LTZ, TIMESTAMP_TZ, TIMESTAMP → TIMESTAMP  
- Replace Snowflake DDL with Databricks equivalents:
  CREATE OR REPLACE DATABASE → CREATE DATABASE IF NOT EXISTS  
  CREATE OR REPLACE SCHEMA  → CREATE SCHEMA IF NOT EXISTS  
  USE DATABASE and USE SCHEMA remain the same in Databricks  
- Remove unsupported constraints: PRIMARY KEY, FOREIGN KEY, UNIQUE, DEFAULT, CHECK  
- Convert functions:
  GETDATE(), NOW(), SYSDATETIME() → current_timestamp()  
  ISNULL(a,b) → coalesce(a,b)  
  LEN(x) → length(x)  
  CAST / CONVERT(expr AS datatype) → CAST(expr AS <converted datatype>)  
- Remove any Snowflake-specific syntax or unsupported features (e.g. VARIANT, OBJECT, ARRAY unless mapped/handled)  
- Fix any syntax errors automatically  
- Do NOT add:
  - WITH (...) clauses  
  - COMMENT statements  
  - Extra explanations  
  - Multiple versions  
- Only return one valid Databricks SQL script
if have a Snowflake stored procedure written in SQL. Please convert it into a Python equivalent suitable for running in a Databricks notebook (including as a PySpark UDF if needed).

Instructions:

Include the procedure’s name, parameters, return type and logic in the converted code.

Write a Python function with type hints that implements the same logic.

Also show how to register that function as a PySpark UDF (if it makes sense) and how to use it on a DataFrame.

Give an example call showing sample inputs and outputs.

Point out any differences or caveats between Snowflake SQL procedural behavior and Python.
SQL Code:
{sql_content}
"""




            completion = client.chat.completions.create(
                model=cfg.llm_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=cfg.temperature,
                max_completion_tokens=4096,
                top_p=0.95,
                stream=False,
                stop=None,
            )

            modified_sql = completion.choices[0].message.content
            # Save only as a cleaned notebook (.ipynb)
            _save_modified_sql_to_notebook(modified_sql, sql_file, output_dir)
            print(f"Successfully processed {sql_file}")
            print("-" * 50)
        except Exception as e:
            print(f"Error processing {sql_file}: {str(e)}")
            print("-" * 50)

    print("Processing complete!")