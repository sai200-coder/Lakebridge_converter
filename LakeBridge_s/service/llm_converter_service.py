# Databricks notebook source
import os
import shutil
import json
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
from groq import Groq
import nbformat
from nbformat.v4 import new_notebook, new_code_cell, new_markdown_cell

from models.llm_converter_model import LLMConverterModel
from service.sql_parser import SnowflakeSQLParser, SQLObject, SQLObjectType


class LLMConverterService:
    """Service for converting Snowflake SQL to Databricks SQL using LLM."""
    
    def __init__(self, config: LLMConverterModel):
        """Initialize the LLMConverter service."""
        self.config = config
        self.parser = SnowflakeSQLParser()
        self.groq_client = None
        # Keep track of created table notebooks so INSERTs can be appended
        self._table_notebook_by_name: Dict[str, str] = {}
        self._setup_groq_client()
        
    def _setup_groq_client(self):
        """Setup Groq client with API key."""
        load_dotenv()
        
        api_key = self.config.groq_api_key or os.getenv('GROQ_API_KEY')
        if not api_key:
            raise ValueError("Groq API key not found. Set GROQ_API_KEY environment variable or provide in config.")
        
        self.groq_client = Groq(api_key=api_key)
        print("✅ Groq client initialized successfully")
    
    def convert_folder(self) -> Dict[str, any]:
        """
        Main method to convert all SQL files in a folder.
        
        Returns:
            Dictionary with conversion results and statistics
        """
        print(f"🚀 Starting LLM conversion process...")
        print(f"📁 Source folder: {self.config.source_folder}")
        print(f"📁 Output folder: {self.config.output_folder}")
        print(f"📁 Databricks notebooks: {self.config.databricks_notebooks_folder}")
        
        # Step 1: Parse SQL files
        print("\n🔍 Step 1: Parsing SQL files...")
        parsed_objects = self.parser.parse_folder(self.config.source_folder)
        
        total_objects = sum(len(objects) for objects in parsed_objects.values())
        if total_objects == 0:
            print("❌ No SQL objects found to convert")
            return {"success": False, "message": "No SQL objects found"}
        
        print(f"✅ Found {total_objects} SQL objects across {len([f for f in os.listdir(self.config.source_folder) if f.endswith('.sql')])} files")
        
        # Step 2: Create output directories
        print("\n📁 Step 2: Creating output directories...")
        self._create_output_directories()
        
        # Step 3: Convert and save objects
        print("\n🤖 Step 3: Converting SQL objects...")
        conversion_results = self._convert_objects(parsed_objects)
        
        # Step 4: Generate summary
        print("\n📊 Step 4: Generating conversion summary...")
        summary = self._generate_summary(conversion_results)
        
        print(f"\n✅ Conversion completed successfully!")
        print(f"📈 Converted {conversion_results['successful']} out of {conversion_results['total']} objects")
        
        return {
            "success": True,
            "summary": summary,
            "conversion_results": conversion_results,
            "output_folders": {
                "sql_objects": self.config.output_folder,
                "databricks_notebooks": self.config.databricks_notebooks_folder
            }
        }
    
    def _create_output_directories(self):
        """Create the necessary output directory structure."""
        directories = [
            self.config.output_folder,
            self.config.databricks_notebooks_folder
        ]
        
        # Create subdirectories for each object type
        for base_dir in directories:
            for obj_type in SQLObjectType:
                # For output_folder we keep originals under a nested 'original' directory
                if base_dir == self.config.output_folder:
                    dir_path = os.path.join(base_dir, obj_type.value, "original")
                else:
                    dir_path = os.path.join(base_dir, obj_type.value)
                os.makedirs(dir_path, exist_ok=True)
        
        print("✅ Output directories created successfully")
    
    def _convert_objects(self, parsed_objects: Dict[SQLObjectType, List[SQLObject]]) -> Dict[str, any]:
        """Convert all parsed SQL objects using LLM."""
        results = {
            "total": 0,
            "successful": 0,
            "failed": 0,
            "conversions": []
        }
        
        for obj_type, objects in parsed_objects.items():
            if not objects:
                continue
                
            print(f"\n🔄 Converting {len(objects)} {obj_type.value}...")
            
            for obj in objects:
                results["total"] += 1
                
                try:
                    # Always save the original SQL to file first
                    original_sql_path = self._save_original_sql_object(obj)
                    
                    # Convert SQL using LLM
                    converted_sql = self._convert_sql_with_llm(obj)
                    
                    if converted_sql:
                        # For INSERT statements, append to the corresponding table notebook
                        if obj.object_type == SQLObjectType.INSERT:
                            notebook_path = self._append_insert_to_table_notebook(obj, converted_sql)
                        elif obj.object_type == SQLObjectType.TABLE:
                            notebook_path = self._upsert_table_notebook(obj, converted_sql)
                            self._table_notebook_by_name[obj.name.lower()] = notebook_path
                        else:
                            # Create Databricks notebook including original SQL as markdown
                            notebook_path = self._create_databricks_notebook(obj, converted_sql)
                        
                        results["successful"] += 1
                        results["conversions"].append({
                            "object_name": obj.name,
                            "object_type": obj.object_type.value,
                            "original_sql_path": original_sql_path,
                            "notebook_path": notebook_path,
                            "status": "success"
                        })
                        
                        print(f"  ✅ {obj.name} -> {os.path.basename(notebook_path)}")
                    else:
                        results["failed"] += 1
                        results["conversions"].append({
                            "object_name": obj.name,
                            "object_type": obj.object_type.value,
                            "status": "failed",
                            "error": "LLM conversion returned empty result"
                        })
                        print(f"  ❌ {obj.name} - LLM conversion failed")
                        
                except Exception as e:
                    results["failed"] += 1
                    results["conversions"].append({
                        "object_name": obj.name,
                        "object_type": obj.object_type.value,
                        "status": "failed",
                        "error": str(e)
                    })
                    print(f"  ❌ {obj.name} - Error: {str(e)}")
        
        return results
    
    def _convert_sql_with_llm(self, sql_obj: SQLObject) -> Optional[str]:
        """Convert SQL object using Groq LLM."""
        
        # Create conversion prompt based on object type
        prompt = self._create_conversion_prompt(sql_obj)
        
        try:
            response = self.groq_client.chat.completions.create(
                model=self.config.llm_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config.temperature,
                max_completion_tokens=self.config.max_tokens,
                top_p=0.95,
                stream=False,
                stop=None
            )
            
            converted_sql = response.choices[0].message.content.strip()
            
            # Clean up the response
            converted_sql = self._clean_llm_response(converted_sql)
            
            return converted_sql if converted_sql else None
            
        except Exception as e:
            print(f"    ⚠️ LLM API error for {sql_obj.name}: {str(e)}")
            return None
    
    def _create_conversion_prompt(self, sql_obj: SQLObject) -> str:
        """Create a conversion prompt tailored to the SQL object type."""
        
        base_prompt = f"""You are an expert in SQL migration from Snowflake to Databricks. Convert the following Snowflake SQL {sql_obj.object_type.value} to Databricks SQL.

CRITICAL REQUIREMENTS:
1. Convert ALL Snowflake-specific features to Databricks equivalents.
2. Ensure the converted SQL runs successfully in Databricks without errors.
3. Preserve the original logic, functionality, and comments.
4. Use proper Databricks SQL syntax and functions.
5. Handle multi-line statements, BEGIN...END blocks, and dollar-quoted strings correctly.

CONVERSION RULES:

- Use full three-level table names: {self.config.catalog_name}.{self.config.schema_name}.table_name

- Data Type Conversion:
  * NVARCHAR, VARCHAR(n), VARCHAR, TEXT, CHAR(n) → STRING
  * NUMBER(p,s), DECIMAL(p,s), NUMERIC(p,s) → DECIMAL(p,s)
  * FLOAT, FLOAT4, FLOAT8, DOUBLE, REAL → DOUBLE
  * INTEGER, INT, BIGINT, SMALLINT → BIGINT or INT
  * BOOLEAN → BOOLEAN
  * DATE → DATE
  * TIME → STRING
  * TIMESTAMP_NTZ, TIMESTAMP_LTZ, TIMESTAMP_TZ, TIMESTAMP → TIMESTAMP
  * VARIANT → STRING
  * OBJECT → STRING
  * ARRAY → ARRAY<STRING>

- Identity / Auto-increment Columns:
  * Use BIGINT GENERATED ALWAYS/BY DEFAULT AS IDENTITY (INT not supported)
  * Ensure proper syntax: parentheses, commas, comments closed

- Computed / Generated Columns:
  * Remove VIRTUAL/STORED keywords
  * Ensure column type matches expression result
  * Cast expressions if needed, e.g., CAST(quantity * price - discount AS DECIMAL(14,2))

- Replace Snowflake DDL with Databricks equivalents:
  * CREATE OR REPLACE DATABASE → CREATE DATABASE IF NOT EXISTS
  * CREATE OR REPLACE SCHEMA → CREATE SCHEMA IF NOT EXISTS
  * CREATE OR REPLACE TABLE → CREATE TABLE IF NOT EXISTS
  * CREATE OR REPLACE VIEW → CREATE VIEW IF NOT EXISTS
  * CREATE OR REPLACE FUNCTION → CREATE FUNCTION IF NOT EXISTS
  * CREATE OR REPLACE PROCEDURE → Convert to Python UDF if needed

- Convert functions:
  * GETDATE(), NOW(), SYSDATETIME() → current_timestamp()
  * ISNULL(a,b) → coalesce(a,b)
  * LEN(x) → length(x)
  * FLATTEN() → explode()
  * OBJECT_KEYS() → map_keys()
  * PARSE_JSON() → from_json()
  * TO_JSON() → to_json()

- Remove unsupported features:
  * PRIMARY KEY, FOREIGN KEY, UNIQUE, DEFAULT, CHECK constraints
  * SEQUENCE objects (replace with IDENTITY columns)
  * Snowflake-only functions not available in Databricks

- For stored procedures:
  * Convert JavaScript procedures → Python UDFs
  * Convert SQL procedures → Python functions
  * Show how to register as PySpark UDF if applicable
  * Provide example usage

- For INSERT statements:
  * Preserve all INSERT INTO statements exactly as they are
  * Convert table names to use full three-level naming: {self.config.catalog_name}.{self.config.schema_name}.table_name
  * Convert data types in VALUES clauses according to the data type conversion rules above
  * Convert function calls in VALUES clauses (e.g., CURRENT_TIMESTAMP → current_timestamp())
  * Preserve all data and maintain the same logical structure

- Preserve:
  * Multi-line statements
  * BEGIN...END blocks (convert to Python if needed)
  * Dollar-quoted strings → Python triple quotes
  * Delta Lake syntax: USING DELTA after column definitions

OBJECT TYPE: {sql_obj.object_type.value.upper()}
OBJECT NAME: {sql_obj.name}

SNOWFLAKE SQL:
{sql_obj.sql_content}

Convert this to **Databricks SQL** ensuring it will run without errors."""
    
        return base_prompt
    
    def _clean_llm_response(self, response: str) -> str:
        """Clean up the LLM response to extract pure SQL."""
        
        if not response:
            return ""
        
        # Remove markdown code fences
        if "```sql" in response:
            start = response.find("```sql") + 6
            end = response.find("```", start)
            if end != -1:
                response = response[start:end].strip()
        elif "```" in response:
            start = response.find("```") + 3
            end = response.find("```", start)
            if end != -1:
                response = response[start:end].strip()
        
        # Remove any leading/trailing whitespace
        response = response.strip()
        
        return response
    
    def _save_sql_object(self, sql_obj: SQLObject, converted_sql: str) -> str:
        """Save the converted SQL object to a file."""
        
        # Create filename
        safe_name = self._sanitize_filename(sql_obj.name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{safe_name}_{timestamp}.sql"
        
        # Create file path
        file_path = os.path.join(
            self.config.output_folder,
            sql_obj.object_type.value,
            filename
        )
        
        # Write the file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(converted_sql)
        
        return file_path
    
    def _create_databricks_notebook(self, sql_obj: SQLObject, converted_sql: str) -> str:
        """Create a Databricks notebook for the converted SQL object."""
        
        # Create notebook
        notebook = new_notebook()
        
        # Attach structured metadata (production-safe; no executable content)
        notebook.metadata["lakebridge"] = {
            "object_name": sql_obj.name,
            "object_type": sql_obj.object_type.value,
            "source_file": os.path.basename(sql_obj.file_path),
            "converted_at": datetime.now().isoformat(),
            "categories": [sql_obj.object_type.value],
            "description": f"Databricks conversion of Snowflake {sql_obj.object_type.value} {sql_obj.name}.",
            "dependencies": sql_obj.dependencies or [],
            # Store original SQL in metadata to avoid code cell execution issues
            "original_sql": sql_obj.sql_content,
        }
        
        # Add metadata cell (Cell 0)
        metadata_cell = new_markdown_cell(f"""# {sql_obj.name} ({sql_obj.object_type.value.title()})

**Table name:** {sql_obj.name}  
**Original file name:** {os.path.basename(sql_obj.file_path)}  
**Object type:** {sql_obj.object_type.value}  
**Converted timestamp:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source system:** Snowflake → Databricks

## Original code
```sql
{sql_obj.sql_content}
```
""")
        notebook.cells.append(metadata_cell)
        
        # Add converted content based on object type
        if sql_obj.object_type in [SQLObjectType.PROCEDURE, SQLObjectType.FUNCTION]:
            # For procedures and functions, create Python cells
            self._add_procedure_notebook_cells(notebook, sql_obj, converted_sql)
        else:
            # For other objects, create SQL cells
            self._add_sql_notebook_cells(notebook, converted_sql)
        
        # Save notebook (stable filename for tables to avoid duplicates)
        safe_name = self._sanitize_filename(sql_obj.name)
        if sql_obj.object_type == SQLObjectType.TABLE:
            filename = f"{safe_name}.ipynb"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_name}_{timestamp}.ipynb"
        
        file_path = os.path.join(
            self.config.databricks_notebooks_folder,
            sql_obj.object_type.value,
            filename
        )
        
        with open(file_path, 'w', encoding='utf-8') as f:
            nbformat.write(notebook, f)
        
        return file_path

    def _upsert_table_notebook(self, sql_obj: SQLObject, converted_sql: str) -> str:
        """Create or append the table DDL into a single stable notebook for the table."""
        safe_name = self._sanitize_filename(sql_obj.name)
        file_path = os.path.join(
            self.config.databricks_notebooks_folder,
            SQLObjectType.TABLE.value,
            f"{safe_name}.ipynb",
        )

        if os.path.exists(file_path):
            # Append DDL as a new section
            notebook = nbformat.read(file_path, as_version=4)
            header_md = new_markdown_cell(f"## Table definition for `{sql_obj.name}`")
            sql_cell = new_code_cell(f"%sql\n{converted_sql}")
            notebook.cells.extend([header_md, sql_cell])
            nbformat.write(notebook, file_path)
            return file_path

        # Create fresh notebook with metadata and initial DDL
        notebook = new_notebook()
        notebook.metadata["lakebridge"] = {
            "object_name": sql_obj.name,
            "object_type": SQLObjectType.TABLE.value,
            "source_file": os.path.basename(sql_obj.file_path),
            "converted_at": datetime.now().isoformat(),
            "categories": [SQLObjectType.TABLE.value],
            "description": f"Databricks conversion of Snowflake table {sql_obj.name}.",
            "dependencies": sql_obj.dependencies or [],
            "original_sql": sql_obj.sql_content,
        }

        metadata_cell = new_markdown_cell(f"""# {sql_obj.name} (Table)

**Table name:** {sql_obj.name}  
**Original file name:** {os.path.basename(sql_obj.file_path)}  
**Object type:** {SQLObjectType.TABLE.value}  
**Converted timestamp:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Source system:** Snowflake → Databricks

## Original code
```sql
{sql_obj.sql_content}
```
""")
        notebook.cells.append(metadata_cell)

        self._add_sql_notebook_cells(notebook, converted_sql)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            nbformat.write(notebook, f)

        return file_path

    def _append_insert_to_table_notebook(self, sql_obj: SQLObject, converted_sql: str) -> str:
        """Append INSERT SQL to the relevant table notebook. Create a minimal table notebook if missing."""
        table_name_key = sql_obj.name.lower()
        notebook_path = self._table_notebook_by_name.get(table_name_key)

        if not notebook_path:
            # Try to find an existing notebook on disk
            notebook_path = self._find_existing_table_notebook(sql_obj.name)
            if notebook_path:
                self._table_notebook_by_name[table_name_key] = notebook_path

        if not notebook_path:
            # If still not found (edge case: INSERT appears before TABLE), create a minimal table notebook
            placeholder_obj = SQLObject(
                name=sql_obj.name,
                object_type=SQLObjectType.TABLE,
                sql_content=f"-- Placeholder for table {sql_obj.name}. Original DDL not found at generation time.",
                start_line=sql_obj.start_line,
                end_line=sql_obj.end_line,
                file_path=sql_obj.file_path,
                dependencies=[],
            )
            notebook_path = self._create_databricks_notebook(placeholder_obj, converted_sql="")
            self._table_notebook_by_name[table_name_key] = notebook_path

        # Load notebook, append an INSERT section and save
        with open(notebook_path, 'r', encoding='utf-8') as f:
            notebook = nbformat.read(f, as_version=4)

        # Add a small header then the SQL cell
        header_md = new_markdown_cell(f"## INSERT statements for `{sql_obj.name}`")
        sql_cell = new_code_cell(f"%sql\n{converted_sql}")
        notebook.cells.extend([header_md, sql_cell])

        with open(notebook_path, 'w', encoding='utf-8') as f:
            nbformat.write(notebook, f)

        return notebook_path

    def _find_existing_table_notebook(self, table_name: str) -> Optional[str]:
        """Search the tables notebooks directory for a notebook matching the table name prefix."""
        tables_dir = os.path.join(self.config.databricks_notebooks_folder, SQLObjectType.TABLE.value)
        if not os.path.isdir(tables_dir):
            return None

        safe_prefix = self._sanitize_filename(table_name)
        try:
            for fname in os.listdir(tables_dir):
                if fname.lower().startswith(safe_prefix.lower()) and fname.lower().endswith('.ipynb'):
                    return os.path.join(tables_dir, fname)
        except Exception:
            return None
        return None
    
    def _add_procedure_notebook_cells(self, notebook, sql_obj: SQLObject, converted_sql: str):
        """Add cells for procedure/function conversion to Python."""
        
        # Add explanation cell
        explanation_cell = new_markdown_cell(f"""## Converted Python
""")
        notebook.cells.append(explanation_cell)
        
        # Add Python code cell
        python_cell = new_code_cell(converted_sql)
        notebook.cells.append(python_cell)
        
        # Add usage example cell
        usage_cell = new_markdown_cell("""## Usage Example

```python
# Example usage of the converted function
# Replace with actual parameters as needed
result = your_function_name(parameter1, parameter2)
print(result)
```""")
        notebook.cells.append(usage_cell)
    
    def _add_sql_notebook_cells(self, notebook, converted_sql: str):
        """Add SQL cells for regular SQL objects."""
        
        # Split SQL into logical blocks if it's long
        sql_blocks = self._split_sql_into_blocks(converted_sql)
        
        for i, block in enumerate(sql_blocks):
            if block.strip():
                # Add explanation for complex blocks
                if len(sql_blocks) > 1:
                    explanation_cell = new_markdown_cell(f"## SQL Block {i + 1}")
                    notebook.cells.append(explanation_cell)
                
                # Add SQL cell
                sql_cell = new_code_cell(f"%sql\n{block}")
                notebook.cells.append(sql_cell)

    def _save_original_sql_object(self, sql_obj: SQLObject) -> str:
        """Save the original SQL object to a file (no converted .sql files)."""
        
        # Create filename
        safe_name = self._sanitize_filename(sql_obj.name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{safe_name}_{timestamp}.sql"
        
        # Create file path under 'original' subfolder
        file_path = os.path.join(
            self.config.output_folder,
            sql_obj.object_type.value,
            "original",
            filename
        )
        
        # Write the original SQL
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(sql_obj.sql_content)
        
        return file_path
    
    def _split_sql_into_blocks(self, sql: str) -> List[str]:
        """Split SQL into logical blocks for better notebook organization."""
        
        # Simple splitting by major statements
        statements = []
        current_statement = []
        in_string = False
        escape_next = False
        
        lines = sql.split('\n')
        
        for line in lines:
            current_statement.append(line)
            
            # Check for string literals
            for char in line:
                if escape_next:
                    escape_next = False
                    continue
                if char == '\\':
                    escape_next = True
                    continue
                if char in ["'", '"']:
                    in_string = not in_string
                    continue
                if char == ';' and not in_string:
                    # End of statement
                    statement = '\n'.join(current_statement).strip()
                    if statement:
                        statements.append(statement)
                    current_statement = []
                    break
        
        # Add any remaining statement
        if current_statement:
            statement = '\n'.join(current_statement).strip()
            if statement:
                statements.append(statement)
        
        return statements if statements else [sql]
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe file system usage."""
        
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove leading/trailing dots and spaces
        filename = filename.strip('. ')
        
        # Ensure it's not empty
        if not filename:
            filename = "unnamed"
        
        # Limit length
        if len(filename) > 100:
            filename = filename[:100]
        
        return filename
    
    def _generate_summary(self, conversion_results: Dict[str, any]) -> Dict[str, any]:
        """Generate a summary of the conversion process."""
        
        summary = {
            "total_objects": conversion_results["total"],
            "successful_conversions": conversion_results["successful"],
            "failed_conversions": conversion_results["failed"],
            "success_rate": (conversion_results["successful"] / conversion_results["total"] * 100) if conversion_results["total"] > 0 else 0,
            "object_type_breakdown": {},
            "timestamp": datetime.now().isoformat(),
            "config": {
                "source_folder": self.config.source_folder,
                "output_folder": self.config.output_folder,
                "databricks_notebooks_folder": self.config.databricks_notebooks_folder,
                "llm_model": self.config.llm_model,
                "temperature": self.config.temperature
            }
        }
        
        # Count by object type
        for conversion in conversion_results["conversions"]:
            obj_type = conversion["object_type"]
            if obj_type not in summary["object_type_breakdown"]:
                summary["object_type_breakdown"][obj_type] = {"total": 0, "successful": 0, "failed": 0}
            
            summary["object_type_breakdown"][obj_type]["total"] += 1
            if conversion["status"] == "success":
                summary["object_type_breakdown"][obj_type]["successful"] += 1
            else:
                summary["object_type_breakdown"][obj_type]["failed"] += 1
        
        # Save summary to file
        summary_path = os.path.join(self.config.output_folder, "conversion_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        print(f"📄 Summary saved to: {summary_path}")
        
        return summary


def run_llm_converter(config: LLMConverterModel) -> Dict[str, any]:
    """
    Run the LLM converter service.
    
    Args:
        config: LLMConverterModel configuration
        
    Returns:
        Dictionary with conversion results
    """
    try:
        service = LLMConverterService(config)
        return service.convert_folder()
    except Exception as e:
        print(f"❌ Error running LLM converter: {str(e)}")
        return {"success": False, "error": str(e)}