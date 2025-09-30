# from models.test_model import TestModel

from models.analyzer_model import AnalyzerModel
from models.transpile_model import TranspilerModel
from models.reconcile_model import ReconcilerModel
import subprocess


def get_source_directory():
    while True:
        source_directory = input("Enter your source directory path (e.g., /path/to/source): ")
        if source_directory:
            return source_directory
        else:
            print("Source directory cannot be empty. Please try again.")

def get_report_file():
    while True:
        report_file = input("Enter your report file name (e.g., analysis_report.xlsx): ")
        if report_file.endswith('.xlsx'):
            return report_file
        else:
            print("Invalid file format. Please enter a valid .xlsx file.")
            
def get_source_tech():
    valid_options = {
        "0": "abinitio",
        "1": "adf",
        "2": "alteryx",
        "3": "athena",
        "4": "bigquery",
        "5": "bods",
        "6": "cloudera-impala",
        "7": "datastage",
        "8": "greenplum",
        "9": "hive",
        "10": "ibmdb2",
        "11": "informatica-bde",         
        "12": "informatica-pc",
        "13": "informatica-cloud",
        "14": "MS SQL Server",
        "15": "netezza",
        "16": "oozie",
        "17": "oracle",
        "18": "odi",                     
        "19": "pentahodi",
        "20": "pig",
        "21": "presto",
        "22": "pyspark",
        "23": "redshift",
        "24": "saphana-calcviews",
        "25": "sas",
        "26": "snowflake",
        "27": "spss",
        "28": "sqoop",
        "29": "ssis",
        "30": "ssrs",
        "31": "synapse",
        "32": "talend",
        "33": "teradata",
        "34": "vertica",
    }

    print("Select the source technology:")
    for k, v in valid_options.items():
        print(f"[{k}] {v}")

    choice = input("Enter a number between 0 and 34: ").strip()
    if choice in valid_options:
        return valid_options[choice]
    else:
        print(f"Invalid choice. Valid numbers are 0–34.")
        return get_source_tech()


# transpiler config
def get_input_source():
    while True:
        val = input("Enter your input source directory path (e.g., ./sql_scripts): ")
        if val:
            return val
        else:
            print("Input source cannot be empty. Please try again.")

def get_output_folder():
    while True:
        val = input("Enter your output folder path (e.g., ./transpiled_code): ")
        if val:
            return val
        else:
            print("Output folder cannot be empty. Please try again.")

def get_error_file_path():
    while True:
        val = input("Enter your error file path (e.g., ./errors.log): ")
        if val:
            return val
        else:
            print("Error file path cannot be empty. Please try again.")

def get_catalog_name():
    while True:
        val = input("Enter your catalog name (e.g., my_catalog): ").strip()
        if not val:
            print("Catalog name cannot be empty. Please try again.")
            continue
            
        # Validate catalog exists in Databricks using centralized function
        if validate_catalog_exists(val):
            return val
        else:
            # Get available catalogs for better error message
            try:
                result = subprocess.run(
                    ['databricks', 'catalogs', 'list'],
                    capture_output=True, text=True, check=True
                )
                catalogs = []
                for line in result.stdout.splitlines():
                    if line.startswith('---') or not line.strip():
                        continue
                    parts = line.split()
                    if parts:
                        catalogs.append(parts[0])
                available = ', '.join(catalogs) if catalogs else 'none'
                print(f"Catalog '{val}' not found. Available catalogs: {available}")
            except subprocess.CalledProcessError:
                print("Please try again with a valid catalog name.")

def get_schema_name(catalog_name):
    while True:
        val = input(f"Enter your schema name in catalog '{catalog_name}' (e.g., my_schema): ").strip()
        if not val:
            print("Schema name cannot be empty. Please try again.")
            continue
            
        # Validate schema exists in the catalog using centralized function
        if validate_schema_exists(catalog_name, val):
            return val
        else:
            # Get available schemas for better error message
            try:
                result = subprocess.run(
                    ['databricks','schemas','list',catalog_name],
                    capture_output=True, text=True, check=True
                )
                schemas = [line.split(maxsplit=1)[0].replace(f'{catalog_name}.', '') for line in result.stdout.splitlines()[1:] if line.strip()]
                print(f"Schema '{val}' not found in catalog '{catalog_name}'. Please try again. Available are '{schemas}'")
            except subprocess.CalledProcessError:
                print("Error: Failed to list schemas. Check catalog name or Databricks CLI configuration.")
def get_validate():
    while True:
        val = input("Do you want to validate the transpiled code? (true/false) [default:  ]: ").strip().lower()
        if val in ["true", "false", ""]:
            return val if val else "false"
        else:
            print("Invalid input. Please enter 'true' or 'false'.")
def get_warehouse():
    while True:
        val = input("Enter your warehouse ID (e.g., 0123456789abcde0) [default: 1]: ").strip()
        if val == "":
            return "1"
            
        # Validate warehouse exists using centralized function
        if validate_warehouse_exists(val):
            return val
        else:
            # Get available warehouses for better error message
            try:
                result = subprocess.run(
                    ['databricks','warehouses', 'list'],
                    capture_output=True, text=True, check=True
                )
                warehouses = []
                for line in result.stdout.splitlines():
                    if line.startswith('---') or not line.strip() or line.startswith('ID '):
                        continue
                    parts = line.split()
                    if parts:
                        warehouses.append(parts[0])
                available = ', '.join(warehouses) if warehouses else 'none'
                print(f"Warehouse ID '{val}' not found. Available warehouses: {available}")
            except subprocess.CalledProcessError:
                print("Error: Failed to list warehouses. Please try again with a valid warehouse ID.")
def get_override():
    while True:
        val = input("Do you want to override existing files? (yes/no) [default: yes]: ").strip().lower()
        if val in ["yes", "no", ""]:
            return val if val else "yes"
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
def get_open_config():
    while True:
        val = input("Do you want to open the config file after generation? (yes/no) [default: yes]: ").strip().lower()
        if val in ["yes", "no", ""]:
            return val if val else "yes"
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")
            

def get_source_dialect():
    dialects = {
        "0": "Set it later",
        "1": "datastage",
        "2": "informatica (desktop edition)",
        "3": "informatica cloud",
        "4": "mssql",
        "5": "netezza",
        "6": "oracle",
        "7": "snowflake",
        "8": "synapse",
        "9": "teradata",
        "10": "tsql"
    }

    while True:
        print("\nSelect the source dialect:")
        for key, value in dialects.items():
            print(f"[{key}] {value}")

        choice = input("Enter a number between 0 and 10: ").strip()

        if choice in dialects:
            return dialects[choice]   
        else:
            print("Invalid choice. Please enter a number between 0 and 10.")






# reconciler config
def get_profile_name():
    while True:
        val = input("Enter your profile name (e.g., my_profile): ")
        if val:
            return val
        else:
            print("Profile name cannot be empty. Please try again.")

def get_target():
    while True:
        val = input("Enter your target (e.g., dev): ")
        if val:
            return val
        else:
            print("Target cannot be empty. Please try again.")


def validate_catalog_exists(catalog_name: str) -> bool:
    """Validate if catalog exists in Databricks workspace."""
    try:
        result = subprocess.run(
            ['databricks', 'catalogs', 'list'],
            capture_output=True, text=True, check=True
        )
        # Parse output: skip header lines and get first column
        catalogs = []
        for line in result.stdout.splitlines():
            if line.startswith('---') or not line.strip():
                continue
            parts = line.split()
            if parts:
                catalogs.append(parts[0])
        return catalog_name in catalogs
    except subprocess.CalledProcessError:
        print("❌ Error: Failed to list catalogs. Check Databricks CLI configuration.")
        return False


def validate_schema_exists(catalog_name: str, schema_name: str) -> bool:
    """Validate if schema exists in the specified catalog."""
    try:
        result = subprocess.run(
            ['databricks', 'schemas', 'list', catalog_name],
            capture_output=True, text=True, check=True
        )
        schemas = [line.split(maxsplit=1)[0].replace(f'{catalog_name}.', '') 
                  for line in result.stdout.splitlines()[1:] if line.strip()]
        return schema_name in schemas
    except subprocess.CalledProcessError:
        print(f"❌ Error: Failed to list schemas in catalog '{catalog_name}'. Check catalog name or Databricks CLI configuration.")
        return False


def validate_warehouse_exists(warehouse_id: str) -> bool:
    """Validate if warehouse exists in Databricks workspace."""
    try:
        result = subprocess.run(
            ['databricks', 'warehouses', 'list'],
            capture_output=True, text=True, check=True
        )
        # Parse output: skip headers and get first column
        warehouses = []
        for line in result.stdout.splitlines():
            if line.startswith('---') or not line.strip() or line.startswith('ID '):
                continue
            parts = line.split()
            if parts:
                warehouses.append(parts[0])
        return warehouse_id in warehouses
    except subprocess.CalledProcessError:
        print("❌ Error: Failed to list warehouses. Check Databricks CLI configuration.")
        return False


# LLM Converter helper functions
def get_llm_converter_source_folder():
    """Get source folder for LLM converter."""
    while True:
        val = input("Enter source folder path containing Snowflake SQL files: ").strip()
        if val and os.path.exists(val):
            return val
        else:
            print("Invalid or non-existent folder path. Please try again.")

def get_llm_converter_output_folder():
    """Get output folder for converted SQL files."""
    while True:
        val = input("Enter output folder for converted SQL files: ").strip()
        if val:
            return val
        else:
            print("Output folder cannot be empty. Please try again.")

def get_llm_converter_notebooks_folder():
    """Get folder for Databricks notebooks."""
    while True:
        val = input("Enter folder for Databricks notebooks: ").strip()
        if val:
            return val
        else:
            print("Notebooks folder cannot be empty. Please try again.")

def get_llm_converter_model():
    """Get LLM model for conversion."""
    val = input("Enter LLM model name [default: llama-3.1-8b-instant]: ").strip()
    return val if val else "llama-3.1-8b-instant"

def get_llm_converter_temperature():
    """Get temperature for LLM conversion."""
    while True:
        val = input("Enter temperature (0.0 - 1.0) [default: 0.1]: ").strip()
        if val == "":
            return 0.1
        try:
            temp = float(val)
            if 0.0 <= temp <= 1.0:
                return temp
            else:
                print("Temperature must be between 0.0 and 1.0. Please try again.")
        except ValueError:
            print("Invalid temperature value. Please enter a number between 0.0 and 1.0.")