import subprocess
from models.transpile_model import TranspilerModel


def run_transpiler(transpiler: TranspilerModel, profile: str = "DEFAULT") -> None:
    """
    Run Databricks Lakebridge transpiler with correct flags.
    """

    command = [
        "databricks", "labs", "lakebridge", "transpile",
        "--input-source", transpiler.input_source,
        "--output-folder", transpiler.output_folder,
        "--error-file-path", transpiler.error_file_path,
        "--catalog-name", transpiler.catalog_name,
        "--schema-name", transpiler.schema_name,
        "--skip-validation", transpiler.validate,
    ]
    print("Running Transpiler with command:")
    print(" ".join(command))

    try:
        subprocess.run(command, check=True)
        print(f"Transpiler completed. Output generated at {transpiler.output_folder}")
    except subprocess.CalledProcessError as e:
        print(f"Transpiler failed: {e}")
    except FileNotFoundError:
        print("Databricks CLI not found. Please install and configure it first.")
