import argparse
import os
import sys
import yaml
import subprocess

from service.analyzer_service import run_analyzer
from service.transpile_service import run_transpiler
from service.modify_service import run_modify_and_create_notebooks
from service.upload_service import UploadService
from service.run_service import RunService
from service.sequential_job_service import SequentialJobService
from service.llm_converter_service import run_llm_converter
from models.analyzer_model import AnalyzerModel
from models.transpile_model import TranspilerModel
from models.modify_model import ModifyNotebookModel
from models.upload_model import UploadModel
from models.llm_converter_model import LLMConverterModel
from models.sequential_job_model import SequentialJobModel
from models.full_config import FullConfigModel
from service.config_service import create_run_model
from service.helper import (
    get_catalog_name, get_schema_name, get_warehouse,
    get_source_directory, get_report_file, get_source_tech,
    get_input_source, get_output_folder, get_error_file_path,
    get_source_dialect, get_validate, get_override, get_open_config,
    validate_catalog_exists, validate_schema_exists, validate_warehouse_exists
)


def validate_and_prompt_config(config: dict) -> dict:
    """Validate config file structure and prompt user for missing details."""
    required_sections = ["analyzer", "transpiler", "modify", "upload", "llm_converter"]
    for section in required_sections:
        if section not in config:
            print(f"❌ Missing required section '{section}' in config")
            sys.exit(1)

    # Validate and prompt for analyzer section
    analyzer_cfg = config["analyzer"]
    if not analyzer_cfg.get("source_directory"):
        print("⚠️  Missing analyzer.source_directory in config")
        analyzer_cfg["source_directory"] = get_source_directory()
    if not analyzer_cfg.get("report_file"):
        print("⚠️  Missing analyzer.report_file in config")
        analyzer_cfg["report_file"] = get_report_file()
    if not analyzer_cfg.get("source_tech"):
        print("⚠️  Missing analyzer.source_tech in config")
        analyzer_cfg["source_tech"] = get_source_tech()

    # Validate and prompt for transpiler section
    transpiler_cfg = config["transpiler"]
    if not transpiler_cfg.get("source_dialect"):
        print("⚠️  Missing transpiler.source_dialect in config")
        transpiler_cfg["source_dialect"] = get_source_dialect()
    if not transpiler_cfg.get("input_source"):
        print("⚠️  Missing transpiler.input_source in config")
        transpiler_cfg["input_source"] = get_input_source()
    if not transpiler_cfg.get("output_folder"):
        print("⚠️  Missing transpiler.output_folder in config")
        transpiler_cfg["output_folder"] = get_output_folder()
    if not transpiler_cfg.get("error_file_path"):
        print("⚠️  Missing transpiler.error_file_path in config")
        transpiler_cfg["error_file_path"] = get_error_file_path()
    
    # Validate and prompt for catalog, schema, and warehouse with workspace validation
    if not transpiler_cfg.get("catalog_name"):
        print("⚠️  Missing transpiler.catalog_name in config")
        transpiler_cfg["catalog_name"] = get_catalog_name()
    else:
        # Validate existing catalog
        if not validate_catalog_exists(transpiler_cfg["catalog_name"]):
            print(f"❌ Catalog '{transpiler_cfg['catalog_name']}' not found in workspace")
            transpiler_cfg["catalog_name"] = get_catalog_name()
    
    if not transpiler_cfg.get("schema_name"):
        print("⚠️  Missing transpiler.schema_name in config")
        transpiler_cfg["schema_name"] = get_schema_name(transpiler_cfg["catalog_name"])
    else:
        # Validate existing schema
        if not validate_schema_exists(transpiler_cfg["catalog_name"], transpiler_cfg["schema_name"]):
            print(f"❌ Schema '{transpiler_cfg['schema_name']}' not found in catalog '{transpiler_cfg['catalog_name']}'")
            transpiler_cfg["schema_name"] = get_schema_name(transpiler_cfg["catalog_name"])
    
    if not transpiler_cfg.get("warehouse"):
        print("⚠️  Missing transpiler.warehouse in config")
        transpiler_cfg["warehouse"] = get_warehouse()
    else:
        # Validate existing warehouse
        if not validate_warehouse_exists(transpiler_cfg["warehouse"]):
            print(f"❌ Warehouse '{transpiler_cfg['warehouse']}' not found in workspace")
            transpiler_cfg["warehouse"] = get_warehouse()
    
    # Set defaults for optional fields if not present
    if not transpiler_cfg.get("validate"):
        transpiler_cfg["validate"] = "false"
    if not transpiler_cfg.get("override"):
        transpiler_cfg["override"] = "yes"
    if not transpiler_cfg.get("open_config"):
        transpiler_cfg["open_config"] = "yes"

    # Validate and prompt for modify section
    modify_cfg = config["modify"]
    if not modify_cfg.get("transpiled_dir"):
        print("⚠️  Missing modify.transpiled_dir in config")
        modify_cfg["transpiled_dir"] = input("Enter transpiled SQL directory [default: ./transpiled]: ").strip() or "./transpiled"
    if not modify_cfg.get("output_dir"):
        print("⚠️  Missing modify.output_dir in config")
        modify_cfg["output_dir"] = input("Enter output directory for notebooks [default: ./transpiled]: ").strip() or "./transpiled"
    if not modify_cfg.get("llm_model"):
        print("⚠️  Missing modify.llm_model in config")
        modify_cfg["llm_model"] = input("Enter LLM model name [default: llama-3.1-8b-instant]: ").strip() or "llama-3.1-8b-instant"
    if not modify_cfg.get("temperature"):
        modify_cfg["temperature"] = "0.6"

    # Validate and prompt for upload section
    upload_cfg = config["upload"]
    if not upload_cfg.get("source_notebook_path"):
        print("⚠️  Missing upload.source_notebook_path in config")
        upload_cfg["source_notebook_path"] = input("Enter path to local file or directory to upload: ").strip()
    if not upload_cfg.get("destination_directory"):
        print("⚠️  Missing upload.destination_directory in config")
        upload_cfg["destination_directory"] = input("Enter Databricks workspace directory (e.g., /Users/you/project): ").strip()

    # Validate and prompt for llm_converter section
    llm_converter_cfg = config["llm_converter"]
    if not llm_converter_cfg.get("source_folder"):
        print("⚠️  Missing llm_converter.source_folder in config")
        llm_converter_cfg["source_folder"] = input("Enter source folder path containing Snowflake SQL files: ").strip()
    if not llm_converter_cfg.get("output_folder"):
        print("⚠️  Missing llm_converter.output_folder in config")
        llm_converter_cfg["output_folder"] = input("Enter output folder for converted SQL files: ").strip()
    if not llm_converter_cfg.get("databricks_notebooks_folder"):
        print("⚠️  Missing llm_converter.databricks_notebooks_folder in config")
        llm_converter_cfg["databricks_notebooks_folder"] = input("Enter folder for Databricks notebooks: ").strip()
    if not llm_converter_cfg.get("llm_model"):
        llm_converter_cfg["llm_model"] = "llama-3.1-8b-instant"
    if not llm_converter_cfg.get("temperature"):
        llm_converter_cfg["temperature"] = "0.1"
    if not llm_converter_cfg.get("catalog_name"):
        llm_converter_cfg["catalog_name"] = "workspace"
    if not llm_converter_cfg.get("schema_name"):
        llm_converter_cfg["schema_name"] = "default"

    return config


def load_config(path: str) -> dict:
    """Load and validate config file from local path."""
    # Validate that path is a local file path
    if not os.path.isabs(path):
        # Convert relative path to absolute path
        path = os.path.abspath(path)
    
    # Check if path exists
    if not os.path.exists(path):
        sys.exit(f"❌ Config file not found: {path}")
    
    # Check if it's a file (not a directory)
    if not os.path.isfile(path):
        sys.exit(f"❌ Path is not a file: {path}")
    
    # Validate file extension
    if not path.lower().endswith(('.yaml', '.yml')):
        sys.exit(f"❌ Config file must be a YAML file (.yaml or .yml): {path}")
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            if not config:
                sys.exit("❌ Config file is empty or invalid YAML")
            print(f"✅ Successfully loaded config from: {path}")
            return config
    except Exception as e:
        sys.exit(f"❌ Failed to load config file {path}: {e}")


def main():
    """
    Main function for Lakebridge CLI Runner.
    
    This function now includes comprehensive validation:
    1. Checks all required configuration details
    2. Prompts user for missing values
    3. Validates catalog, schema, and warehouse existence in Databricks workspace
    4. Ensures all steps have proper configuration before execution
    """
    parser = argparse.ArgumentParser(description="Lakebridge Utilizer CLI Runner")

    parser.add_argument("config", help="Local path to config YAML file (e.g., C:\\Users\\username\\config.yaml)")

    parser.add_argument("--analyzer", choices=["y", "n"], default="n", help="Step1: Run analyzer")
    parser.add_argument("--transpiler", choices=["y", "n"], default="n", help="Step2: Run transpiler")
    parser.add_argument("--modify", choices=["y", "n"], default="n", help="Step3: Run modify ")
    parser.add_argument("--llm_converter", choices=["y", "n"], default="n", help="Step3.5: Run LLM converter (Snowflake to Databricks)")
    parser.add_argument("--upload", choices=["y", "n"], default="n", help="Step4: Run upload ")
    parser.add_argument("--run", choices=["y", "n"], default="n", help="Step5: Run notebooks as a Job ")
    parser.add_argument("--sequential", choices=["y", "n"], default="n", help="Step5.5: Run notebooks as sequential jobs (Tables/Views first, then Functions/Procedures)")

    args = parser.parse_args()

    config = load_config(args.config)
    config = validate_and_prompt_config(config)

    analyzer_cfg = config["analyzer"]
    transpiler_cfg = config["transpiler"]
    modify_cfg_dict = config["modify"]
    llm_converter_cfg_dict = config["llm_converter"]
    upload_cfg_dict = config["upload"]

    analyzer = AnalyzerModel(
        source_directory=analyzer_cfg.get("source_directory", ""),
        report_file=analyzer_cfg.get("report_file", ""),
        source_tech=analyzer_cfg.get("source_tech", ""),
    )

    full_cfg = FullConfigModel(
        analyzer=analyzer,
        transpiler=None,
        reconciler=None,
        catalog_name=transpiler_cfg.get("catalog_name", ""),
        schema_name=transpiler_cfg.get("schema_name", ""),
    )

    uploaded_paths = None

    if args.analyzer == "y":
        print("\n🔍 Running analyzer...")
        run_analyzer(full_cfg.analyzer)

    if args.transpiler == "y":
        print("\n🔄 Running transpiler...")
        transpiler_model = TranspilerModel(
            source_dialect=transpiler_cfg.get("source_dialect", ""),
            input_source=transpiler_cfg.get("input_source", ""),
            output_folder=transpiler_cfg.get("output_folder", ""),
            error_file_path=transpiler_cfg.get("error_file_path", ""),
            catalog_name=full_cfg.catalog_name,
            schema_name=full_cfg.schema_name,
            validate=transpiler_cfg.get("validate", "false"),
            warehouse=transpiler_cfg.get("warehouse", ""),
            override=transpiler_cfg.get("override", "yes"),
            open_config=transpiler_cfg.get("open_config", "yes"),
            llm_model=config.get("modify", {}).get("llm_model", "llama-3.1-8b-instant"),
            temperature=float(config.get("modify", {}).get("temperature", 0.6)),
        )
        run_transpiler(transpiler_model)

    if args.modify == "y":
        print("\n🤖 Running modify...")
        modify_cfg = ModifyNotebookModel(
            transpiled_dir=modify_cfg_dict.get("transpiled_dir", "./transpiled"),
            output_dir=modify_cfg_dict.get("output_dir", "./transpiled"),
            llm_model=modify_cfg_dict.get("llm_model", "llama-3.1-8b-instant"),
            temperature=float(modify_cfg_dict.get("temperature", 0.6)),
        )
        run_modify_and_create_notebooks(modify_cfg, full_cfg.catalog_name, full_cfg.schema_name)

    if args.llm_converter == "y":
        print("\n🔄 Running LLM converter (Snowflake to Databricks)...")
        llm_converter_cfg = LLMConverterModel(
            source_folder=llm_converter_cfg_dict.get("source_folder", ""),
            output_folder=llm_converter_cfg_dict.get("output_folder", ""),
            databricks_notebooks_folder=llm_converter_cfg_dict.get("databricks_notebooks_folder", ""),
            groq_api_key=llm_converter_cfg_dict.get("groq_api_key"),
            llm_model=llm_converter_cfg_dict.get("llm_model", "llama-3.1-8b-instant"),
            temperature=float(llm_converter_cfg_dict.get("temperature", 0.1)),
            max_tokens=int(llm_converter_cfg_dict.get("max_tokens", 4096)),
            catalog_name=llm_converter_cfg_dict.get("catalog_name", "workspace"),
            schema_name=llm_converter_cfg_dict.get("schema_name", "default"),
            preserve_structure=llm_converter_cfg_dict.get("preserve_structure", True),
            create_backup=llm_converter_cfg_dict.get("create_backup", True)
        )
        result = run_llm_converter(llm_converter_cfg)
        
        if result.get("success"):
            print(f"\n✅ LLM conversion completed successfully!")
            print(f"📊 Summary: {result.get('summary', {})}")
        else:
            print(f"\n❌ LLM conversion failed: {result.get('error', 'Unknown error')}")

    if args.upload == "y":
        print("\n📤 Running upload...")
        upload_cfg = UploadModel(
            source_notebook_path=upload_cfg_dict.get("source_notebook_path", ""),
            destination_directory=upload_cfg_dict.get("destination_directory", ""),
        )
        uploaded_paths = UploadService(upload_cfg).upload()

    if args.run == "y":
        if uploaded_paths:
            print(f"\n🚀 Running {len(uploaded_paths)} uploaded files in Databricks...")
            run_cfg = create_run_model("")
            run_service = RunService(run_cfg)
            run_id = run_service.run_in_databricks(uploaded_paths)

            if run_id:
                print(f"\n✅ Successfully created and started job in Databricks")
                print(f"Job Run ID: {run_id}")
                print(f"Contains {len(uploaded_paths)} individual tasks")
            else:
                print("\n❌ Failed to create or run job.")
        else:
            print("\n⚠️  No uploaded files found. Please run upload step first.")

    if args.sequential == "y":
        if uploaded_paths:
            print(f"\n🚀 Running {len(uploaded_paths)} uploaded files as sequential jobs in Databricks...")
            run_cfg = create_run_model("")
            sequential_cfg = SequentialJobModel()
            sequential_service = SequentialJobService(run_cfg, sequential_cfg)
            job_results = sequential_service.create_sequential_jobs(uploaded_paths)

            if job_results.get("first_job", {}).get("run_id") or job_results.get("second_job", {}).get("run_id"):
                print(f"\n✅ Successfully created sequential jobs in Databricks")
                
                # Display job information
                if job_results.get("first_job", {}).get("run_id"):
                    print(f"First Job (Tables/Views) - Run ID: {job_results['first_job']['run_id']}")
                    print(f"  Contains {len(job_results['categorized_paths']['tables_views'])} notebooks")
                
                if job_results.get("second_job", {}).get("run_id"):
                    print(f"Second Job (Functions/Procedures) - Run ID: {job_results['second_job']['run_id']}")
                    print(f"  Contains {len(job_results['categorized_paths']['functions_procedures'])} notebooks")
                    print(f"  Depends on first job completion")
                
                # Optional: Monitor job completion
                monitor = input("\nDo you want to monitor job completion? (y/n): ").strip().lower()
                if monitor == 'y':
                    final_status = sequential_service.wait_for_completion(job_results, timeout_minutes=60)
                    print(f"\n📊 Final job status:")
                    print(f"  First job: {final_status.get('first_job_status', 'unknown')}")
                    print(f"  Second job: {final_status.get('second_job_status', 'unknown')}")
            else:
                print("\n❌ Failed to create sequential jobs.")
        else:
            print("\n⚠️  No uploaded files found. Please run upload step first.")


if __name__ == "__main__":
    main()