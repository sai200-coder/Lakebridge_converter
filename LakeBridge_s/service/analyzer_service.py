import subprocess
from models.analyzer_model import AnalyzerModel


def run_analyzer(analyzer: AnalyzerModel, profile: str = "DEFAULT") -> None:

    try:
        
        command =  [
            "databricks", "labs", "lakebridge", "analyze",
            "--source-directory", analyzer.source_directory,
            "--report-file", analyzer.report_file,
            "--source-tech", analyzer.source_tech
        ]

        print("Running Analyzer with command:")
        print(" ".join(command))

        try:
            subprocess.run(command, check=True)
            print(f"Analyzer completed. Report generated at {analyzer.report_file}")
        except subprocess.CalledProcessError as e:
            print(f"Analyzer failed: {e}")
        except FileNotFoundError:
            print("Databricks CLI not found. Please install and configure it first.")
    except  FileNotFoundError:
        print("what is error?")