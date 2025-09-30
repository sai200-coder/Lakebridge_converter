import subprocess
import json
from typing import List
from models.run_model import RunModel


class RunService:
    def __init__(self, model: RunModel) -> None:
        self.model = model

    def run_in_databricks(self, workspace_paths: List[str]) -> str:
        """Run all uploaded notebooks as individual tasks in a single Databricks job (inline JSON)."""
        if not workspace_paths:
            print("No workspace paths provided for execution.")
            return None

        try:
            run_id = self.run_all_in_databricks_job(workspace_paths)
            print(f"✅ Submitted single job with {len(workspace_paths)} tasks - Run ID: {run_id}")
            return run_id
        except Exception as e:
            print(f"❌ Failed to submit job: {str(e)}")
            return None

    def run_all_in_databricks_job(self, workspace_paths: List[str]) -> str:
        """Create and run a Databricks job with inline JSON (no temp file)."""
        if not workspace_paths:
            raise ValueError("No workspace paths provided for execution.")

        # Build tasks array
        tasks = []
        for idx, workspace_path in enumerate(workspace_paths, start=1):
            task = {
                "task_key": f"task_{idx}",
                "notebook_task": {
                    "notebook_path": workspace_path
                }
            }
            tasks.append(task)

        # Minimal job spec
        job_spec = {
            "name": "emp_sql_job",
            "tasks": tasks
        }

        # Convert to escaped inline JSON string
        job_json = json.dumps(job_spec)
        
        try:
            print("📝 Creating Databricks job (inline JSON)...")
            create_result = subprocess.run(
                ["databricks", "jobs", "create", "--json", job_json],
                capture_output=True, text=True, check=True
            )

            create_response = json.loads(create_result.stdout)
            job_id = str(create_response.get("job_id", "unknown"))
            print(f"✅ Job created successfully. Job ID:{job_id}")

            print("🚀 Running the job...")
            run_result = subprocess.run(
                ["databricks", "jobs", "run-now",job_id],
                capture_output=True, text=True, check=True
            )

            run_response = json.loads(run_result.stdout)
            run_id = str(run_response.get("run_id", "unknown"))
            print(f"✅ Job started successfully. Run ID: {run_id}")
            return run_id

        except subprocess.CalledProcessError as e:
            print("❌ Databricks CLI error:")
            print(f"   Exit code: {e.returncode}")
            print(f"   Error output: {e.stderr}")
            print(f"   Standard output: {e.stdout}")
            print(f"   Submitted JSON: {job_json}")
            raise e

    def get_run_status(self, run_id: str) -> dict:
        """Get the status of a Databricks run."""
        try:
            result = subprocess.run(
                ["databricks", "runs", "get", "--run-id", run_id],
                capture_output=True, text=True, check=True
            )
            return json.loads(result.stdout)
        except Exception as e:
            return {"error": str(e)}

    def list_runs(self, limit: int = 20) -> List[dict]:
        """List recent Databricks runs."""
        try:
            result = subprocess.run(
                ["databricks", "runs", "list", "--limit", str(limit)],
                capture_output=True, text=True, check=True
            )
            return json.loads(result.stdout).get("runs", [])
        except Exception as e:
            print(f"Error listing runs: {str(e)}")
            return []

    def run(self) -> str:
        raise NotImplementedError("Use run_in_databricks() or run_all_in_databricks_job() instead")
