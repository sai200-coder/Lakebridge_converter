import subprocess
import json
import time
from typing import List, Dict, Optional
from models.run_model import RunModel
from models.sequential_job_model import SequentialJobModel


class SequentialJobService:
    """Service for creating and managing sequential Databricks jobs with dependencies."""
    
    def __init__(self, model: RunModel, config: Optional[SequentialJobModel] = None) -> None:
        self.model = model
        self.config = config or SequentialJobModel()

    def create_sequential_jobs(self, workspace_paths: List[str]) -> Dict[str, str]:
        """
        Create two sequential jobs:
        1. First job: Tables and Views (runs immediately)
        2. Second job: Functions and Procedures (runs after first job succeeds)
        
        Args:
            workspace_paths: List of uploaded notebook paths
            
        Returns:
            Dictionary with job IDs and run IDs
        """
        if not workspace_paths:
            print("No workspace paths provided for execution.")
            return {}

        # Categorize notebooks by object type
        categorized_paths = self._categorize_notebooks(workspace_paths)
        
        result = {
            "first_job": {},
            "second_job": {},
            "categorized_paths": categorized_paths
        }

        # Create first job (Tables and Views)
        if categorized_paths["tables_views"]:
            print(f"\n🚀 Creating first job for Tables and Views ({len(categorized_paths['tables_views'])} notebooks)...")
            first_job_result = self._create_job(
                job_name=self.config.first_job_name,
                notebook_paths=categorized_paths["tables_views"],
                job_description=self.config.first_job_description
            )
            result["first_job"] = first_job_result
            
            if first_job_result.get("run_id"):
                print(f"✅ First job created and started - Run ID: {first_job_result['run_id']}")
                
                # Create second job (Functions and Procedures) with dependency
                if categorized_paths["functions_procedures"]:
                    print(f"\n🚀 Creating second job for Functions and Procedures ({len(categorized_paths['functions_procedures'])} notebooks)...")
                    second_job_result = self._create_dependent_job(
                        job_name=self.config.second_job_name,
                        notebook_paths=categorized_paths["functions_procedures"],
                        job_description=self.config.second_job_description,
                        depends_on_job_id=first_job_result["job_id"]
                    )
                    result["second_job"] = second_job_result
                    
                    if second_job_result.get("run_id"):
                        print(f"✅ Second job created with dependency - Run ID: {second_job_result['run_id']}")
                    else:
                        print("❌ Failed to create second job")
                else:
                    print("ℹ️  No Functions/Procedures found - skipping second job")
            else:
                print("❌ Failed to create first job")
        else:
            print("ℹ️  No Tables/Views found - skipping first job")
            
            # If no tables/views, run functions/procedures immediately
            if categorized_paths["functions_procedures"]:
                print(f"\n🚀 Creating single job for Functions and Procedures ({len(categorized_paths['functions_procedures'])} notebooks)...")
                second_job_result = self._create_job(
                    job_name=self.config.second_job_name,
                    notebook_paths=categorized_paths["functions_procedures"],
                    job_description=self.config.second_job_description
                )
                result["second_job"] = second_job_result

        return result

    def _categorize_notebooks(self, workspace_paths: List[str]) -> Dict[str, List[str]]:
        """Categorize notebook paths by object type based on their directory structure."""
        
        categorized = {
            "tables_views": [],
            "functions_procedures": [],
            "others": []
        }
        
        for path in workspace_paths:
            # Extract the object type from the path
            # Expected format: /path/to/notebooks/object_type/notebook.ipynb
            path_parts = path.split('/')
            if len(path_parts) >= 2:
                object_type = path_parts[-2].lower()  # Get the directory name
                
                if object_type in ['tables', 'views']:
                    categorized["tables_views"].append(path)
                elif object_type in ['functions', 'procedures']:
                    categorized["functions_procedures"].append(path)
                else:
                    categorized["others"].append(path)
            else:
                categorized["others"].append(path)
        
        print(f"📊 Categorized notebooks:")
        print(f"  Tables/Views: {len(categorized['tables_views'])}")
        print(f"  Functions/Procedures: {len(categorized['functions_procedures'])}")
        print(f"  Others: {len(categorized['others'])}")
        
        return categorized

    def _create_job(self, job_name: str, notebook_paths: List[str], job_description: str) -> Dict[str, str]:
        """Create a Databricks job with the given notebooks."""
        
        if not notebook_paths:
            return {"error": "No notebook paths provided"}

        # Build tasks array
        tasks = []
        for idx, notebook_path in enumerate(notebook_paths, start=1):
            task = {
                "task_key": f"task_{idx}",
                "notebook_task": {
                    "notebook_path": notebook_path
                }
            }
            tasks.append(task)

        # Job specification
        job_spec = {
            "name": job_name,
            "description": job_description,
            "tasks": tasks,
            "max_concurrent_runs": self.config.max_concurrent_runs,
            "timeout_seconds": self.config.timeout_seconds,
            "retry_on_timeout": self.config.retry_on_timeout,
            "max_retries": self.config.max_retries
        }

        # Convert to escaped inline JSON string
        job_json = json.dumps(job_spec)
        
        try:
            print(f"📝 Creating Databricks job: {job_name}")
            create_result = subprocess.run(
                ["databricks", "jobs", "create", "--json", job_json],
                capture_output=True, text=True, check=True
            )

            create_response = json.loads(create_result.stdout)
            job_id = str(create_response.get("job_id", "unknown"))
            print(f"✅ Job created successfully. Job ID: {job_id}")

            print("🚀 Running the job...")
            run_result = subprocess.run(
                ["databricks", "jobs", "run-now", job_id],
                capture_output=True, text=True, check=True
            )

            run_response = json.loads(run_result.stdout)
            run_id = str(run_response.get("run_id", "unknown"))
            print(f"✅ Job started successfully. Run ID: {run_id}")
            
            return {
                "job_id": job_id,
                "run_id": run_id,
                "status": "started"
            }

        except subprocess.CalledProcessError as e:
            print("❌ Databricks CLI error:")
            print(f"   Exit code: {e.returncode}")
            print(f"   Error output: {e.stderr}")
            print(f"   Standard output: {e.stdout}")
            return {"error": str(e)}

    def _create_dependent_job(self, job_name: str, notebook_paths: List[str], 
                            job_description: str, depends_on_job_id: str) -> Dict[str, str]:
        """Create a Databricks job that depends on another job's completion."""
        
        if not notebook_paths:
            return {"error": "No notebook paths provided"}

        # Build tasks array
        tasks = []
        for idx, notebook_path in enumerate(notebook_paths, start=1):
            task = {
                "task_key": f"task_{idx}",
                "notebook_task": {
                    "notebook_path": notebook_path
                }
            }
            tasks.append(task)

        # Job specification with dependency
        job_spec = {
            "name": job_name,
            "description": job_description,
            "tasks": tasks,
            "max_concurrent_runs": self.config.max_concurrent_runs,
            "timeout_seconds": self.config.timeout_seconds,
            "retry_on_timeout": self.config.retry_on_timeout,
            "max_retries": self.config.max_retries,
            "depends_on": [
                {
                    "job_id": depends_on_job_id,
                    "outcome": "SUCCESS"
                }
            ]
        }

        # Convert to escaped inline JSON string
        job_json = json.dumps(job_spec)
        
        try:
            print(f"📝 Creating dependent Databricks job: {job_name}")
            create_result = subprocess.run(
                ["databricks", "jobs", "create", "--json", job_json],
                capture_output=True, text=True, check=True
            )

            create_response = json.loads(create_result.stdout)
            job_id = str(create_response.get("job_id", "unknown"))
            print(f"✅ Dependent job created successfully. Job ID: {job_id}")

            print("🚀 Running the dependent job...")
            run_result = subprocess.run(
                ["databricks", "jobs", "run-now", job_id],
                capture_output=True, text=True, check=True
            )

            run_response = json.loads(run_result.stdout)
            run_id = str(run_response.get("run_id", "unknown"))
            print(f"✅ Dependent job started successfully. Run ID: {run_id}")
            
            return {
                "job_id": job_id,
                "run_id": run_id,
                "status": "started",
                "depends_on": depends_on_job_id
            }

        except subprocess.CalledProcessError as e:
            print("❌ Databricks CLI error:")
            print(f"   Exit code: {e.returncode}")
            print(f"   Error output: {e.stderr}")
            print(f"   Standard output: {e.stdout}")
            return {"error": str(e)}

    def monitor_jobs(self, job_results: Dict[str, Dict[str, str]]) -> Dict[str, str]:
        """Monitor the status of created jobs."""
        
        print("\n📊 Monitoring job status...")
        
        status = {
            "first_job_status": "unknown",
            "second_job_status": "unknown"
        }
        
        # Monitor first job
        if job_results.get("first_job", {}).get("run_id"):
            first_status = self._get_job_status(job_results["first_job"]["run_id"])
            status["first_job_status"] = first_status
            print(f"First job status: {first_status}")
        
        # Monitor second job
        if job_results.get("second_job", {}).get("run_id"):
            second_status = self._get_job_status(job_results["second_job"]["run_id"])
            status["second_job_status"] = second_status
            print(f"Second job status: {second_status}")
        
        return status

    def _get_job_status(self, run_id: str) -> str:
        """Get the status of a specific job run."""
        try:
            result = subprocess.run(
                ["databricks", "jobs", "get", run_id],
                capture_output=True, text=True, check=True
            )
            response = json.loads(result.stdout)
            return response.get("state", {}).get("life_cycle_state", "unknown")
        except Exception as e:
            print(f"Error getting job status for run {run_id}: {e}")
            return "error"

    def wait_for_completion(self, job_results: Dict[str, Dict[str, str]], 
                          timeout_minutes: Optional[int] = None) -> Dict[str, str]:
        """Wait for jobs to complete with timeout."""
        
        timeout_minutes = timeout_minutes or self.config.monitor_timeout_minutes
        print(f"\n⏳ Waiting for jobs to complete (timeout: {timeout_minutes} minutes)...")
        
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while time.time() - start_time < timeout_seconds:
            status = self.monitor_jobs(job_results)
            
            # Check if first job is complete
            first_complete = status["first_job_status"] in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
            second_complete = status["second_job_status"] in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
            
            if first_complete and (not job_results.get("second_job") or second_complete):
                print("\n✅ All jobs completed!")
                return status
            
            time.sleep(self.config.check_interval_seconds)
        
        print(f"\n⏰ Timeout reached after {timeout_minutes} minutes")
        return self.monitor_jobs(job_results)
