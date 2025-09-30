from dataclasses import dataclass
import os


@dataclass
class UploadModel:
    source_notebook_path: str  # File or directory
    destination_directory: str  # Databricks workspace dir, e.g., /Users/me/project

    def validate(self) -> None:
        if not self.source_notebook_path:
            raise ValueError("Source path is required")
        if not os.path.exists(self.source_notebook_path):
            raise FileNotFoundError(f"Source not found: {self.source_notebook_path}")
        # If it's a file, ensure supported extensions
        if os.path.isfile(self.source_notebook_path):
            ext = os.path.splitext(self.source_notebook_path)[1].lower()
            if ext not in {".ipynb", ".py", ".sql"}:
                raise ValueError("Only .ipynb, .py, .sql files are supported")
        # Directory is okay; individual files will be filtered in service
        if not self.destination_directory:
            raise ValueError("Destination workspace directory is required")


