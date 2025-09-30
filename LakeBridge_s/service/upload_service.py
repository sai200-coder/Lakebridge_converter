import os
import subprocess
from typing import List, Tuple
from models.upload_model import UploadModel


class UploadService:
    def __init__(self, model: UploadModel) -> None:
        self.model = model

    def _infer_language(self, file_path: str) -> str:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".py":
            return "PYTHON"
        if ext in {".ipynb", ".sql"}:
            return "SQL"
        if ext in {".scala", ".sc"}:
            return "SCALA"
        # Default language for notebooks if not clear
        return "SQL"

    def _infer_format(self, file_path: str) -> str:
        return "JUPYTER" if file_path.lower().endswith(".ipynb") else "SOURCE"

    def _workspace_object_path_from_rel(self, workspace_dir: str, relative_path: str) -> str:
        # Strip extension for workspace object name
        rel_no_ext = os.path.splitext(relative_path)[0]
        # Normalize separators to workspace style
        rel_unix = rel_no_ext.replace("\\", "/").lstrip("/")
        return workspace_dir.rstrip("/") + "/" + rel_unix

    def _ensure_workspace_dir(self, workspace_object_path: str) -> None:
        # Ensure parent directory exists in workspace
        parent = workspace_object_path.rsplit("/", 1)[0]
        if parent:
            subprocess.run([
                "databricks",
                "workspace",
                "mkdirs",
                parent,
            ], check=True)

    def _import_to_workspace(self, local_file: str, workspace_path: str) -> None:
        language = self._infer_language(local_file)
        fmt = self._infer_format(local_file)
        cmd = [
            "databricks",
            "workspace",
            "import",
            workspace_path,
            "--file",
            os.path.abspath(local_file),
            "--format",
            fmt,
            "--overwrite",
        ]
        # Only add --language for non-JUPYTER imports (optional for JUPYTER)
        if fmt != "JUPYTER":
            cmd.extend(["--language", language])

        # Ensure workspace directory structure exists
        self._ensure_workspace_dir(workspace_path)
        subprocess.run(cmd, check=True)
        print(f"Imported to workspace: {workspace_path}")


    def _iter_supported_files(self, source_root: str) -> List[Tuple[str, str]]:
        supported_exts = {".ipynb", ".py", ".sql"}
        collected: List[Tuple[str, str]] = []
        if os.path.isdir(source_root):
            for dirpath, _, filenames in os.walk(source_root):
                for name in filenames:
                    ext = os.path.splitext(name)[1].lower()
                    if ext in supported_exts:
                        local_file = os.path.join(dirpath, name)
                        rel_path = os.path.relpath(local_file, source_root)
                        collected.append((local_file, rel_path))
        else:
            ext = os.path.splitext(source_root)[1].lower()
            if ext in supported_exts:
                collected.append((source_root, os.path.basename(source_root)))
        return collected

    def upload(self) -> List[str]:
        source_path = self.model.source_notebook_path
        workspace_dir = self.model.destination_directory  # Interpret as Databricks workspace directory

        if not source_path:
            raise ValueError("Source path is required")
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source not found: {source_path}")
        if not workspace_dir:
            raise ValueError("Destination workspace directory is required")

        # Determine traversal root
        traversal_root = source_path if os.path.isdir(source_path) else os.path.dirname(source_path) or "."

        local_with_rel = self._iter_supported_files(source_path)
        if not local_with_rel:
            raise ValueError("No supported files found to upload (.ipynb, .py, .sql)")

        imported_paths: List[str] = []
        for file, rel in local_with_rel:
            workspace_path = self._workspace_object_path_from_rel(workspace_dir, rel)
            self._import_to_workspace(file, workspace_path)
            imported_paths.append(workspace_path)

        return imported_paths


