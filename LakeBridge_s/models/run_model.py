from dataclasses import dataclass


@dataclass
class RunModel:
    notebook_path: str

    def get_notebook_path(self) -> str:
        return self.notebook_path


