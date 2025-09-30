from dataclasses import dataclass

@dataclass
class TranspilerModel:
    source_dialect: str
    input_source: str
    output_folder: str
    error_file_path: str
    catalog_name: str
    schema_name: str
    validate: str      
    warehouse: str         
    override: str         
    open_config: str     
    llm_model: str
    temperature: float
