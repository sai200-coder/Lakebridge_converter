from dataclasses import dataclass

@dataclass
class AnalyzerModel:
    source_directory: str       
    report_file: str              
    source_tech: str        
