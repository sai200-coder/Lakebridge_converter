from dataclasses import dataclass
from typing import Optional


@dataclass
class SequentialJobModel:
    """Model for sequential job configuration."""
    
    # Job configuration
    first_job_name: str = "lakebridge_tables_views_job"
    second_job_name: str = "lakebridge_functions_procedures_job"
    
    # Job settings
    max_concurrent_runs: int = 1
    timeout_seconds: int = 3600  # 1 hour
    retry_on_timeout: bool = True
    max_retries: int = 2
    
    # Monitoring settings
    monitor_timeout_minutes: int = 60
    check_interval_seconds: int = 30
    
    # Job descriptions
    first_job_description: str = "LakeBridge conversion - Tables and Views"
    second_job_description: str = "LakeBridge conversion - Functions and Procedures"
    
    # Object type categorization
    first_job_types: list = None  # Will be set to ['tables', 'views']
    second_job_types: list = None  # Will be set to ['functions', 'procedures']
    
    def __post_init__(self):
        if self.first_job_types is None:
            self.first_job_types = ['tables', 'views']
        if self.second_job_types is None:
            self.second_job_types = ['functions', 'procedures']
