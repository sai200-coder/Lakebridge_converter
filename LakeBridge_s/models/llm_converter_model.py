# Databricks notebook source
from dataclasses import dataclass
from typing import Optional


@dataclass
class LLMConverterModel:
    """Model for LLMConverter service configuration."""
    source_folder: str
    output_folder: str
    databricks_notebooks_folder: str
    groq_api_key: Optional[str] = None
    llm_model: str = "llama-3.1-8b-instant"
    temperature: float = 0.1
    max_tokens: int = 4096
    catalog_name: str = "workspace"
    schema_name: str = "default"
    preserve_structure: bool = True
    create_backup: bool = True
