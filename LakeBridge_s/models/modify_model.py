# Databricks notebook source
from dataclasses import dataclass


@dataclass
class ModifyNotebookModel:
    transpiled_dir: str
    output_dir: str
    llm_model: str
    temperature: float


