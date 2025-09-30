# Databricks notebook source
from dataclasses import dataclass
from models.analyzer_model import AnalyzerModel
from models.transpile_model import TranspilerModel
from models.reconcile_model import ReconcilerModel

@dataclass
class FullConfigModel:
    analyzer: AnalyzerModel
    transpiler: TranspilerModel
    reconciler: ReconcilerModel
    catalog_name: str
    schema_name: str