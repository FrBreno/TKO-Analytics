"""
TKO-Analytics: Sistema de análise de telemetria educacional.

Este pacote implementa pipeline ETL, métricas pedagógicas, Process Mining
e dashboard para análise de logs do sistema TKO (Test Knowledge Online).
"""

__version__ = "0.1.0"
__author__ = "Francisco Breno"
__email__ = "fbreno.dev@gmail.com"

# Facilitadores de importação para usuários do pacote
from src.models import BaseEvent, ExecEvent, MoveEvent, SelfEvent
from src.parsers import LogParser
from src.etl import (
    EventValidator,
    ValidationReport,
    SQLiteLoader,
    SessionDetector,
    Session
)
from src.metrics import MetricsEngine, MetricResult
from src.exporters import XESExporter, export_to_xes
from src.process_mining import ProcessAnalyzer, ProcessAnalysisResult

__all__ = [
    "__version__",
    "BaseEvent",
    "ExecEvent",
    "MoveEvent",
    "SelfEvent",
    "LogParser",
    "EventValidator",
    "ValidationReport",
    "SQLiteLoader",
    "SessionDetector",
    "Session",
    "MetricsEngine",
    "MetricResult",
    "XESExporter",
    "export_to_xes",
    "ProcessAnalyzer",
    "ProcessAnalysisResult",
]
