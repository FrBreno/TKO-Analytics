"""
TKO-Analytics: Sistema de análise de telemetria educacional.

Este pacote implementa pipeline ETL, métricas pedagógicas, Process Mining
e dashboard para análise de logs do sistema TKO (Test Knowledge Online).
"""

__version__ = "0.1.0"
__author__ = "Francisco Breno"
__email__ = "fbreno.dev@gmail.com"

# Facilitadores de importação para usuários do pacote
from tko_analytics.models import BaseEvent, ExecEvent, MoveEvent, SelfEvent
from tko_analytics.parsers import LogParser
from tko_analytics.etl import (
    EventValidator,
    ValidationReport,
    SQLiteLoader,
    SessionDetector,
    Session
)
from tko_analytics.metrics import MetricsEngine, MetricResult
from tko_analytics.exporters import XESExporter, export_to_xes
from tko_analytics.process_mining import ProcessAnalyzer, ProcessAnalysisResult

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
