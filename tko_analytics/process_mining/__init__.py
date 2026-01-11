"""
Process Mining module - Integração com PM4Py.

Este módulo fornece análise de processos educacionais usando PM4Py,
incluindo descoberta de modelos, análise de conformidade e variantes.
"""

from .analyzer import ProcessAnalyzer, ProcessAnalysisResult

__all__ = [
    'ProcessAnalyzer',
    'ProcessAnalysisResult',
]
