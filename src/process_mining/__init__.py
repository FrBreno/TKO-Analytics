"""
Process Mining module - Integração com PM4Py.

Este módulo fornece análise de processos educacionais usando PM4Py,
incluindo descoberta de modelos, análise de conformidade, detecção de
padrões comportamentais e variantes de processo.
"""

from .analyzer import ProcessAnalyzer, ProcessAnalysisResult
from .tko_to_xes import TKOToXESConverter
from .process_discovery import ProcessDiscovery, ProcessDiscoveryResult
from .conformance_checker import ConformanceChecker, ConformanceResult, ConformanceCheckingError
from .pattern_detector import BehavioralPatternDetector, BehavioralPattern

__all__ = [
    'ProcessAnalyzer',
    'ProcessAnalysisResult',
    'TKOToXESConverter',
    'ProcessDiscovery',
    'ProcessDiscoveryResult',
    'ConformanceChecker',
    'ConformanceResult',
    'ConformanceCheckingError',
    'BehavioralPatternDetector',
    'BehavioralPattern',
]
