"""
Helper functions para limpeza separada de dados MODEL e ANALYSIS.

Facilita a manutenção de tabelas separadas.
"""

import sqlite3
from pathlib import Path
import structlog

logger = structlog.get_logger()


def clear_model_data(db_path: str) -> dict:
    """
    Limpa apenas dados relacionados ao dataset MODEL.
    
    Remove:
    - model_events
    - Manter analysis_events, sessions e metrics intactos
    
    Args:
        db_path: Caminho do banco de dados
        
    Returns:
        Dict com estatísticas da limpeza
    """
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Contar antes
        model_events_before = conn.execute("SELECT COUNT(*) FROM model_events").fetchone()[0]
        
        # Limpar
        conn.execute("DELETE FROM model_events")
        conn.commit()
        
        logger.info("Model data cleared",
                   events_removed=model_events_before)
        
        return {
            'success': True,
            'model_events_removed': model_events_before,
            'message': f'{model_events_before} eventos MODEL removidos'
        }
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Failed to clear model data", error=str(e))
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        conn.close()


def clear_analysis_data(db_path: str) -> dict:
    """
    Limpa apenas dados relacionados ao dataset ANALYSIS.
    
    Remove:
    - analysis_events
    - sessions (associadas a analysis)
    - metrics (associadas a analysis)
    - Manter model_events intacto
    
    Args:
        db_path: Caminho do banco de dados
        
    Returns:
        Dict com estatísticas da limpeza
    """
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Contar antes
        analysis_events_before = conn.execute("SELECT COUNT(*) FROM analysis_events").fetchone()[0]
        sessions_before = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        metrics_before = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        
        # Limpar
        conn.execute("DELETE FROM analysis_events")
        conn.execute("DELETE FROM sessions")
        conn.execute("DELETE FROM metrics")
        conn.commit()
        
        logger.info("Analysis data cleared",
                   events_removed=analysis_events_before,
                   sessions_removed=sessions_before,
                   metrics_removed=metrics_before)
        
        return {
            'success': True,
            'analysis_events_removed': analysis_events_before,
            'sessions_removed': sessions_before,
            'metrics_removed': metrics_before,
            'message': f'{analysis_events_before} eventos ANALYSIS, {sessions_before} sessões e {metrics_before} métricas removidos'
        }
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Failed to clear analysis data", error=str(e))
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        conn.close()


def clear_all_data(db_path: str) -> dict:
    """
    Limpa TODOS os dados (MODEL + ANALYSIS).
    
    Remove:
    - model_events
    - analysis_events  
    - sessions
    - metrics
    
    Args:
        db_path: Caminho do banco de dados
        
    Returns:
        Dict com estatísticas da limpeza
    """
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Contar antes
        model_events_before = conn.execute("SELECT COUNT(*) FROM model_events").fetchone()[0]
        analysis_events_before = conn.execute("SELECT COUNT(*) FROM analysis_events").fetchone()[0]
        sessions_before = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        metrics_before = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        
        # Limpar tudo
        conn.execute("DELETE FROM model_events")
        conn.execute("DELETE FROM analysis_events")
        conn.execute("DELETE FROM sessions")
        conn.execute("DELETE FROM metrics")
        conn.commit()
        
        logger.info("All data cleared",
                   model_events_removed=model_events_before,
                   analysis_events_removed=analysis_events_before,
                   sessions_removed=sessions_before,
                   metrics_removed=metrics_before)
        
        return {
            'success': True,
            'model_events_removed': model_events_before,
            'analysis_events_removed': analysis_events_before,
            'sessions_removed': sessions_before,
            'metrics_removed': metrics_before,
            'message': f'Total: {model_events_before} MODEL + {analysis_events_before} ANALYSIS eventos, {sessions_before} sessões, {metrics_before} métricas removidos'
        }
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Failed to clear all data", error=str(e))
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        conn.close()


def get_data_stats(db_path: str) -> dict:
    """
    Retorna estatísticas atuais de ambos os datasets.
    
    Returns:
        Dict com contagens de model_events, analysis_events, sessions, metrics
    """
    db_path = Path(db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    
    try:
        stats = {
            'model_events': conn.execute("SELECT COUNT(*) FROM model_events").fetchone()[0],
            'analysis_events': conn.execute("SELECT COUNT(*) FROM analysis_events").fetchone()[0],
            'sessions': conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0],
            'metrics': conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0],
        }
        return stats
    except sqlite3.Error as e:
        logger.error("Failed to get stats", error=str(e))
        return {'error': str(e)}
    finally:
        conn.close()
