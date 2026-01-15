"""
Dashboard Flask para visualização de análises TKO.

Este módulo fornece interface web para exploração de:
- Métricas pedagógicas por cohort
- Timeline individual de estudantes
- Análises de tarefas
- Visualizações de Process Mining
"""

import os
import sqlite3
import structlog
from flask import Flask
from pathlib import Path

logger = structlog.get_logger()

# Carrega variáveis de ambiente do arquivo .env
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        logger.info("[app] - Environment variables loaded from .env file")
except ImportError:
    logger.warning("[app] - python-dotenv not installed, .env file not loaded")


def create_app(db_path: str) -> Flask:
    """
    Factory para criar aplicação Flask.
    
    Args:
        db_path: Caminho para banco SQLite
        
    Returns:
        Aplicação Flask configurada
    """
    app = Flask(__name__)
    # Garantir caminho absoluto
    db_path = str(Path(db_path).resolve())
    app.config['DB_PATH'] = db_path
    
    logger.info("[create_app] - Database path configured",
               db_path=db_path)
    
    # Configurar SECRET_KEY para sessões
    secret_key = os.getenv('FLASK_SECRET_KEY')
    if not secret_key:
        # Gera uma chave aleatória se não houver variável de ambiente
        import secrets
        secret_key = secrets.token_hex(32)
        logger.info("[create_app] - Generated random secret key for Flask sessions")
    
    app.config['SECRET_KEY'] = secret_key
    
    # Registra rotas
    from src.dashboard import routes
    routes.register_routes(app)
    
    logger.info("flask_app_created", db_path=db_path)
    
    return app


def get_db_connection(app: Flask) -> sqlite3.Connection:
    """Cria conexão com banco de dados."""
    conn = sqlite3.connect(app.config['DB_PATH'])
    conn.row_factory = sqlite3.Row
    return conn


def run_server(db_path: str, host: str = '127.0.0.1', port: int = 5000, debug: bool = True):
    """
    Inicia servidor Flask.
    
    Args:
        db_path: Caminho para banco SQLite
        host: Host para bind
        port: Porta do servidor
        debug: Modo debug
    """
    app = create_app(db_path)
    
    logger.info("starting_flask_server", host=host, port=port, debug=debug)
    print(f"\n{'=' * 60}")
    print(f"TKO Analytics Dashboard")
    print(f"{'=' * 60}")
    print(f"\nServidor iniciado em: http://{host}:{port}")
    print(f"Database: {db_path}")
    print(f"\nRotas disponiveis:")
    print(f"  - http://{host}:{port}/")
    print(f"  - http://{host}:{port}/cohort")
    print(f"  - http://{host}:{port}/student/<student_hash>")
    print(f"  - http://{host}:{port}/task/<task_id>")
    print(f"\nPressione Ctrl+C para parar o servidor\n")
    
    app.run(host=host, port=port, debug=debug)
