"""
Rotas do dashboard Flask.
"""

import os
import csv
import sqlite3
import structlog
import plotly.graph_objects as go
import pm4py
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, jsonify, abort, current_app, request, flash, redirect, url_for

from src.metrics.engine import MetricsEngine
from src.etl.session_detector import SessionDetector

logger = structlog.get_logger()


def get_db():
    """Obtém conexão com banco de dados."""
    conn = sqlite3.connect(current_app.config['DB_PATH'])
    conn.row_factory = sqlite3.Row
    return conn


def has_events_in_database() -> bool:
    """
    Verifica se há eventos no banco de dados (qualquer tabela).
    
    Returns:
        True se houver pelo menos um evento, False caso contrário
    """
    conn = get_db()
    # Verificar ambas as tabelas
    model_count = conn.execute('SELECT COUNT(*) as count FROM model_events').fetchone()['count']
    analysis_count = conn.execute('SELECT COUNT(*) as count FROM analysis_events').fetchone()['count']
    conn.close()
    return (model_count + analysis_count) > 0


def has_model_events() -> bool:
    """
    Verifica se há eventos MODEL no banco de dados.
    
    Returns:
        True se houver pelo menos um evento MODEL, False caso contrário
    """
    conn = get_db()
    model_count = conn.execute('SELECT COUNT(*) as count FROM model_events').fetchone()['count']
    conn.close()
    return model_count > 0


def has_analysis_events() -> bool:
    """
    Verifica se há eventos ANALYSIS no banco de dados.
    
    Returns:
        True se houver pelo menos um evento ANALYSIS, False caso contrário
    """
    conn = get_db()
    analysis_count = conn.execute('SELECT COUNT(*) as count FROM analysis_events').fetchone()['count']
    conn.close()
    return analysis_count > 0


def register_routes(app: Flask):
    """Registra todas as rotas no app Flask."""
    
    @app.route('/')
    def index():
        """Homepage com visão geral."""
        # Verificar se banco está vazio
        if not has_events_in_database():
            return render_template('setup_wizard.html')
        
        conn = get_db()
        
        # Contar eventos de ambas as tabelas
        model_count = conn.execute('SELECT COUNT(*) as count FROM model_events').fetchone()['count']
        analysis_count = conn.execute('SELECT COUNT(*) as count FROM analysis_events').fetchone()['count']
        total_events = model_count + analysis_count
        
        # Estatísticas gerais (combinando ambas as tabelas)
        stats = {
            'total_events': total_events,
            'total_students': conn.execute(
                'SELECT COUNT(DISTINCT student_hash) FROM ('
                '  SELECT student_hash FROM model_events '
                '  UNION '
                '  SELECT student_hash FROM analysis_events'
                ')'
            ).fetchone()[0],
            'total_tasks': conn.execute(
                'SELECT COUNT(DISTINCT task_id) FROM ('
                '  SELECT task_id FROM model_events '
                '  UNION '
                '  SELECT task_id FROM analysis_events'
                ')'
            ).fetchone()[0],
            'total_sessions': conn.execute('SELECT COUNT(*) as count FROM sessions').fetchone()['count'],
        }
        
        # Estatísticas de Process Mining
        stats['model_events'] = model_count
        stats['analysis_events'] = analysis_count
        stats['conformance_metrics'] = conn.execute("SELECT COUNT(*) as count FROM metrics WHERE metric_name='conformance_fitness'").fetchone()['count']
        
        conn.close()
        
        return render_template('index.html', stats=stats)
    
    
    # DESABILITADO: Visão do Cohort (não será usado por enquanto)
    # @app.route('/cohort')
    # def cohort_overview():
    #     """Visão geral do cohort com heatmap."""
    #     ...
    
    
    @app.route('/student/<student_hash>')
    def student_detail(student_hash: str):
        """Detalhes de um estudante específico."""
        conn = get_db()
        
        # Verifica se estudante existe em alguma das tabelas
        model_count = conn.execute(
            'SELECT COUNT(*) as count FROM model_events WHERE student_hash = ?',
            (student_hash,)
        ).fetchone()['count']
        
        analysis_count = conn.execute(
            'SELECT COUNT(*) as count FROM analysis_events WHERE student_hash = ?',
            (student_hash,)
        ).fetchone()['count']
        
        if model_count + analysis_count == 0:
            conn.close()
            abort(404, description="Estudante não encontrado")
        
        # Busca eventos do estudante (combinando ambas as tabelas)
        events_query = """
        SELECT 
            id, timestamp, task_id, event_type, activity as activity_name, metadata, 'model' as source
        FROM model_events
        WHERE student_hash = ?
        UNION ALL
        SELECT 
            id, timestamp, task_id, event_type, activity as activity_name, metadata, 'analysis' as source
        FROM analysis_events
        WHERE student_hash = ?
        ORDER BY timestamp ASC
        """
        
        events = conn.execute(events_query, (student_hash, student_hash)).fetchall()
        
        # Busca métricas do estudante
        metrics_query = """
        SELECT 
            task_id,
            metric_name,
            metric_value,
            metadata
        FROM metrics
        WHERE student_hash = ?
        ORDER BY task_id, metric_name
        """
        
        metrics_rows = conn.execute(metrics_query, (student_hash,)).fetchall()
        
        # Organiza métricas por tarefa
        metrics_by_task = {}
        for row in metrics_rows:
            task = row['task_id']
            if task not in metrics_by_task:
                metrics_by_task[task] = {}
            metrics_by_task[task][row['metric_name']] = row['metric_value']
        
        # Busca sessões
        sessions_query = """
        SELECT 
            id,
            task_id,
            start_time,
            end_time,
            duration_seconds,
            total_events,
            exec_events,
            move_events,
            self_events
        FROM sessions
        WHERE student_hash = ?
        ORDER BY start_time ASC
        """
        
        sessions = conn.execute(sessions_query, (student_hash,)).fetchall()
        
        # Cria timeline de eventos
        timestamps = [datetime.fromisoformat(e['timestamp']) for e in events]
        event_types = [e['event_type'] for e in events]
        activities = [e['activity_name'] for e in events]
        tasks = [e['task_id'] for e in events]
        
        # Mapeia tipos de evento para cores
        color_map = {
            'ExecEvent': 'blue',
            'MoveEvent': 'green',
            'SelfEvent': 'orange'
        }
        colors = [color_map.get(et, 'gray') for et in event_types]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=[1] * len(timestamps),
            mode='markers+text',
            marker=dict(
                size=12,
                color=colors,
                symbol='circle',
                line=dict(width=1, color='white')
            ),
            text=activities,
            textposition='top center',
            textfont=dict(size=8),
            hovertemplate='<b>%{text}</b><br>Task: %{customdata}<br>Time: %{x}<extra></extra>',
            customdata=tasks,
            name='Events'
        ))
        
        fig.update_layout(
            title=f'Event Timeline - Student {student_hash[:8]}',
            xaxis_title='Time',
            yaxis=dict(visible=False),
            height=300,
            showlegend=False,
            hovermode='closest'
        )
        
        timeline_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        
        conn.close()
        
        return render_template(
            'student.html',
            student_hash=student_hash,
            events=events,
            metrics_by_task=metrics_by_task,
            sessions=sessions,
            timeline=timeline_html,
            total_events=len(events),
            total_sessions=len(sessions)
        )
    
    
    # DESABILITADO: Análise de Tarefa (não será usado por enquanto)
    # @app.route('/task/<task_id>')
    # def task_analytics(task_id: str):
    #     """Análise agregada de uma tarefa."""
    #     ...
    
    
    @app.route('/api/metrics/<student_hash>')
    def api_metrics(student_hash: str):
        """API endpoint para obter métricas de um estudante."""
        conn = get_db()
        
        query = """
        SELECT 
            task_id,
            metric_name,
            metric_value,
            metadata
        FROM metrics
        WHERE student_hash = ?
        """
        
        rows = conn.execute(query, (student_hash,)).fetchall()
        
        metrics = [dict(row) for row in rows]
        
        conn.close()
        
        return jsonify(metrics)
    
    
    @app.route('/import', methods=['GET', 'POST'])
    def import_tko_data():
        """Interface de importação de dados TKO."""
        if request.method == 'POST':
            root_dir = request.form.get('root_dir')
            import_mode = request.form.get('import_mode', 'incremental')
            dataset_role = request.form.get('dataset_role', 'analysis')  # MODEL ou ANALYSIS
            
            # Validar dataset_role
            if dataset_role not in ('model', 'analysis'):
                flash('Tipo de dataset inválido! Use "model" ou "analysis".', 'danger')
                return render_template('import.html', 
                                     has_model_data=has_model_events(),
                                     has_analysis_data=has_analysis_events())
            
            # Verificar se há dados do tipo correto no banco
            has_data = has_model_events() if dataset_role == 'model' else has_analysis_events()
            
            # Validar modo incremental apenas se houver dados do tipo correto
            if import_mode == 'incremental' and not has_data:
                dataset_label = "MODEL" if dataset_role == 'model' else "ANALYSIS"
                flash(f'Modo incremental não disponível para {dataset_label}: nenhum dado deste tipo no banco. Use modo limpa para primeira importação.', 'warning')
                return render_template('import.html', 
                                     has_model_data=has_model_events(),
                                     has_analysis_data=has_analysis_events())
            
            # Validar diretório
            root_path = Path(root_dir)
            if not root_path.exists():
                flash('Diretório não encontrado!', 'danger')
                return render_template('import.html', 
                                     has_model_data=has_model_events(),
                                     has_analysis_data=has_analysis_events())
            
            try:
                # Importar módulos
                from src.tko_integration.scanner import ClassroomScanner
                from src.tko_integration.transformer import TKOTransformer
                from src.tko_integration.validator import DataValidator
                from src.parsers.log_parser import LogParser
                from src.etl.loader import SQLiteLoader
                import shutil
                
                # Diretório temporário fixo para CSV (será excluído após importação)
                output_dir = Path("data/temp_import")
                csv_path = output_dir / "events.csv"
                
                # Se modo limpo, limpar banco de dados ANALYSIS
                if import_mode == 'clean':
                    from src.etl.data_cleanup import clear_analysis_data
                    result = clear_analysis_data(current_app.config['DB_PATH'])
                    if result['success']:
                        logger.info("Analysis data cleared (clean mode)", **result)
                        flash(result['message'], 'info')
                    else:
                        logger.error("Failed to clear analysis data", error=result.get('error'))
                        flash(f'Erro ao limpar dados: {result.get("error")}', 'warning')
                
                # Executar scan
                logger.info("Starting TKO data scan", root_dir=str(root_path), mode=import_mode)
                scanner = ClassroomScanner()
                scan = scanner.scan_directory(root_path)
                
                logger.info("Scan complete", 
                           turmas=len(scan.turmas),
                           students=scan.total_students,
                           valid_repos=scan.valid_repos)
                
                # Transformar para CSV
                output_dir.mkdir(parents=True, exist_ok=True)
                
                salt = os.getenv('STUDENT_ID_SALT', 'default-salt-change-me')
                transformer = TKOTransformer(salt)
                
                logger.info("Transforming to CSV", output=str(csv_path))
                
                # Sempre gerar CSV novo
                total_events = transformer.transform_scan_to_csv(scan, csv_path, mode='new')
                
                logger.info("Transformation complete", events=total_events, mode=import_mode)
                
                # Adicionar dados ao resultado
                scan.total_events = total_events
                scan.csv_path = str(csv_path)
                scan.import_mode = import_mode
                
                # Gerar relatório de validação
                validator = DataValidator()
                report = validator.generate_report(scan)
                logger.info("Validation report generated", warnings=len(scan.warnings))
                
                # Carregar CSV no banco de dados SQLite usando Pydantic + SQLiteLoader
                try:
                    logger.info("Loading CSV into database", csv=str(csv_path), db=current_app.config['DB_PATH'])
                    # Se modo limpo, limpar banco antes (apenas dados do dataset_role atual)
                    if import_mode == 'clean':
                        from src.etl.data_cleanup import clear_model_data, clear_analysis_data
                        if dataset_role == 'model':
                            result = clear_model_data(current_app.config['DB_PATH'])
                        else:
                            result = clear_analysis_data(current_app.config['DB_PATH'])
                        
                        logger.info("Database cleared before import",
                               dataset_role=dataset_role,
                               mode=import_mode,
                               result=result)
                    
                    # Agrupar eventos por student_id do CSV
                    events_by_student = {}
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            student_id = row.get('student_id', 'unknown')
                            if student_id not in events_by_student:
                                events_by_student[student_id] = []
                            events_by_student[student_id].append(row)
                    
                    if events_by_student:
                        parser = LogParser(strict=False)
                        all_events = parser.parse_file(csv_path)
                        
                        logger.info("CSV parsed to Pydantic models",
                                   events=len(all_events),
                                   parse_errors=len(parser.errors))
                        
                        if parser.errors:
                            logger.warning("Parse errors detected",
                                         error_count=len(parser.errors),
                                         sample_errors=[str(e) for e in parser.errors[:3]])
                        
                        # FIX: Reprocessar CSV para obter student_id de cada linha
                        # e associar corretamente aos eventos Pydantic parseados
                        pydantic_by_student = {}
                        
                        with open(csv_path, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for idx, (row, event) in enumerate(zip(reader, all_events)):
                                student_id = row.get('student_id', 'unknown')
                                if student_id not in pydantic_by_student:
                                    pydantic_by_student[student_id] = []
                                pydantic_by_student[student_id].append(event)
                        
                        loader = SQLiteLoader(current_app.config['DB_PATH'], batch_size=1000)
                        total_loaded = 0
                        
                        logger.info("SQLiteLoader initialized",
                                   db_path=current_app.config['DB_PATH'],
                                   students_to_load=len(pydantic_by_student))

                        for student_id, student_events in pydantic_by_student.items():
                            if student_events:
                                try:
                                    # Gerar case_id único baseado em timestamp
                                    import time
                                    case_id = f"case_{int(time.time())}_{student_id[:8]}"
                                    
                                    logger.info("Loading events for student",
                                              student_hash=student_id[:8],
                                              events_count=len(student_events),
                                              case_id=case_id)
                                    
                                    loaded = loader.load_events(
                                        events=student_events,
                                        student_id=student_id,
                                        case_id=case_id,
                                        session_id=None,
                                        dataset_role=dataset_role  # Passar dataset_role
                                    )
                                    total_loaded += loaded
                                    logger.info("Loaded events for student",
                                              student_hash=student_id[:8],
                                              events=loaded,
                                              dataset_role=dataset_role)
                                except Exception as e:
                                    logger.error("Failed to load events for student",
                                               student_hash=student_id[:8],
                                               error=str(e),
                                               exc_info=True)
                                    continue
                        
                        logger.info("Events loaded into database", 
                                   total=total_loaded,
                                   students=len(pydantic_by_student),
                                   dataset_role=dataset_role)
                        
                        # Verificar persistência nas tabelas corretas
                        conn_verify = get_db()
                        cursor_verify = conn_verify.cursor()
                        table_name = f"{dataset_role}_events"
                        cursor_verify.execute(f"SELECT COUNT(*) FROM {table_name}")
                        db_event_count = cursor_verify.fetchone()[0]
                        conn_verify.close()
                        
                        logger.info("Database verification after load",
                                   table=table_name,
                                   total_in_table=db_event_count,
                                   just_loaded=total_loaded)
                        
                        # Limpar arquivos temporários CSV após carregamento bem-sucedido
                        try:
                            if output_dir.exists():
                                shutil.rmtree(output_dir)
                                logger.info("Temporary CSV files deleted", path=str(output_dir))
                        except Exception as e:
                            logger.warning("Failed to delete temporary files", error=str(e))
                        
                        role_label = "MODEL (geração de processo)" if dataset_role == "model" else "ANALYSIS (análise comportamental)"
                        flash(f'Dados carregados no banco: {total_loaded} eventos de {len(pydantic_by_student)} estudante(s). Dataset: {role_label}', 'success')
                        
                        # Se for ANALYSIS, processar métricas automaticamente
                        if dataset_role == 'analysis':
                            try:
                                logger.info("Auto-processing metrics for ANALYSIS dataset")
                                from src.etl.engine import ETLEngine
                                
                                engine = ETLEngine(db_path=current_app.config['DB_PATH'])
                                result = engine.process_events()
                                
                                logger.info("ETL processing complete",
                                          students=result['students_processed'],
                                          sessions=result['sessions_detected'],
                                          metrics=result['metrics_calculated'])
                                
                                flash(f'✅ Métricas processadas automaticamente: {result["sessions_detected"]} sessões detectadas, {result["metrics_calculated"]} métricas calculadas.', 'success')
                            except Exception as e:
                                logger.error("Auto-processing failed", error=str(e), exc_info=True)
                                flash(f'⚠️ Aviso: Falha ao processar métricas automaticamente: {str(e)}', 'warning')
                    else:
                        logger.warning("No events found in CSV")
                        flash('Aviso: Nenhum evento encontrado no CSV.', 'warning')
                        
                except Exception as e:
                    logger.error("Failed to load CSV into database", error=str(e), exc_info=True)
                    flash(f'Aviso: Falha ao carregar dados no banco: {str(e)}', 'warning')
                    # Tentar limpar arquivos temporários mesmo em caso de erro
                    try:
                        if output_dir.exists():
                            shutil.rmtree(output_dir)
                    except:
                        pass
                
                mode_msg = 'incremental' if import_mode == 'incremental' else 'limpa'
                role_label = "MODEL" if dataset_role == "model" else "ANALYSIS"
                flash(f'Importação {mode_msg} de {role_label} concluída! {total_events} eventos processados.', 'success')
                
                # Redirecionar para página limpa (não mostrar scan_result)
                return redirect(url_for('import_tko_data'))
                
            except Exception as e:
                logger.error("Import failed", error=str(e), exc_info=True)
                flash(f'Erro durante importação: {str(e)}', 'danger')
                # Tentar limpar arquivos temporários mesmo em caso de erro
                try:
                    if output_dir.exists():
                        shutil.rmtree(output_dir)
                except:
                    pass
                return render_template('import.html', 
                                     has_model_data=has_model_events(),
                                     has_analysis_data=has_analysis_events())
        
        # Verificar se há dados no banco para modo GET
        return render_template('import.html', 
                             has_model_data=has_model_events(),
                             has_analysis_data=has_analysis_events())
    
    
    @app.route('/clear_model_data', methods=['POST'])
    def clear_model_data():
        """Limpa apenas os dados MODEL do banco de dados."""
        try:
            conn = get_db()
            cursor = conn.cursor()
            
            # Limpar apenas tabelas relacionadas a MODEL
            cursor.execute("DELETE FROM model_events")
            model_count = cursor.rowcount
            
            # Limpar process_models se existir
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='process_models'")
            if cursor.fetchone():
                cursor.execute("DELETE FROM process_models")
            
            conn.commit()
            conn.close()
            
            flash(f'Dados MODEL limpos com sucesso! {model_count} eventos removidos. Modelo PM deletado.', 'success')
            logger.info("MODEL data cleared successfully", events_removed=model_count)
            
        except Exception as e:
            logger.error("Failed to clear MODEL data", error=str(e), exc_info=True)
            flash(f'Erro ao limpar dados MODEL: {str(e)}', 'danger')
        
        return redirect(url_for('import_tko_data'))
    
    @app.route('/clear_analysis_data', methods=['POST'])
    def clear_analysis_data():
        """Limpa apenas os dados ANALYSIS do banco de dados."""
        try:
            conn = get_db()
            cursor = conn.cursor()
            
            # Limpar apenas tabelas relacionadas a ANALYSIS
            cursor.execute("DELETE FROM analysis_events")
            events_count = cursor.rowcount
            
            # Limpar sessions, metrics e behavioral_patterns
            cursor.execute("DELETE FROM sessions")
            sessions_count = cursor.rowcount
            
            cursor.execute("DELETE FROM metrics")
            metrics_count = cursor.rowcount
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='behavioral_patterns'")
            if cursor.fetchone():
                cursor.execute("DELETE FROM behavioral_patterns")
            
            # Limpar code_snapshots
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='code_snapshots'")
            if cursor.fetchone():
                cursor.execute("DELETE FROM code_snapshots")
            
            conn.commit()
            conn.close()
            
            flash(f'Dados ANALYSIS limpos com sucesso! {events_count} eventos, {sessions_count} sessões e {metrics_count} métricas removidas.', 'success')
            logger.info("ANALYSIS data cleared successfully", 
                       events_removed=events_count,
                       sessions_removed=sessions_count,
                       metrics_removed=metrics_count)
            
        except Exception as e:
            logger.error("Failed to clear ANALYSIS data", error=str(e), exc_info=True)
            flash(f'Erro ao limpar dados ANALYSIS: {str(e)}', 'danger')
        
        return redirect(url_for('import_tko_data'))
    
    @app.route('/clear_database', methods=['POST'])
    def clear_database():
        """Limpa todas as tabelas do banco de dados."""
        try:
            conn = get_db()
            cursor = conn.cursor()
            
            # Desabilitar foreign keys temporariamente
            cursor.execute("PRAGMA foreign_keys = OFF")
            
            # Listar todas as tabelas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Limpar cada tabela
            for table in tables:
                if table != 'sqlite_sequence':
                    cursor.execute(f"DELETE FROM {table}")
                    logger.info("Table cleared", table=table)
            
            # Reabilitar foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")
            
            conn.commit()
            conn.close()
            
            # Limpar diretório data/
            try:
                import shutil
                data_dir = Path("data")
                if data_dir.exists():
                    for item in data_dir.iterdir():
                        if item.is_dir() and item.name != '.gitkeep':
                            shutil.rmtree(item)
                            logger.info("Temporary data directory removed", path=str(item))
                        elif item.is_file() and item.name != '.gitkeep':
                            item.unlink()
                            logger.info("Temporary data file removed", path=str(item))
                    logger.info("Data directory cleaned")
            except Exception as e:
                logger.warning("Failed to clean data directory", error=str(e))
            
            flash('Banco de dados limpo com sucesso! Todas as tabelas foram esvaziadas.', 'success')
            logger.info("Database cleared successfully")
            
        except Exception as e:
            logger.error("Failed to clear database", error=str(e), exc_info=True)
            flash(f'Erro ao limpar banco de dados: {str(e)}', 'danger')
        
        # Redireciona para home (mostrar wizard de configuração)
        return render_template('setup_wizard.html')
    
    
    @app.route('/api/browse_directory', methods=['POST'])
    def browse_directory():
        """API para navegar pelo sistema de arquivos."""
        import json
        from pathlib import Path
        
        data = request.get_json()
        current_path = data.get('path', '')
        
        try:
            # Se path vazio, listar drives no Windows ou root no Unix
            if not current_path:
                import platform
                if platform.system() == 'Windows':
                    import string
                    drives = []
                    for letter in string.ascii_uppercase:
                        drive = f"{letter}:\\"
                        if Path(drive).exists():
                            drives.append({
                                'name': drive,
                                'path': drive,
                                'is_dir': True
                            })
                    return jsonify({
                        'current_path': '',
                        'parent_path': None,
                        'items': drives
                    })
                else:
                    current_path = '/'
            
            path = Path(current_path)
            
            if not path.exists():
                return jsonify({'error': 'Diretório não encontrado'}), 404
            
            if not path.is_dir():
                return jsonify({'error': 'Caminho não é um diretório'}), 400
            
            # Listar itens do diretório
            items = []
            try:
                for item in sorted(path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
                    if item.is_dir():
                        # Filtrar diretórios ocultos e sistema
                        if not item.name.startswith('.') and not item.name.startswith('$'):
                            items.append({
                                'name': item.name,
                                'path': str(item),
                                'is_dir': True
                            })
            except PermissionError:
                pass
            
            # Parent path
            parent_path = str(path.parent) if path.parent != path else None
            
            return jsonify({
                'current_path': str(path),
                'parent_path': parent_path,
                'items': items
            })
            
        except Exception as e:
            logger.error("Browse directory failed", error=str(e), exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/process_etl', methods=['POST'])
    def process_etl():
        """
        Processa ETL completo: detecta sessões e calcula métricas.
        
        Este endpoint:
        1. Lê todos os eventos do banco de dados
        2. Agrupa por estudante
        3. Detecta sessões usando SessionDetector
        4. Calcula métricas usando MetricsEngine
        5. Popula tabelas sessions e metrics
        
        Returns:
            JSON com estatísticas do processamento
        """
        try:
            logger.info("ETL processing started")
            
            # Conectar ao banco
            conn = get_db()
            
            # Verificar se há eventos ANALYSIS para processar
            count = conn.execute('SELECT COUNT(*) as count FROM analysis_events').fetchone()['count']
            if count == 0:
                conn.close()
                return jsonify({
                    'success': False,
                    'error': 'Nenhum evento ANALYSIS encontrado no banco de dados. Importe dados ANALYSIS primeiro.'
                }), 400
            
            # Limpar tabelas sessions e metrics antes de reprocessar
            conn.execute('DELETE FROM sessions')
            conn.execute('DELETE FROM metrics')
            conn.commit()
            logger.info("Previous sessions and metrics cleared")
            
            # Buscar todos os eventos ANALYSIS agrupados por estudante
            events_query = """
                SELECT 
                    student_hash,
                    case_id,
                    task_id,
                    event_type,
                    timestamp,
                    activity,
                    metadata
                FROM analysis_events
                ORDER BY student_hash, timestamp
            """
            
            rows = conn.execute(events_query).fetchall()
            
            # Agrupar eventos por estudante
            students_events = {}
            students_case_ids = {}  # Mapeia (student, event_idx) -> case_id
            for row in rows:
                student_hash = row['student_hash']
                if student_hash not in students_events:
                    students_events[student_hash] = []
                    students_case_ids[student_hash] = []
                
                # Parse metadata JSON
                import json
                metadata = json.loads(row['metadata']) if row['metadata'] else {}
                
                timestamp = datetime.fromisoformat(row['timestamp'])
                
                # Criar evento apropriado baseado no event_type
                if row['event_type'] == 'ExecEvent':
                    from src.models.events import ExecEvent
                    event = ExecEvent(
                        timestamp=timestamp,
                        task_id=row['task_id'],
                        mode=metadata.get('mode', 'FREE'),
                        rate=metadata.get('rate'),
                        size=metadata.get('size', 0),
                        error=metadata.get('error', 'NONE')
                    )
                elif row['event_type'] == 'MoveEvent':
                    from src.models.events import MoveEvent
                    event = MoveEvent(
                        timestamp=timestamp,
                        task_id=row['task_id'],
                        action=metadata.get('action', 'EDIT')
                    )
                elif row['event_type'] == 'SelfEvent':
                    from src.models.events import SelfEvent
                    event = SelfEvent(
                        timestamp=timestamp,
                        task_id=row['task_id'],
                        rate=metadata.get('rate'),
                        autonomy=metadata.get('autonomy', 'MEDIUM'),
                        study_minutes=metadata.get('study_minutes', 0)
                    )
                else:
                    continue
                
                students_events[student_hash].append(event)
                students_case_ids[student_hash].append(row['case_id'])
            
            # Processar cada estudante
            session_detector = SessionDetector(timeout_minutes=30)
            metrics_engine = MetricsEngine(session_timeout_minutes=30)
            
            total_students = len(students_events)
            total_sessions = 0
            total_metrics = 0
            
            for idx, (student_hash, events) in enumerate(students_events.items(), 1):
                case_ids = students_case_ids[student_hash]
                
                logger.info(
                    "Processing student",
                    student=student_hash[:8],
                    progress=f"{idx}/{total_students}",
                    events_count=len(events)
                )
                
                # Detectar sessões e grupar por case_id primeiro
                cases = {}
                for event_idx, event in enumerate(events):
                    case_id = case_ids[event_idx]
                    if case_id not in cases:
                        cases[case_id] = []
                    cases[case_id].append(event)
                
                all_sessions = []
                for case_id, case_events in cases.items():
                    sessions = session_detector.detect_sessions(
                        events=case_events,
                        case_id=case_id,
                        student_id=student_hash
                    )
                    all_sessions.extend(sessions)
                
                # Inserir sessões no banco
                if all_sessions:
                    for session in all_sessions:
                        conn.execute(
                            """
                            INSERT INTO sessions (
                                id, case_id, student_hash, task_id,
                                start_timestamp, end_timestamp, duration_seconds,
                                event_count, exec_count, move_count, self_count
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            session.to_db_row()
                        )
                    total_sessions += len(all_sessions)
                
                # Calcular métricas por tarefa
                tasks = {}
                for event_idx, event in enumerate(events):
                    task_id = event.task_id
                    if task_id not in tasks:
                        tasks[task_id] = {'events': [], 'case_ids': []}
                    tasks[task_id]['events'].append(event)
                    tasks[task_id]['case_ids'].append(case_ids[event_idx])
                
                for task_id, task_data in tasks.items():
                    task_events = task_data['events']
                    # Filtrar sessões desta tarefa
                    task_sessions = [s for s in all_sessions if s.task_id == task_id]
                    case_id = task_data['case_ids'][0] if task_data['case_ids'] else "unknown"
                    
                    # Calcular métricas
                    metrics = metrics_engine.compute_all_metrics(
                        events=task_events,
                        sessions=task_sessions,
                        case_id=case_id,
                        student_id=student_hash,
                        task_id=task_id
                    )
                    
                    # Inserir métricas no banco
                    if metrics:
                        for metric in metrics:
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO metrics (
                                    id, case_id, student_hash, task_id,
                                    metric_name, metric_value, metadata, computed_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                metric.to_db_row()
                            )
                        total_metrics += len(metrics)
            
            # Commit final
            conn.commit()
            conn.close()
            
            logger.info(
                "ETL processing completed",
                students=total_students,
                sessions=total_sessions,
                metrics=total_metrics
            )
            
            return jsonify({
                'success': True,
                'students_processed': total_students,
                'sessions_created': total_sessions,
                'metrics_calculated': total_metrics
            })
            
        except Exception as e:
            logger.error("ETL processing failed", error=str(e), exc_info=True)
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/process_mining/available_tasks', methods=['GET'])
    def get_available_tasks():
        """
        Retorna lista de tarefas disponíveis nos dados MODEL.
        
        GET /api/process_mining/available_tasks
        
        Returns:
            JSON com lista de tarefas e estatísticas
        """
        from src.process_mining import ProcessModelGenerator
        
        try:
            generator = ProcessModelGenerator(db_path=current_app.config['DB_PATH'])
            tasks = generator.get_available_tasks()
            
            # Adiciona estatísticas globais
            result = {
                'success': True,
                'tasks': tasks,
                'global_stats': {
                    'total_events': sum(t['event_count'] for t in tasks),
                    'total_students': len(set([t['student_count'] for t in tasks])),
                    'total_tasks': len(tasks)
                }
            }
            
            logger.info("[get_available_tasks] - Success", total_tasks=len(tasks))
            return jsonify(result)
        
        except Exception as e:
            logger.error("[get_available_tasks] - Error", error=str(e))
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/process_mining/students_by_task/<task_id>', methods=['GET'])
    def get_students_by_task(task_id: str):
        """
        Retorna lista de estudantes que têm eventos ANALYSIS para uma tarefa específica.
        
        GET /api/process_mining/students_by_task/<task_id>
        
        Returns:
            JSON com lista de estudantes
        """
        try:
            conn = get_db()
            
            # Buscar estudantes com eventos ANALYSIS para a tarefa
            students = conn.execute("""
                SELECT DISTINCT student_hash, student_name, COUNT(*) as event_count
                FROM analysis_events
                WHERE task_id = ?
                GROUP BY student_hash, student_name
                ORDER BY student_name
            """, (task_id,)).fetchall()
            
            conn.close()
            
            result = {
                'success': True,
                'task_id': task_id,
                'students': [
                    {
                        'student_hash': s['student_hash'],
                        'student_name': s['student_name'] or 'Unknown',
                        'event_count': s['event_count']
                    } for s in students
                ]
            }
            
            logger.info("[get_students_by_task] - Success", 
                       task_id=task_id,
                       student_count=len(students))
            
            return jsonify(result)
        
        except Exception as e:
            logger.error("[get_students_by_task] - Error", 
                        task_id=task_id,
                        error=str(e))
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/process_mining/generate_model', methods=['POST'])
    def generate_process_model():
        """
        Gera modelo de processo a partir dos eventos MODEL.
        
        POST /api/process_mining/generate_model
        Body: {
            "noise_threshold": 0.2,  (opcional, padrão: 0.2)
            "task_id": "motoca"      (opcional, null = modelo global)
        }
        
        Returns:
            JSON com estatísticas do modelo e DFG
        """
        from src.process_mining import ProcessModelGenerator, ModelGenerationError
        
        try:
            data = request.get_json() or {}
            noise_threshold = data.get('noise_threshold', 0.2)
            task_id = data.get('task_id', None)
            
            # Validação: task_id é obrigatório
            if not task_id:
                return jsonify({
                    'success': False,
                    'error': 'Parâmetro "task_id" é obrigatório. Selecione uma tarefa para gerar o modelo.'
                }), 400
            
            logger.info("[generate_process_model] - Starting model generation",
                       noise_threshold=noise_threshold,
                       task_id=task_id)
            
            # Inicializa gerador
            generator = ProcessModelGenerator(db_path=current_app.config['DB_PATH'])
            
            # Verifica estatísticas antes (com filtro opcional)
            stats = generator.get_model_statistics(task_id=task_id)
            
            if stats['total_events'] == 0:
                error_msg = f'Nenhum evento MODEL encontrado'
                if task_id:
                    error_msg += f' para a tarefa "{task_id}"'
                error_msg += '. Importe dados com dataset_role="model" primeiro.'
                return jsonify({
                    'success': False,
                    'error': error_msg
                }), 400
            
            # Gera modelo (filtrado ou global)
            net, initial_marking, final_marking = generator.generate_model(
                noise_threshold=noise_threshold,
                task_id=task_id
            )
            
            # Gera DFG para estatísticas (com mesmo filtro)
            dfg, start_activities, end_activities = generator.get_dfg(task_id=task_id)
            
            # Converter DFG para formato serializável em JSON
            # DFG usa tuplas como chaves: {('A', 'B'): 5} -> {"A->B": 5}
            dfg_serializable = {f"{k[0]}->{k[1]}": v for k, v in dfg.items()}
            start_activities_serializable = {k: v for k, v in start_activities.items()}
            end_activities_serializable = {k: v for k, v in end_activities.items()}
            
            # Salva modelo E DFG na sessão Flask para uso posterior
            from flask import session
            session['process_model'] = {
                'generated_at': datetime.now().isoformat(),
                'noise_threshold': noise_threshold,
                'task_id': task_id,
                'scope': task_id or 'global',
                'stats': stats,
                'dfg': {
                    'activities': dfg_serializable,
                    'start_activities': start_activities_serializable,
                    'end_activities': end_activities_serializable
                }
            }
            
            logger.info("[generate_process_model] - Model generated successfully",
                       places=len(net.places),
                       transitions=len(net.transitions),
                       scope=task_id or "global")
            
            return jsonify({
                'success': True,
                'model_info': {
                    'places': len(net.places),
                    'transitions': len(net.transitions),
                    'arcs': len(net.arcs),
                    'scope': task_id or 'global',
                    'task_id': task_id
                },
                'statistics': stats,
                'dfg_transitions': len(dfg)
            })
        
        except ModelGenerationError as e:
            logger.error("[generate_process_model] - Model generation failed",
                        error=str(e))
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        
        except Exception as e:
            logger.error("[generate_process_model] - Unexpected error",
                        error=str(e),
                        exc_info=True)
            return jsonify({
                'success': False,
                'error': f"Erro inesperado: {str(e)}"
            }), 500
    
    @app.route('/api/process_mining/conformance_analysis', methods=['POST'])
    def conformance_analysis():
        """
        Executa análise de conformidade para estudante+tarefa específicos.
        
        POST /api/process_mining/conformance_analysis
        Body: {
            "task_id": "motoca",         (obrigatório)
            "student_hash": "abc123..."  (obrigatório)
        }
        
        Returns:
            JSON com resultados do replay
        """
        from src.process_mining import (
            ProcessModelGenerator,
            ConformanceReplayer,
            ReplayError
        )
        
        try:
            data = request.get_json() or {}
            requested_task_id = data.get('task_id', None)
            requested_student_hash = data.get('student_hash', None)
            
            # Validação de parâmetros obrigatórios
            if not requested_task_id:
                return jsonify({
                    'success': False,
                    'error': 'Parâmetro "task_id" é obrigatório para análise de conformidade.'
                }), 400
            
            if not requested_student_hash:
                return jsonify({
                    'success': False,
                    'error': 'Parâmetro "student_hash" é obrigatório para análise de conformidade.'
                }), 400
            
            # Verifica se há modelo gerado na sessão
            from flask import session
            model_info = session.get('process_model', None)
            
            if not model_info:
                return jsonify({
                    'success': False,
                    'error': 'Nenhum modelo gerado. Gere um modelo primeiro usando /api/process_mining/generate_model'
                }), 400
            
            model_scope = model_info.get('task_id', None)  # None = global
            
            # Validação: se modelo é específico, análise deve ser da mesma tarefa
            if model_scope and model_scope != requested_task_id:
                return jsonify({
                    'success': False,
                    'error': f'Modelo gerado para tarefa "{model_scope}", '
                            f'mas análise solicitada para "{requested_task_id}". '
                            f'Gere um novo modelo ou ajuste o filtro.'
                }), 400
            
            logger.info("[conformance_analysis] - Starting conformance analysis",
                       task_id=requested_task_id,
                       student_hash=requested_student_hash[:16] + "...",
                       model_scope=model_scope or "global")
            
            # 1. Regenera modelo DIRETAMENTE do banco (evita bug de serialização)
            generator = ProcessModelGenerator(db_path=current_app.config['DB_PATH'])
            
            # Gerar DFG primeiro
            dfg, start_activities, end_activities = generator.get_dfg(task_id=model_scope)
            
            logger.info("[conformance_analysis] - DFG generated for reference model",
                       dfg_edges=len(dfg),
                       start_activities=list(start_activities.keys()),
                       end_activities=list(end_activities.keys()))
            
            # Converter DFG para Petri Net
            net, initial_marking, final_marking = pm4py.convert_to_petri_net(
                dfg, start_activities, end_activities
            )
            
            logger.info("[conformance_analysis] - Petri Net created",
                       places=len(net.places),
                       transitions=len(net.transitions),
                       arcs=len(net.arcs))
            
            # 2. Inicializa replayer
            replayer = ConformanceReplayer(
                db_path=current_app.config['DB_PATH'],
                net=net,
                initial_marking=initial_marking,
                final_marking=final_marking,
                loop_threshold=5
            )
            
            # 3. Executa replay para estudante+tarefa específicos
            results = replayer.replay_all_students(
                task_id=requested_task_id,
                model_scope=model_scope
            )
            
            # Filtrar resultado para o estudante específico
            student_result = next(
                (m for m in results if m.student_hash == requested_student_hash),
                None
            )
            
            if not student_result:
                return jsonify({
                    'success': False,
                    'error': f'Nenhum evento ANALYSIS encontrado para estudante "{requested_student_hash[:16]}..." na tarefa "{requested_task_id}".'
                }), 404
            
            # 4. Salva métrica no banco
            try:
                replayer.save_conformance_metrics(student_result)
                saved_count = 1
            except Exception as e:
                logger.warning("[conformance_analysis] - Failed to save metrics",
                              case_id=student_result.case_id,
                              error=str(e))
                saved_count = 0
            
            # 5. Armazena informações na sessão para visualização posterior
            session['last_conformance_analysis'] = {
                'student_hash': requested_student_hash,
                'task_id': requested_task_id,
                'fitness': student_result.fitness,
                'analyzed_at': datetime.now().isoformat()
            }
            
            logger.info("[conformance_analysis] - Conformance analysis completed",
                       fitness=student_result.fitness,
                       deviations=student_result.deviations_count)
            
            return jsonify({
                'success': True,
                'student_hash': requested_student_hash,
                'task_id': requested_task_id,
                'metrics_saved': saved_count,
                'model_scope': model_scope or 'global',
                'metrics': {
                    'fitness': round(student_result.fitness, 4),
                    'deviations_count': student_result.deviations_count,
                    'excessive_loops_count': student_result.excessive_loops_count,
                    'trace_length': student_result.trace_length
                }
            })
        
        except ReplayError as e:
            logger.error("[conformance_analysis] - Replay error", error=str(e))
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
        
        except Exception as e:
            logger.error("[conformance_analysis] - Unexpected error",
                        error=str(e),
                        exc_info=True)
            return jsonify({
                'success': False,
                'error': f"Erro inesperado: {str(e)}"
            }), 500
    
    @app.route('/api/process_mining/visualize_global', methods=['GET'])
    def visualize_global_process():
        """
        Gera visualização V1: Modelo de Processo já gerado (DFG).
        
        GET /api/process_mining/visualize_global
        
        Returns:
            HTML com gráfico Plotly embarcado
        """
        from src.process_mining import ProcessVisualizer
        from flask import session
        
        try:
            # Recuperar DFG do modelo gerado da sessão
            model_info = session.get('process_model', None)
            
            if not model_info or 'dfg' not in model_info:
                return jsonify({
                    'success': False,
                    'error': 'Nenhum modelo gerado. Gere um modelo primeiro na aba "Gerar Modelo de Processo".'
                }), 400
            
            dfg_data = model_info['dfg']
            task_id = model_info.get('task_id', None)
            
            # Converter DFG de volta para formato PM4Py (strings -> tuplas)
            # {"A->B": 5} -> {('A', 'B'): 5}
            dfg_reconverted = {}
            for key_str, value in dfg_data['activities'].items():
                parts = key_str.split('->')
                if len(parts) == 2:
                    dfg_reconverted[(parts[0], parts[1])] = value
            
            scope_label = f"📋 {task_id}" if task_id else "🌍 Global"
            logger.info("[visualize_global_process] - Displaying pre-generated model", 
                       task_id=task_id or "ALL_TASKS",
                       scope=scope_label)
            
            # Cria visualização com DFG já gerado
            visualizer = ProcessVisualizer()
            title = f"V1: Modelo de Processo - {scope_label} (Dataset MODEL)"
            
            svg = visualizer.visualize_global_dfg(
                dfg=dfg_reconverted,
                start_activities=dfg_data['start_activities'],
                end_activities=dfg_data['end_activities'],
                title=title
            )
            
            # Buscar eventos MODEL que geraram o modelo
            conn = sqlite3.connect(current_app.config['DB_PATH'])
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if task_id:
                cursor.execute("""
                    SELECT timestamp, activity, event_type, student_hash, student_name, metadata
                    FROM model_events
                    WHERE task_id = ?
                    ORDER BY timestamp ASC
                """, (task_id,))
            else:
                cursor.execute("""
                    SELECT timestamp, activity, event_type, student_hash, student_name, task_id, metadata
                    FROM model_events
                    ORDER BY timestamp ASC
                """)
            
            model_events = cursor.fetchall()
            conn.close()
            
            # Criar HTML da listagem de eventos
            events_html = '<div class="events-list"><h3>Eventos MODEL (Ordem Cronológica)</h3><table>'
            events_html += '<thead><tr><th>#</th><th>Timestamp</th><th>Atividade</th><th>Tipo</th><th>Estudante</th>'
            if not task_id:
                events_html += '<th>Tarefa</th>'
            events_html += '</tr></thead><tbody>'
            
            for idx, event in enumerate(model_events, 1):
                ts = event['timestamp'][:19] if event['timestamp'] else ''
                student_display = event['student_name'] if event['student_name'] else f"{event['student_hash'][:12]}..."
                events_html += f'<tr><td>{idx}</td><td>{ts}</td><td>{event["activity"]}</td>'
                events_html += f'<td>{event["event_type"]}</td><td>{student_display}</td>'
                if not task_id:
                    events_html += f'<td>{event["task_id"]}</td>'
                events_html += '</tr>'
            
            events_html += '</tbody></table></div>'
            
            # Retorna HTML com SVG interativo
            html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/svg-pan-zoom@3.6.1/dist/svg-pan-zoom.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
        }}
        #svg-container {{
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        #svg-container svg {{
            max-width: 100%;
            height: auto;
        }}
        .controls {{
            margin-bottom: 15px;
            padding: 10px;
            background: white;
            border-radius: 4px;
            border: 1px solid #ddd;
        }}
        button {{
            padding: 8px 16px;
            margin-right: 10px;
            cursor: pointer;
            background: #007bff;
            color: white;
            border: none;
            border-radius: 4px;
        }}
        button:hover {{
            background: #0056b3;
        }}
        .events-list {{
            margin-top: 30px;
            padding: 20px;
            background: white;
            border-radius: 4px;
            border: 1px solid #ddd;
        }}
        .events-list h3 {{
            margin-top: 0;
            color: #333;
        }}
        .events-list table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        .events-list th, .events-list td {{
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        .events-list th {{
            background: #f8f9fa;
            font-weight: bold;
            color: #555;
        }}
        .events-list tr:hover {{
            background: #f8f9fa;
        }}
        .events-list code {{
            background: #f0f0f0;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="controls">
        <button onclick="panZoomInstance.zoom(1.2)">Zoom In (+)</button>
        <button onclick="panZoomInstance.zoom(0.8)">Zoom Out (-)</button>
        <button onclick="panZoomInstance.reset()">Reset View</button>
        <button onclick="panZoomInstance.center()">Center</button>
    </div>
    <div id="svg-container">{svg}</div>
    {events_html}
    <script>
        var panZoomInstance = svgPanZoom('#svg-container svg', {{
            zoomEnabled: true,
            controlIconsEnabled: true,
            fit: true,
            center: true,
            minZoom: 0.1,
            maxZoom: 10
        }});
    </script>
</body>
</html>
            """
            
            return html
        
        except Exception as e:
            logger.error("[visualize_global_process] - Visualization failed",
                        error=str(e),
                        exc_info=True)
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/process_mining/visualize_student/<student_hash>/<task_id>', methods=['GET'])
    def visualize_student_trace(student_hash: str, task_id: str):
        """
        Gera visualização V2: Trajetória Individual de um estudante.
        
        GET /api/process_mining/visualize_student/<student_hash>/<task_id>
        
        Returns:
            HTML com gráfico Plotly embarcado
        """
        from src.process_mining import (
            ProcessModelGenerator,
            ConformanceReplayer,
            ProcessVisualizer
        )
        from flask import session
        
        try:
            logger.info("[visualize_student_trace] - Generating V2",
                       student_hash=student_hash[:8],
                       task_id=task_id)
            
            # 1. SOLUÇÃO: Regenerar modelo ao invés de usar sessão (evita bug de serialização JSON)
            model_info = session.get('process_model', None)
            
            if not model_info:
                return jsonify({
                    'success': False,
                    'error': 'Nenhum modelo gerado. Gere um modelo primeiro na aba "Gerar Modelo de Processo".'
                }), 400
            
            # Obter parâmetros do modelo da sessão
            model_scope = model_info.get('task_id', None)
            noise_threshold = model_info.get('noise_threshold', 0.2)
            
            logger.info("[visualize_student_trace] - Regenerating model from database",
                       model_scope=model_scope or "global",
                       noise_threshold=noise_threshold)
            
            # Regenerar modelo DIRETAMENTE do banco (não usar sessão corrompida)
            generator = ProcessModelGenerator(db_path=current_app.config['DB_PATH'])
            dfg, start_activities, end_activities = generator.get_dfg(task_id=model_scope)
            
            # Converter DFG para Petri Net (conversão limpa, sem passar por JSON)
            net, initial_marking, final_marking = pm4py.convert_to_petri_net(
                dfg, start_activities, end_activities
            )
            
            logger.info("[visualize_student_trace] - Model regenerated successfully",
                       dfg_edges=len(dfg),
                       start_activities=len(start_activities),
                       end_activities=len(end_activities),
                       net_places=len(net.places),
                       net_transitions=len(net.transitions))
            
            # 2. Busca trace do estudante
            replayer = ConformanceReplayer(
                db_path=current_app.config['DB_PATH'],
                net=net,
                initial_marking=initial_marking,
                final_marking=final_marking
            )
            
            events = replayer._fetch_analysis_events(student_hash, task_id)
            
            if not events:
                return jsonify({
                    'success': False,
                    'error': f'Nenhum evento ANALYSIS encontrado para estudante {student_hash[:8]} na tarefa {task_id}'
                }), 404
            
            case_id = events[0]['case_id']
            trace = replayer._events_to_trace(events, case_id)
            
            # 3. Cria visualização de trajetória
            visualizer = ProcessVisualizer()
            svg_trajectory = visualizer.visualize_student_trace(
                dfg=dfg,
                start_activities=start_activities,
                end_activities=end_activities,
                student_trace=trace,
                student_hash=student_hash,
                task_id=task_id
            )
            
            # 4. Cria visualização do modelo de referência
            svg_reference = visualizer.visualize_global_dfg(
                dfg=dfg,
                start_activities=start_activities,
                end_activities=end_activities,
                title=f"Modelo de Referência - {task_id}"
            )
            
            # 5. Buscar métricas de conformidade do banco de dados
            conn = sqlite3.connect(current_app.config['DB_PATH'])
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Buscar métricas do estudante
            cursor.execute("""
                SELECT metric_name, metric_value, metadata
                FROM metrics
                WHERE student_hash = ? AND task_id = ?
                  AND metric_name IN (
                      'conformance_fitness', 
                      'conformance_deviations', 
                      'conformance_excessive_loops', 
                      'conformance_trace_length',
                      'conformance_missing_tokens',
                      'conformance_remaining_tokens',
                      'conformance_consumed_tokens',
                      'conformance_produced_tokens'
                  )
                ORDER BY computed_at DESC
            """, (student_hash, task_id))
            
            metrics_rows = cursor.fetchall()
            metrics_dict = {}
            for row in metrics_rows:
                metrics_dict[row['metric_name']] = row['metric_value']
            
            # Extrair métricas
            fitness = float(metrics_dict.get('conformance_fitness', 0.0))
            trace_length = int(metrics_dict.get('conformance_trace_length', len(events)))
            missing_tokens = int(metrics_dict.get('conformance_missing_tokens', 0))
            remaining_tokens = int(metrics_dict.get('conformance_remaining_tokens', 0))
            consumed_tokens = int(metrics_dict.get('conformance_consumed_tokens', 0))
            produced_tokens = int(metrics_dict.get('conformance_produced_tokens', 0))
            
            logger.info("[visualize_student_trace] - Metrics retrieved",
                       fitness=fitness,
                       missing_tokens=missing_tokens,
                       remaining_tokens=remaining_tokens,
                       consumed_tokens=consumed_tokens,
                       produced_tokens=produced_tokens)
            
            # Criar card de métricas com TODAS as métricas de conformidade
            metrics_card_html = f'''
            <div class="metrics-card">
                <h3><i class="fas fa-chart-line"></i> Métricas de Conformidade</h3>
                <div class="metrics-grid">
                    <div class="metric-item" title="Fitness indica o grau de aderência da trajetória do estudante ao modelo de processo esperado. Valores próximos de 1.0 (100%) indicam conformidade perfeita, enquanto valores baixos sugerem muitos desvios do comportamento esperado. É calculado como: 1 - (missing_tokens + remaining_tokens) / (consumed_tokens + missing_tokens + remaining_tokens).">
                        <div class="metric-label">Fitness</div>
                        <div class="metric-value">{fitness:.2f}</div>
                        <div class="metric-badge {'badge-success' if fitness >= 0.9 else 'badge-warning' if fitness >= 0.7 else 'badge-danger'}">
                            {'Excelente' if fitness >= 0.9 else 'Bom' if fitness >= 0.7 else 'Moderado' if fitness >= 0.5 else 'Baixo'}
                        </div>
                    </div>
                    <div class="metric-item" title="Missing Tokens representam atividades esperadas pelo modelo que não foram executadas pelo estudante. Indica que o estudante pulou etapas obrigatórias ou necessárias. Valores altos sugerem que o estudante não seguiu o processo completo ou tomou atalhos inadequados.">
                        <div class="metric-label">Missing Tokens</div>
                        <div class="metric-value">{missing_tokens}</div>
                        <div class="metric-badge {'badge-success' if missing_tokens == 0 else 'badge-danger'}">
                            {'Nenhuma atividade pulada' if missing_tokens == 0 else f'{missing_tokens} atividade(s) pulada(s)'}
                        </div>
                    </div>
                    <div class="metric-item" title="Remaining Tokens representam atividades que foram iniciadas mas não finalizadas corretamente. Indica que o processo não foi completado até o final esperado. Valores maiores que zero sugerem abandono da tarefa ou processo incompleto.">
                        <div class="metric-label">Remaining Tokens</div>
                        <div class="metric-value">{remaining_tokens}</div>
                        <div class="metric-badge {'badge-success' if remaining_tokens == 0 else 'badge-warning'}">
                            {'Processo completo' if remaining_tokens == 0 else f'{remaining_tokens} atividade(s) incompleta(s)'}
                        </div>
                    </div>
                    <div class="metric-item" title="Consumed Tokens representam o número total de atividades que foram executadas com sucesso e que estão alinhadas com o modelo de processo. Valores mais altos indicam maior número de atividades válidas realizadas pelo estudante.">
                        <div class="metric-label">Consumed Tokens</div>
                        <div class="metric-value">{consumed_tokens}</div>
                        <div class="metric-badge badge-info">
                            {consumed_tokens} atividade(s) válida(s)
                        </div>
                    </div>
                    <div class="metric-item" title="Produced Tokens representam o número de tokens gerados durante a execução do processo. Normalmente deve ser igual ou próximo aos Consumed Tokens. Desequilíbrios significativos podem indicar problemas na execução ou no log de eventos.">
                        <div class="metric-label">Produced Tokens</div>
                        <div class="metric-value">{produced_tokens}</div>
                        <div class="metric-badge badge-info">
                            {produced_tokens} token(s) gerado(s)
                        </div>
                    </div>
                    <div class="metric-item" title="Eventos no Trace representa o número total de atividades registradas durante a execução da tarefa pelo estudante. Inclui todas as ações como navegação, execução de testes e autoavaliação. Um número muito alto comparado à média pode indicar dificuldades ou abordagem ineficiente.">
                        <div class="metric-label">Eventos no Trace</div>
                        <div class="metric-value">{trace_length}</div>
                        <div class="metric-badge badge-info">
                            {trace_length} evento(s) registrado(s)
                        </div>
                    </div>
                </div>
            </div>
            '''
            
            # 6. Criar listagens de eventos lado a lado
            # Eventos ANALYSIS (trajetória do estudante)
            analysis_events_html = '<div class="events-column"><h4>Eventos da Trajetória (ANALYSIS)</h4><table>'
            analysis_events_html += '<thead><tr><th>#</th><th>Timestamp</th><th>Atividade</th><th>Tipo</th><th>Estudante</th></tr></thead><tbody>'
            
            for idx, event in enumerate(events, 1):
                ts = event['timestamp'][:19] if event['timestamp'] else ''
                student_display = event.get('student_name') or f"{event.get('student_hash', 'N/A')[:12]}..."
                analysis_events_html += f'<tr><td>{idx}</td><td>{ts}</td><td>{event["activity"]}</td><td>{event["event_type"]}</td><td>{student_display}</td></tr>'
            
            analysis_events_html += '</tbody></table></div>'
            
            # Eventos MODEL (referência) - reutilizar conexão já aberta
            cursor.execute("""
                SELECT timestamp, activity, event_type, student_hash, student_name
                FROM model_events
                WHERE task_id = ?
                ORDER BY timestamp ASC
            """, (task_id,))
            
            model_events = cursor.fetchall()
            conn.close()
            
            model_events_html = '<div class="events-column"><h4>Eventos do Modelo (MODEL)</h4><table>'
            model_events_html += '<thead><tr><th>#</th><th>Timestamp</th><th>Atividade</th><th>Tipo</th><th>Estudante</th></tr></thead><tbody>'
            
            for idx, event in enumerate(model_events, 1):
                ts = event['timestamp'][:19] if event['timestamp'] else ''
                student_display = event['student_name'] if event['student_name'] else f"{event['student_hash'][:12]}..."
                model_events_html += f'<tr><td>{idx}</td><td>{ts}</td><td>{event["activity"]}</td><td>{event["event_type"]}</td><td>{student_display}</td></tr>'
            
            model_events_html += '</tbody></table></div>'
            
            # Retorna HTML com SVG interativo
            title = f"V2: Trajetória Individual - Estudante {student_hash[:8]} - Tarefa {task_id}"
            html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/svg-pan-zoom@3.6.1/dist/svg-pan-zoom.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
        }}
        #svg-container {{
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        #svg-container svg {{
            max-width: 100%;
            height: auto;
        }}
        .controls {{
            margin-bottom: 15px;
            padding: 10px;
            background: white;
            border-radius: 4px;
            border: 1px solid #ddd;
        }}
        button {{
            padding: 8px 16px;
            margin-right: 10px;
            cursor: pointer;
            background: #007bff;
            color: white;
            border: none;
            border-radius: 4px;
        }}
        button:hover {{
            background: #0056b3;
        }}
        .info {{
            margin-bottom: 15px;
            padding: 10px;
            background: #e3f2fd;
            border-radius: 4px;
            border-left: 4px solid #2196F3;
        }}
        .model-section {{
            margin-bottom: 30px;
            padding: 20px;
            background: white;
            border-radius: 4px;
            border: 1px solid #ddd;
        }}
        .model-section h3 {{
            margin-top: 0;
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }}
        #svg-container-trajectory, #svg-container-reference {{
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-top: 15px;
        }}
        #svg-container-trajectory svg, #svg-container-reference svg {{
            max-width: 100%;
            height: auto;
        }}
        .metrics-card {{
            margin-bottom: 30px;
            padding: 25px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            color: white;
        }}
        .metrics-card h3 {{
            margin-top: 0;
            margin-bottom: 20px;
            color: white;
            font-size: 1.5rem;
            border-bottom: 2px solid rgba(255,255,255,0.3);
            padding-bottom: 10px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
        }}
        .metric-item {{
            background: rgba(255,255,255,0.15);
            padding: 20px;
            border-radius: 6px;
            text-align: center;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.2);
            cursor: help;
            transition: all 0.3s ease;
        }}
        .metric-item:hover {{
            background: rgba(255,255,255,0.25);
            transform: translateY(-5px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.2);
        }}
        .metric-label {{
            font-size: 0.9rem;
            opacity: 0.9;
            margin-bottom: 10px;
            font-weight: 500;
        }}
        .metric-value {{
            font-size: 2.5rem;
            font-weight: bold;
            margin: 10px 0;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        .metric-badge {{
            display: inline-block;
            padding: 6px 12px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-top: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .badge-success {{
            background: rgba(40, 167, 69, 0.9);
            color: white;
        }}
        .badge-warning {{
            background: rgba(255, 193, 7, 0.9);
            color: #333;
        }}
        .badge-danger {{
            background: rgba(220, 53, 69, 0.9);
            color: white;
        }}
        .badge-info {{
            background: rgba(23, 162, 184, 0.9);
            color: white;
        }}
        .metric-description {{
            font-size: 0.8rem;
            opacity: 0.8;
            margin-top: 8px;
        }}
        .events-container {{
            display: flex;
            gap: 20px;
            margin-top: 30px;
        }}
        .events-column {{
            flex: 1;
            padding: 20px;
            background: white;
            border-radius: 4px;
            border: 1px solid #ddd;
        }}
        .events-column h4 {{
            margin-top: 0;
            color: #555;
            border-bottom: 2px solid #28a745;
            padding-bottom: 8px;
        }}
        .events-column table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        .events-column th, .events-column td {{
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        .events-column th {{
            background: #f8f9fa;
            font-weight: bold;
            color: #555;
        }}
        .events-column tr:hover {{
            background: #f8f9fa;
        }}
    </style>
</head>
<body>
    <div class="info">
        <strong>{title}</strong><br>
        Transições em <span style="color: #0000FF; font-weight: bold;">azul</span> indicam o caminho percorrido pelo estudante.
    </div>
    
    <div class="model-section">
        <h3>Trajetória do Estudante</h3>
        <div class="controls">
            <button onclick="panZoomInstanceTraj.zoom(1.2)">Zoom In (+)</button>
            <button onclick="panZoomInstanceTraj.zoom(0.8)">Zoom Out (-)</button>
            <button onclick="panZoomInstanceTraj.reset()">Reset View</button>
            <button onclick="panZoomInstanceTraj.center()">Center</button>
        </div>
        <div id="svg-container-trajectory">{svg_trajectory}</div>
    </div>
    
    <div class="model-section">
        <h3>Modelo de Referência</h3>
        <div class="controls">
            <button onclick="panZoomInstanceRef.zoom(1.2)">Zoom In (+)</button>
            <button onclick="panZoomInstanceRef.zoom(0.8)">Zoom Out (-)</button>
            <button onclick="panZoomInstanceRef.reset()">Reset View</button>
            <button onclick="panZoomInstanceRef.center()">Center</button>
        </div>
        <div id="svg-container-reference">{svg_reference}</div>
    </div>
    
    {metrics_card_html}
    
    <div class="events-container">
        {analysis_events_html}
        {model_events_html}
    </div>
    <script>
        var panZoomInstanceTraj = svgPanZoom('#svg-container-trajectory svg', {{
            zoomEnabled: true,
            controlIconsEnabled: true,
            fit: true,
            center: true,
            minZoom: 0.1,
            maxZoom: 10
        }});
        
        var panZoomInstanceRef = svgPanZoom('#svg-container-reference svg', {{
            zoomEnabled: true,
            controlIconsEnabled: true,
            fit: true,
            center: true,
            minZoom: 0.1,
            maxZoom: 10
        }});
    </script>
</body>
</html>
            """
            
            return html
        
        except Exception as e:
            logger.error("[visualize_student_trace] - Visualization failed",
                        error=str(e),
                        exc_info=True)
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/process_mining')
    def process_mining_page():
        """Página dedicada para Process Mining com controles e visualizações."""
        conn = get_db()
        
        # Estatísticas de datasets
        stats = {
            'model_events': conn.execute("SELECT COUNT(*) as count FROM model_events").fetchone()['count'],
            'analysis_events': conn.execute("SELECT COUNT(*) as count FROM analysis_events").fetchone()['count'],
            'model_traces': conn.execute("SELECT COUNT(DISTINCT case_id) as count FROM model_events").fetchone()['count'],
            'analysis_traces': conn.execute("SELECT COUNT(DISTINCT case_id) as count FROM analysis_events").fetchone()['count'],
            'conformance_metrics': conn.execute("SELECT COUNT(*) as count FROM metrics WHERE metric_name='conformance_fitness'").fetchone()['count'],
        }
        
        # Buscar lista de estudantes e tarefas para dropdown
        students = conn.execute("SELECT DISTINCT student_hash, student_name FROM analysis_events ORDER BY student_name").fetchall()
        tasks = conn.execute("SELECT DISTINCT task_id FROM analysis_events ORDER BY task_id").fetchall()
        
        # Buscar métricas de conformidade se existirem
        conformance_data = []
        if stats['conformance_metrics'] > 0:
            conformance_data = conn.execute("""
                SELECT student_hash, student_name, task_id, metric_value as fitness
                FROM metrics
                WHERE metric_name = 'conformance_fitness'
                ORDER BY fitness DESC
                LIMIT 20
            """).fetchall()
        
        conn.close()
        
        return render_template('process_mining.html', 
                             stats=stats,
                             students=[{'hash': s['student_hash'], 'name': s['student_name'] or 'Unknown'} for s in students],
                             tasks=[t['task_id'] for t in tasks],
                             conformance_data=conformance_data)
