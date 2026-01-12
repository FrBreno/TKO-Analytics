"""
Rotas do dashboard Flask.
"""

import os
import sqlite3
import structlog
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, jsonify, abort, current_app, request, flash

logger = structlog.get_logger()


def get_db():
    """Obtém conexão com banco de dados."""
    conn = sqlite3.connect(current_app.config['DB_PATH'])
    conn.row_factory = sqlite3.Row
    return conn


def has_events_in_database() -> bool:
    """
    Verifica se há eventos no banco de dados.
    
    Returns:
        True se houver pelo menos um evento, False caso contrário
    """
    conn = get_db()
    count = conn.execute('SELECT COUNT(*) as count FROM events').fetchone()['count']
    conn.close()
    return count > 0


def register_routes(app: Flask):
    """Registra todas as rotas no app Flask."""
    
    @app.route('/')
    def index():
        """Homepage com visão geral."""
        # Verificar se banco está vazio
        if not has_events_in_database():
            return render_template('setup_wizard.html')
        
        conn = get_db()
        total_events = conn.execute('SELECT COUNT(*) as count FROM events').fetchone()['count']
        
        # Estatísticas gerais
        stats = {
            'total_events': total_events,
            'total_students': conn.execute('SELECT COUNT(DISTINCT student_hash) as count FROM events').fetchone()['count'],
            'total_tasks': conn.execute('SELECT COUNT(DISTINCT task_id) as count FROM events').fetchone()['count'],
            'total_sessions': conn.execute('SELECT COUNT(*) as count FROM sessions').fetchone()['count'],
        }
        
        conn.close()
        
        return render_template('index.html', stats=stats)
    
    
    @app.route('/cohort')
    def cohort_overview():
        """Visão geral do cohort com heatmap."""
        conn = get_db()
        
        # Busca métricas por estudante e tarefa
        query = """
        SELECT 
            student_hash,
            task_id,
            metric_name,
            metric_value
        FROM metrics
        WHERE metric_name IN ('time_active_seconds', 'final_success_rate', 'attempts_to_success')
        ORDER BY student_hash, task_id
        """
        
        rows = conn.execute(query).fetchall()
        
        # Organiza dados para heatmap
        students = set()
        tasks = set()
        data = {}
        
        for row in rows:
            students.add(row['student_hash'])
            tasks.add(row['task_id'])
            key = (row['student_hash'], row['task_id'], row['metric_name'])
            data[key] = row['metric_value']
        
        students = sorted(students)
        tasks = sorted(tasks)
        
        # Cria heatmap de success rate
        heatmap_data = []
        for student in students:
            row_data = []
            for task in tasks:
                key = (student, task, 'final_success_rate')
                value = data.get(key, 0)
                row_data.append(value)
            heatmap_data.append(row_data)
        
        # Gera visualização Plotly
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data,
            x=tasks,
            y=[s[:8] for s in students],  # Trunca hash para legibilidade
            colorscale='RdYlGn',
            text=heatmap_data,
            texttemplate='%{text:.0f}%',
            textfont={"size": 10},
            colorbar=dict(title="Success Rate (%)")
        ))
        
        fig.update_layout(
            title='Cohort Success Rate Heatmap',
            xaxis_title='Task ID',
            yaxis_title='Student Hash',
            height=max(400, len(students) * 30),
        )
        
        heatmap_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        
        # Busca resumo de métricas
        summary_query = """
        SELECT 
            student_hash,
            COUNT(DISTINCT task_id) as tasks_attempted,
            AVG(CASE WHEN metric_name = 'final_success_rate' THEN metric_value END) as avg_success_rate,
            AVG(CASE WHEN metric_name = 'time_active_seconds' THEN metric_value END) as avg_time_active
        FROM metrics
        GROUP BY student_hash
        ORDER BY avg_success_rate DESC
        """
        
        students_summary = conn.execute(summary_query).fetchall()
        
        conn.close()
        
        return render_template(
            'cohort.html',
            heatmap=heatmap_html,
            students=students_summary,
            total_students=len(students),
            total_tasks=len(tasks)
        )
    
    
    @app.route('/student/<student_hash>')
    def student_detail(student_hash: str):
        """Detalhes de um estudante específico."""
        conn = get_db()
        
        # Verifica se estudante existe
        check = conn.execute(
            'SELECT COUNT(*) as count FROM events WHERE student_hash = ?',
            (student_hash,)
        ).fetchone()
        
        if check['count'] == 0:
            conn.close()
            abort(404, description="Estudante não encontrado")
        
        # Busca eventos do estudante
        events_query = """
        SELECT 
            id,
            timestamp,
            task_id,
            event_type,
            activity_name,
            metadata
        FROM events
        WHERE student_hash = ?
        ORDER BY timestamp ASC
        """
        
        events = conn.execute(events_query, (student_hash,)).fetchall()
        
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
    
    
    @app.route('/task/<task_id>')
    def task_analytics(task_id: str):
        """Análise agregada de uma tarefa."""
        conn = get_db()
        
        # Verifica se tarefa existe
        check = conn.execute(
            'SELECT COUNT(*) as count FROM events WHERE task_id = ?',
            (task_id,)
        ).fetchone()
        
        if check['count'] == 0:
            conn.close()
            abort(404, description="Tarefa não encontrada")
        
        # Estatísticas da tarefa
        stats_query = """
        SELECT 
            COUNT(DISTINCT student_hash) as total_students,
            COUNT(*) as total_events,
            AVG(CASE WHEN metric_name = 'final_success_rate' THEN metric_value END) as avg_success_rate,
            AVG(CASE WHEN metric_name = 'time_active_seconds' THEN metric_value END) as avg_time_active,
            AVG(CASE WHEN metric_name = 'attempts_to_success' THEN metric_value END) as avg_attempts
        FROM (
            SELECT DISTINCT student_hash FROM events WHERE task_id = ?
        ) students
        LEFT JOIN metrics ON metrics.student_hash = students.student_hash AND metrics.task_id = ?
        """
        
        stats = conn.execute(stats_query, (task_id, task_id)).fetchone()
        
        # Distribuição de success rate
        success_query = """
        SELECT metric_value as success_rate
        FROM metrics
        WHERE task_id = ? AND metric_name = 'final_success_rate'
        """
        
        success_rates = [row['success_rate'] for row in conn.execute(success_query, (task_id,)).fetchall()]
        
        # Cria histograma
        fig = go.Figure(data=[go.Histogram(
            x=success_rates,
            nbinsx=10,
            marker_color='steelblue',
            opacity=0.7
        )])
        
        fig.update_layout(
            title=f'Success Rate Distribution - Task: {task_id}',
            xaxis_title='Success Rate (%)',
            yaxis_title='Number of Students',
            height=400
        )
        
        histogram_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        
        # Lista de estudantes na tarefa
        students_query = """
        SELECT 
            e.student_hash,
            COUNT(e.id) as events_count,
            MAX(CASE WHEN m.metric_name = 'final_success_rate' THEN m.metric_value END) as success_rate,
            MAX(CASE WHEN m.metric_name = 'time_active_seconds' THEN m.metric_value END) as time_active
        FROM events e
        LEFT JOIN metrics m ON e.student_hash = m.student_hash AND e.task_id = m.task_id
        WHERE e.task_id = ?
        GROUP BY e.student_hash
        ORDER BY success_rate DESC
        """
        
        students = conn.execute(students_query, (task_id,)).fetchall()
        
        conn.close()
        
        return render_template(
            'task.html',
            task_id=task_id,
            stats=stats,
            histogram=histogram_html,
            students=students
        )
    
    
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
            output_name = request.form.get('output_name')
            import_mode = request.form.get('import_mode', 'incremental')
            
            # Verificar se há dados no banco
            has_data = has_events_in_database()
            
            # Validar modo incremental apenas se houver dados
            if import_mode == 'incremental' and not has_data:
                flash('Modo incremental não disponível: banco de dados vazio. Use modo limpa para primeira importação.', 'warning')
                return render_template('import.html', has_data=has_data)
            
            # Validar diretório
            root_path = Path(root_dir)
            if not root_path.exists():
                flash('Diretório não encontrado!', 'danger')
                return render_template('import.html', has_data=has_events_in_database())
            
            try:
                # Importar módulos
                from src.tko_integration.scanner import ClassroomScanner
                from src.tko_integration.transformer import TKOTransformer
                from src.tko_integration.validator import DataValidator
                
                # Se modo limpo, limpar diretório de saída
                output_dir = Path(f"tests/real_data/{output_name}")
                if import_mode == 'clean':
                    if output_dir.exists():
                        import shutil
                        logger.info("Cleaning output directory", path=str(output_dir))
                        shutil.rmtree(output_dir)
                        flash('Diretório de saída limpo.', 'info')
                
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
                csv_path = output_dir / "events.csv"
                
                # Verificar se arquivo já existe (modo incremental)
                file_exists = csv_path.exists()
                if file_exists and import_mode == 'incremental':
                    logger.info("Appending to existing CSV", path=str(csv_path))
                    flash('Modo incremental: adicionando eventos ao arquivo existente.', 'info')
                
                salt = os.getenv('STUDENT_ID_SALT', 'default-salt-change-me')
                transformer = TKOTransformer(salt)
                
                logger.info("Transforming to CSV", output=str(csv_path), mode=import_mode)
                
                # Para modo incremental, usar modo append
                write_mode = 'append' if (file_exists and import_mode == 'incremental') else 'new'
                total_events = transformer.transform_scan_to_csv(scan, csv_path, mode=write_mode)
                
                logger.info("Transformation complete", events=total_events, mode=import_mode)
                
                # Adicionar dados ao resultado
                scan.total_events = total_events
                scan.csv_path = str(csv_path)
                scan.import_mode = import_mode
                
                # Gerar relatório de validação
                validator = DataValidator()
                report = validator.generate_report(scan)
                logger.info("Validation report generated", warnings=len(scan.warnings))
                
                mode_msg = 'incremental' if import_mode == 'incremental' else 'limpa'
                flash(f'Importação {mode_msg} concluída! {total_events} eventos processados.', 'success')
                
                return render_template('import.html', scan_result=scan)
                
            except Exception as e:
                logger.error("Import failed", error=str(e), exc_info=True)
                flash(f'Erro durante importação: {str(e)}', 'danger')
                return render_template('import.html', has_data=has_events_in_database())
        
        # Verificar se há dados no banco para modo GET
        return render_template('import.html', has_data=has_events_in_database())
    
    
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
