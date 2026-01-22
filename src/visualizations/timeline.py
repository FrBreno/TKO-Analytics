"""
V.2 - Timeline Visualizer

Cria visualizações de timeline mostrando a evolução temporal do
desenvolvimento de cada estudante ao longo das tarefas.
"""

import structlog
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd

logger = structlog.get_logger()


class TimelineVisualizer:
    """Visualizador de timelines de desenvolvimento para TKO Analytics."""
    
    def __init__(self, db_path: str):
        """
        Inicializa o visualizador.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        logger.info("[TimelineVisualizer.__init__] - visualizer_initialized", db_path=db_path)
    
    def generate_student_timeline(
        self,
        student_hash: str,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera timeline de atividades para um estudante.
        
        Mostra:
        - Quando trabalhou em cada tarefa
        - Tipo de atividade (edit, exec, self-assess)
        - Duração das sessões
        
        Args:
            student_hash: Hash do estudante
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (Gantt-like chart)
        """
        logger.info(
            "[TimelineVisualizer.generate_student_timeline] - generating",
            student_hash=student_hash[:8]
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca sessões do estudante
            cursor.execute("""
                SELECT 
                    task_id,
                    start_timestamp,
                    end_timestamp,
                    duration_seconds,
                    exec_count,
                    move_count,
                    event_count
                FROM sessions
                WHERE student_hash = ?
                ORDER BY start_timestamp ASC
            """, (student_hash,))
            
            sessions = cursor.fetchall()
            
            if not sessions:
                logger.warning(
                    "[TimelineVisualizer.generate_student_timeline] - no_sessions_found",
                    student_hash=student_hash[:8]
                )
                return self._create_empty_figure()
            
            # Prepara dados para timeline
            data = []
            for session in sessions:
                task_id, start_ts, end_ts, duration, execs, moves, events = session
                
                # Parse timestamps
                try:
                    start_dt = datetime.fromisoformat(start_ts.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(end_ts.replace('Z', '+00:00'))
                except:
                    start_dt = datetime.strptime(start_ts, '%Y-%m-%d %H:%M:%S')
                    end_dt = datetime.strptime(end_ts, '%Y-%m-%d %H:%M:%S')
                
                # Determina atividade predominante
                if execs > moves:
                    activity_type = "Execute-heavy"
                    color = '#2ca02c'
                elif moves > execs:
                    activity_type = "Edit-heavy"
                    color = '#ff7f0e'
                else:
                    activity_type = "Mixed"
                    color = '#1f77b4'
                
                data.append({
                    'Task': task_id,
                    'Start': start_dt,
                    'Finish': end_dt,
                    'Activity': activity_type,
                    'Color': color,
                    'Duration': f"{duration}s",
                    'Events': events,
                    'Execs': execs,
                    'Edits': moves
                })
            
            # Cria DataFrame
            df = pd.DataFrame(data)
            
            # Cria timeline (Gantt chart)
            fig = px.timeline(
                df,
                x_start='Start',
                x_end='Finish',
                y='Task',
                color='Activity',
                hover_data=['Duration', 'Events', 'Execs', 'Edits'],
                title=f"Development Timeline - Student {student_hash[:8]}..."
            )
            
            fig.update_yaxes(categoryorder='total ascending')
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Task",
                height=max(400, len(df['Task'].unique()) * 40)
            )
            
            if output_html:
                fig.write_html(output_html)
                logger.info(
                    "[TimelineVisualizer.generate_student_timeline] - html_saved",
                    path=output_html
                )
            
            return fig
            
        finally:
            conn.close()
    
    def generate_task_timeline(
        self,
        task_id: str,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera timeline mostrando quando diferentes estudantes trabalharam
        na mesma tarefa (comparação temporal).
        
        Args:
            task_id: ID da tarefa
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure
        """
        logger.info(
            "[TimelineVisualizer.generate_task_timeline] - generating",
            task_id=task_id
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca sessões da tarefa (todos estudantes)
            cursor.execute("""
                SELECT 
                    student_hash,
                    start_timestamp,
                    end_timestamp,
                    duration_seconds,
                    event_count
                FROM sessions
                WHERE task_id = ?
                ORDER BY student_hash, start_timestamp ASC
            """, (task_id,))
            
            sessions = cursor.fetchall()
            
            if not sessions:
                return self._create_empty_figure()
            
            # Prepara dados
            data = []
            for session in sessions:
                student_hash, start_ts, end_ts, duration, events = session
                
                try:
                    start_dt = datetime.fromisoformat(start_ts.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(end_ts.replace('Z', '+00:00'))
                except:
                    start_dt = datetime.strptime(start_ts, '%Y-%m-%d %H:%M:%S')
                    end_dt = datetime.strptime(end_ts, '%Y-%m-%d %H:%M:%S')
                
                data.append({
                    'Student': student_hash[:8] + '...',
                    'Start': start_dt,
                    'Finish': end_dt,
                    'Duration': f"{duration}s",
                    'Events': events
                })
            
            df = pd.DataFrame(data)
            
            # Cria timeline
            fig = px.timeline(
                df,
                x_start='Start',
                x_end='Finish',
                y='Student',
                hover_data=['Duration', 'Events'],
                title=f"Task Timeline - {task_id}"
            )
            
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Student",
                height=max(400, len(df['Student'].unique()) * 30)
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_activity_over_time(
        self,
        student_hash: Optional[str] = None,
        task_id: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera gráfico de linha mostrando atividade ao longo do tempo.
        
        Args:
            student_hash: Filtrar por estudante (opcional)
            task_id: Filtrar por tarefa (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (line chart)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as event_count
                FROM events
                WHERE 1=1
            """
            params = []
            
            if student_hash:
                query += " AND student_hash = ?"
                params.append(student_hash)
            
            if task_id:
                query += " AND task_id = ?"
                params.append(task_id)
            
            query += " GROUP BY DATE(timestamp) ORDER BY date ASC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                return self._create_empty_figure()
            
            dates = [datetime.strptime(r[0], '%Y-%m-%d') for r in results]
            counts = [r[1] for r in results]
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=dates,
                y=counts,
                mode='lines+markers',
                name='Events',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=6)
            ))
            
            title = "Activity Over Time"
            if student_hash:
                title += f" - Student {student_hash[:8]}..."
            if task_id:
                title += f" - Task {task_id}"
            
            fig.update_layout(
                title_text=title,
                xaxis_title="Date",
                yaxis_title="Event Count",
                height=400,
                hovermode='x unified'
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_session_duration_distribution(
        self,
        task_id: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera histograma de distribuição de durações de sessões.
        
        Args:
            task_id: Filtrar por tarefa (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (histogram)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = "SELECT duration_seconds / 60.0 as duration_minutes FROM sessions WHERE 1=1"
            params = []
            
            if task_id:
                query += " AND task_id = ?"
                params.append(task_id)
            
            cursor.execute(query, params)
            durations = [r[0] for r in cursor.fetchall()]
            
            if not durations:
                return self._create_empty_figure()
            
            fig = go.Figure()
            
            fig.add_trace(go.Histogram(
                x=durations,
                nbinsx=30,
                marker_color='#1f77b4',
                opacity=0.7
            ))
            
            title = "Session Duration Distribution"
            if task_id:
                title += f" - Task {task_id}"
            
            fig.update_layout(
                title_text=title,
                xaxis_title="Duration (minutes)",
                yaxis_title="Frequency",
                height=400
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def _create_empty_figure(self) -> go.Figure:
        """Cria figura vazia."""
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=20, color="gray")
        )
        fig.update_layout(height=400)
        return fig
