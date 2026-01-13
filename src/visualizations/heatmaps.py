"""
V.4 - Activity Heatmaps

Cria heatmaps para visualizar padrões de atividade ao longo de
diferentes dimensões (tempo, estudantes, tarefas).
"""

import structlog
import sqlite3
import plotly.graph_objects as go
import plotly.figure_factory as ff
from typing import Optional, List
from datetime import datetime
import pandas as pd
import numpy as np

logger = structlog.get_logger()


class ActivityHeatmapVisualizer:
    """Visualizador de heatmaps de atividade para TKO Analytics."""
    
    def __init__(self, db_path: str):
        """
        Inicializa o visualizador.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        logger.info("[ActivityHeatmapVisualizer.__init__] - visualizer_initialized")
    
    def generate_time_of_day_heatmap(
        self,
        student_hash: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera heatmap de atividade por hora do dia e dia da semana.
        
        Args:
            student_hash: Filtrar por estudante (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (heatmap)
        """
        logger.info(
            "[ActivityHeatmapVisualizer.generate_time_of_day_heatmap] - generating",
            student_hash=student_hash[:8] if student_hash else "all"
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = "SELECT timestamp FROM events WHERE 1=1"
            params = []
            
            if student_hash:
                query += " AND student_hash = ?"
                params.append(student_hash)
            
            cursor.execute(query, params)
            timestamps = [r[0] for r in cursor.fetchall()]
            
            if not timestamps:
                return self._create_empty_figure()
            
            # Parse timestamps e extrai hora/dia da semana
            hours_days = []
            for ts in timestamps:
                try:
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except:
                    try:
                        dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except:
                        continue
                
                hours_days.append({
                    'hour': dt.hour,
                    'weekday': dt.strftime('%A')
                })
            
            # Cria matriz 24x7
            weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            matrix = np.zeros((24, 7))
            
            for entry in hours_days:
                hour = entry['hour']
                try:
                    day_idx = weekdays.index(entry['weekday'])
                    matrix[hour][day_idx] += 1
                except ValueError:
                    pass
            
            # Cria heatmap
            fig = go.Figure(data=go.Heatmap(
                z=matrix,
                x=weekdays,
                y=list(range(24)),
                colorscale='YlOrRd',
                hovertemplate='%{y}:00 on %{x}<br>Events: %{z}<extra></extra>'
            ))
            
            title = "Activity Heatmap (Hour x Day of Week)"
            if student_hash:
                title += f" - Student {student_hash[:8]}..."
            
            fig.update_layout(
                title_text=title,
                xaxis_title="Day of Week",
                yaxis_title="Hour of Day",
                height=600
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_student_task_heatmap(
        self,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera heatmap de atividade: estudantes x tarefas.
        
        Args:
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (heatmap)
        """
        logger.info("[ActivityHeatmapVisualizer.generate_student_task_heatmap] - generating")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca contagem de eventos por estudante/tarefa
            cursor.execute("""
                SELECT 
                    student_hash,
                    task_id,
                    COUNT(*) as event_count
                FROM events
                GROUP BY student_hash, task_id
                ORDER BY student_hash, task_id
            """)
            
            results = cursor.fetchall()
            
            if not results:
                return self._create_empty_figure()
            
            # Cria DataFrame
            df = pd.DataFrame(results, columns=['student', 'task', 'count'])
            df['student'] = df['student'].apply(lambda x: x[:8] + '...')
            
            # Cria pivot table
            pivot = df.pivot(index='student', columns='task', values='count').fillna(0)
            
            # Cria heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale='Blues',
                hovertemplate='Student: %{y}<br>Task: %{x}<br>Events: %{z}<extra></extra>'
            ))
            
            fig.update_layout(
                title_text="Activity Heatmap (Students x Tasks)",
                xaxis_title="Task",
                yaxis_title="Student",
                height=max(500, len(pivot.index) * 30)
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_event_type_heatmap(
        self,
        task_id: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera heatmap de tipos de eventos por estudante.
        
        Args:
            task_id: Filtrar por tarefa (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (heatmap)
        """
        logger.info(
            "[ActivityHeatmapVisualizer.generate_event_type_heatmap] - generating",
            task_id=task_id or "all"
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    student_hash,
                    event_type,
                    COUNT(*) as count
                FROM events
                WHERE 1=1
            """
            params = []
            
            if task_id:
                query += " AND task_id = ?"
                params.append(task_id)
            
            query += " GROUP BY student_hash, event_type ORDER BY student_hash"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                return self._create_empty_figure()
            
            # Cria DataFrame
            df = pd.DataFrame(results, columns=['student', 'event_type', 'count'])
            df['student'] = df['student'].apply(lambda x: x[:8] + '...')
            
            # Mapeia event types para nomes legíveis
            event_map = {
                'ExecEvent': 'Execute',
                'MoveEvent': 'Edit',
                'SelfEvent': 'Self-Assess',
                'DownEvent': 'Download'
            }
            df['event_type'] = df['event_type'].map(lambda x: event_map.get(x, x))
            
            # Cria pivot table
            pivot = df.pivot(index='student', columns='event_type', values='count').fillna(0)
            
            # Cria heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale='Viridis',
                hovertemplate='Student: %{y}<br>Event: %{x}<br>Count: %{z}<extra></extra>'
            ))
            
            title = "Event Type Distribution"
            if task_id:
                title += f" - Task {task_id}"
            
            fig.update_layout(
                title_text=title,
                xaxis_title="Event Type",
                yaxis_title="Student",
                height=max(500, len(pivot.index) * 30)
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_pattern_frequency_heatmap(
        self,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera heatmap de frequência de padrões comportamentais.
        
        Args:
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (heatmap)
        """
        logger.info("[ActivityHeatmapVisualizer.generate_pattern_frequency_heatmap] - generating")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    student_hash,
                    pattern_type,
                    COUNT(*) as count
                FROM behavioral_patterns
                GROUP BY student_hash, pattern_type
                ORDER BY student_hash
            """)
            
            results = cursor.fetchall()
            
            if not results:
                return self._create_empty_figure()
            
            # Cria DataFrame
            df = pd.DataFrame(results, columns=['student', 'pattern', 'count'])
            df['student'] = df['student'].apply(lambda x: x[:8] + '...')
            
            # Cria pivot table
            pivot = df.pivot(index='student', columns='pattern', values='count').fillna(0)
            
            # Cria heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale='Reds',
                hovertemplate='Student: %{y}<br>Pattern: %{x}<br>Count: %{z}<extra></extra>'
            ))
            
            fig.update_layout(
                title_text="Behavioral Pattern Frequency",
                xaxis_title="Pattern Type",
                yaxis_title="Student",
                height=max(500, len(pivot.index) * 30)
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_session_intensity_heatmap(
        self,
        task_id: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera heatmap de intensidade de sessões (eventos/minuto).
        
        Args:
            task_id: Filtrar por tarefa (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (heatmap)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    student_hash,
                    task_id,
                    AVG(event_count * 60.0 / duration_seconds) as intensity
                FROM sessions
                WHERE duration_seconds > 0
            """
            params = []
            
            if task_id:
                query += " AND task_id = ?"
                params.append(task_id)
            
            query += " GROUP BY student_hash, task_id"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                return self._create_empty_figure()
            
            # Cria DataFrame
            df = pd.DataFrame(results, columns=['student', 'task', 'intensity'])
            df['student'] = df['student'].apply(lambda x: x[:8] + '...')
            
            # Cria pivot table
            pivot = df.pivot(index='student', columns='task', values='intensity').fillna(0)
            
            # Cria heatmap
            fig = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=pivot.columns,
                y=pivot.index,
                colorscale='Hot',
                hovertemplate='Student: %{y}<br>Task: %{x}<br>Intensity: %{z:.2f} events/min<extra></extra>'
            ))
            
            title = "Session Intensity (events/minute)"
            if task_id:
                title += f" - Task {task_id}"
            
            fig.update_layout(
                title_text=title,
                xaxis_title="Task",
                yaxis_title="Student",
                height=max(500, len(pivot.index) * 30)
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
