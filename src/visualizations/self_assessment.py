"""
V.3 - Self-Assessment Comparator

Compara as autoavaliações dos estudantes (SELF events) com o 
comportamento observado via telemetria. Identifica discrepâncias
entre percepção e realidade.
"""

import structlog
import sqlite3
import plotly.graph_objects as go
from typing import List, Dict, Optional, Tuple
import json

logger = structlog.get_logger()


class SelfAssessmentComparator:
    """
    Comparador de autoavaliações para TKO Analytics.
    
    Confronta os dados de SelfEvent (estimativas e autoavaliações)
    com métricas observadas via telemetria.
    """
    
    def __init__(self, db_path: str):
        """
        Inicializa o comparador.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        logger.info("[SelfAssessmentComparator.__init__] - comparator_initialized")
    
    def compare_time_estimates(
        self,
        student_hash: str,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Compara tempo estimado (SELF) vs tempo real observado.
        
        Args:
            student_hash: Hash do estudante
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (scatter plot)
        """
        logger.info(
            "[SelfAssessmentComparator.compare_time_estimates] - comparing",
            student_hash=student_hash[:8]
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca autoavaliações
            cursor.execute("""
                SELECT task_id, custom_data
                FROM events
                WHERE student_hash = ?
                  AND event_type = 'SelfEvent'
                ORDER BY timestamp ASC
            """, (student_hash,))
            
            self_events = cursor.fetchall()
            
            if not self_events:
                return self._create_empty_figure("No self-assessments found")
            
            # Para cada task, busca tempo real gasto
            data = []
            for task_id, custom_json in self_events:
                # Parse custom_data
                try:
                    custom = json.loads(custom_json) if custom_json else {}
                    estimated_minutes = custom.get('estimated_minutes', 0)
                except:
                    estimated_minutes = 0
                
                if estimated_minutes == 0:
                    continue
                
                # Busca tempo real
                cursor.execute("""
                    SELECT SUM(duration_seconds) / 60.0 as actual_minutes
                    FROM sessions
                    WHERE student_hash = ? AND task_id = ?
                """, (student_hash, task_id))
                
                result = cursor.fetchone()
                actual_minutes = result[0] if result and result[0] else 0
                
                if actual_minutes > 0:
                    error_percent = ((actual_minutes - estimated_minutes) / estimated_minutes) * 100
                    
                    data.append({
                        'task_id': task_id,
                        'estimated': estimated_minutes,
                        'actual': actual_minutes,
                        'error_percent': error_percent
                    })
            
            if not data:
                return self._create_empty_figure("No matching data")
            
            # Cria scatter plot
            fig = go.Figure()
            
            # Linha de referência (estimated = actual)
            max_time = max(
                max(d['estimated'] for d in data),
                max(d['actual'] for d in data)
            )
            
            fig.add_trace(go.Scatter(
                x=[0, max_time],
                y=[0, max_time],
                mode='lines',
                name='Perfect Estimate',
                line=dict(color='gray', dash='dash', width=2)
            ))
            
            # Pontos dos dados
            fig.add_trace(go.Scatter(
                x=[d['estimated'] for d in data],
                y=[d['actual'] for d in data],
                mode='markers',
                name='Tasks',
                marker=dict(
                    size=12,
                    color=[d['error_percent'] for d in data],
                    colorscale='RdYlGn_r',
                    showscale=True,
                    colorbar=dict(title="Error %"),
                    line=dict(width=1, color='black')
                ),
                text=[d['task_id'] for d in data],
                hovertemplate='<b>%{text}</b><br>Estimated: %{x:.1f} min<br>Actual: %{y:.1f} min<extra></extra>'
            ))
            
            fig.update_layout(
                title_text=f"Time Estimation Accuracy - Student {student_hash[:8]}...",
                xaxis_title="Estimated Time (minutes)",
                yaxis_title="Actual Time (minutes)",
                height=500,
                hovermode='closest'
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def analyze_autonomy_claims(
        self,
        student_hash: str,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Analisa claims de autonomia vs comportamento observado.
        
        Args:
            student_hash: Hash do estudante
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (bar chart)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca autoavaliações
            cursor.execute("""
                SELECT task_id, custom_data
                FROM events
                WHERE student_hash = ?
                  AND event_type = 'SelfEvent'
            """, (student_hash,))
            
            self_events = cursor.fetchall()
            
            data = []
            for task_id, custom_json in self_events:
                try:
                    custom = json.loads(custom_json) if custom_json else {}
                    autonomy_level = custom.get('autonomy_level', 'unknown')
                except:
                    autonomy_level = 'unknown'
                
                # Busca padrões comportamentais
                cursor.execute("""
                    SELECT pattern_type
                    FROM behavioral_patterns
                    WHERE student_hash = ? AND task_id = ?
                """, (student_hash, task_id))
                
                patterns = [r[0] for r in cursor.fetchall()]
                
                # Heurística: trial-and-error e code-thrashing indicam menor autonomia
                observed_autonomy = 'high'
                if 'trial_and_error' in patterns or 'code_thrashing' in patterns:
                    observed_autonomy = 'low'
                elif 'procrastination' in patterns:
                    observed_autonomy = 'medium'
                
                data.append({
                    'task_id': task_id,
                    'claimed': autonomy_level,
                    'observed': observed_autonomy
                })
            
            if not data:
                return self._create_empty_figure("No data available")
            
            # Conta discrepâncias
            matches = sum(1 for d in data if d['claimed'] == d['observed'])
            total = len(data)
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=['Matches', 'Mismatches'],
                y=[matches, total - matches],
                marker_color=['#2ca02c', '#d62728'],
                text=[matches, total - matches],
                textposition='auto'
            ))
            
            fig.update_layout(
                title_text=f"Autonomy Claim Accuracy - Student {student_hash[:8]}...<br><sub>{matches}/{total} tasks match</sub>",
                yaxis_title="Count",
                height=400
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def compare_help_received(
        self,
        task_id: str,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Compara níveis de ajuda reportados vs inferidos.
        
        Args:
            task_id: ID da tarefa
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca autoavaliações da tarefa
            cursor.execute("""
                SELECT student_hash, custom_data
                FROM events
                WHERE task_id = ?
                  AND event_type = 'SelfEvent'
            """, (task_id,))
            
            results = cursor.fetchall()
            
            data = []
            for student_hash, custom_json in results:
                try:
                    custom = json.loads(custom_json) if custom_json else {}
                    help_received = custom.get('help_received', 'none')
                except:
                    help_received = 'none'
                
                data.append({
                    'student': student_hash[:8] + '...',
                    'help_level': help_received
                })
            
            if not data:
                return self._create_empty_figure("No data available")
            
            # Conta níveis de ajuda
            from collections import Counter
            help_counts = Counter(d['help_level'] for d in data)
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=list(help_counts.keys()),
                y=list(help_counts.values()),
                marker_color='#1f77b4',
                text=list(help_counts.values()),
                textposition='auto'
            ))
            
            fig.update_layout(
                title_text=f"Help Received - Task {task_id}",
                xaxis_title="Help Level",
                yaxis_title="Student Count",
                height=400
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_self_assessment_report(
        self,
        student_hash: str
    ) -> Dict:
        """
        Gera relatório completo de autoavaliação para um estudante.
        
        Args:
            student_hash: Hash do estudante
            
        Returns:
            Dict com métricas e análises
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca todas autoavaliações
            cursor.execute("""
                SELECT task_id, custom_data
                FROM events
                WHERE student_hash = ?
                  AND event_type = 'SelfEvent'
            """, (student_hash,))
            
            self_events = cursor.fetchall()
            
            if not self_events:
                return {'error': 'No self-assessments found'}
            
            # Analisa estimativas de tempo
            time_errors = []
            for task_id, custom_json in self_events:
                try:
                    custom = json.loads(custom_json) if custom_json else {}
                    estimated = custom.get('estimated_minutes', 0)
                except:
                    continue
                
                cursor.execute("""
                    SELECT SUM(duration_seconds) / 60.0
                    FROM sessions
                    WHERE student_hash = ? AND task_id = ?
                """, (student_hash, task_id))
                
                result = cursor.fetchone()
                actual = result[0] if result and result[0] else 0
                
                if estimated > 0 and actual > 0:
                    error = abs(actual - estimated) / estimated
                    time_errors.append(error)
            
            # Calcula métricas
            avg_time_error = sum(time_errors) / len(time_errors) if time_errors else 0
            
            report = {
                'student_hash': student_hash[:8] + '...',
                'total_self_assessments': len(self_events),
                'time_estimation_accuracy': {
                    'tasks_with_estimates': len(time_errors),
                    'avg_error_percent': round(avg_time_error * 100, 2),
                    'accuracy_rating': 'Good' if avg_time_error < 0.3 else 'Poor'
                }
            }
            
            return report
            
        finally:
            conn.close()
    
    def _create_empty_figure(self, message: str = "No data available") -> go.Figure:
        """Cria figura vazia com mensagem."""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=20, color="gray")
        )
        fig.update_layout(height=400)
        return fig
