"""
V.1 - Process Map Visualizer

Cria visualizações interativas de mapas de processo usando Plotly.
Mostra fluxo de atividades com frequências e métricas de performance.
"""

import structlog
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, Counter
from datetime import datetime

logger = structlog.get_logger()


class ProcessMapVisualizer:
    """Visualizador de mapas de processo para TKO Analytics."""
    
    def __init__(self, db_path: str):
        """
        Inicializa o visualizador.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        logger.info("[ProcessMapVisualizer.__init__] - visualizer_initialized", db_path=db_path)
    
    def _map_activity_name(self, event_type: str) -> str:
        """Mapeia tipo de evento para nome de atividade legível."""
        mapping = {
            'ExecEvent': 'Execute',
            'exec': 'Execute',
            'MoveEvent': 'Edit',
            'move': 'Edit',
            'SelfEvent': 'Self-Assess',
            'self': 'Self-Assess',
            'DownEvent': 'Download',
            'down': 'Download',
        }
        return mapping.get(event_type, event_type)
    
    def generate_process_map(
        self,
        task_id: Optional[str] = None,
        student_hash: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera mapa de processo interativo.
        
        Args:
            task_id: Filtrar por tarefa específica (opcional)
            student_hash: Filtrar por estudante específico (opcional)
            output_html: Salvar HTML interativo (opcional)
            
        Returns:
            Plotly Figure
        """
        logger.info(
            "[ProcessMapVisualizer.generate_process_map] - generating",
            task_id=task_id,
            student_hash=student_hash
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Query para buscar eventos
            query = "SELECT event_type, timestamp, case_id FROM events WHERE 1=1"
            params = []
            
            if task_id:
                query += " AND task_id = ?"
                params.append(task_id)
            
            if student_hash:
                query += " AND student_hash = ?"
                params.append(student_hash)
            
            query += " ORDER BY case_id, timestamp ASC"
            
            cursor.execute(query, params)
            events = cursor.fetchall()
            
            if not events:
                logger.warning("[ProcessMapVisualizer.generate_process_map] - no_events_found")
                return self._create_empty_figure()
            
            # Mapeia eventos para atividades
            activities = [self._map_activity_name(e[0]) for e in events]
            
            # Conta transições (activity1 → activity2)
            transitions = defaultdict(int)
            activity_counts = Counter()
            
            current_case = None
            prev_activity = None
            
            for i, event in enumerate(events):
                case_id = event[2]
                activity = activities[i]
                
                activity_counts[activity] += 1
                
                # Nova trace (case)
                if case_id != current_case:
                    current_case = case_id
                    prev_activity = None
                
                # Registra transição
                if prev_activity:
                    transitions[(prev_activity, activity)] += 1
                
                prev_activity = activity
            
            # Cria visualização Sankey (fluxo)
            fig = self._create_sankey_diagram(
                activity_counts,
                transitions,
                task_id,
                student_hash
            )
            
            # Salva HTML se solicitado
            if output_html:
                fig.write_html(output_html)
                logger.info(
                    "[ProcessMapVisualizer.generate_process_map] - html_saved",
                    path=output_html
                )
            
            logger.info(
                "[ProcessMapVisualizer.generate_process_map] - map_generated",
                activities=len(activity_counts),
                transitions=len(transitions),
                events=len(events)
            )
            
            return fig
            
        finally:
            conn.close()
    
    def _create_sankey_diagram(
        self,
        activity_counts: Counter,
        transitions: Dict[Tuple[str, str], int],
        task_id: Optional[str],
        student_hash: Optional[str]
    ) -> go.Figure:
        """Cria diagrama Sankey para visualizar fluxo de processo."""
        
        # Ordena atividades por frequência
        activities = sorted(activity_counts.keys(), key=lambda x: activity_counts[x], reverse=True)
        activity_to_idx = {act: i for i, act in enumerate(activities)}
        
        # Prepara dados para Sankey
        sources = []
        targets = []
        values = []
        labels = []
        colors = []
        
        # Define cores por tipo de atividade
        color_map = {
            'Download': 'rgba(31, 119, 180, 0.8)',
            'Edit': 'rgba(255, 127, 14, 0.8)',
            'Execute': 'rgba(44, 160, 44, 0.8)',
            'Self-Assess': 'rgba(214, 39, 40, 0.8)',
        }
        
        # Adiciona nodes (atividades)
        for activity in activities:
            label = f"{activity}\n({activity_counts[activity]})"
            labels.append(label)
            colors.append(color_map.get(activity, 'rgba(128, 128, 128, 0.8)'))
        
        # Adiciona links (transições)
        for (source_act, target_act), count in transitions.items():
            if count > 0:  # Apenas transições com eventos
                sources.append(activity_to_idx[source_act])
                targets.append(activity_to_idx[target_act])
                values.append(count)
        
        # Cria figura Sankey
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=labels,
                color=colors
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values,
                color='rgba(0, 0, 0, 0.2)'
            )
        )])
        
        # Título
        title_parts = ["Process Flow Map"]
        if task_id:
            title_parts.append(f"Task: {task_id}")
        if student_hash:
            title_parts.append(f"Student: {student_hash[:8]}...")
        
        fig.update_layout(
            title_text=" - ".join(title_parts),
            font_size=12,
            height=600
        )
        
        return fig
    
    def generate_activity_frequency_chart(
        self,
        task_id: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera gráfico de barras com frequência de atividades.
        
        Args:
            task_id: Filtrar por tarefa (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = "SELECT event_type, COUNT(*) as count FROM events"
            params = []
            
            if task_id:
                query += " WHERE task_id = ?"
                params.append(task_id)
            
            query += " GROUP BY event_type ORDER BY count DESC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                return self._create_empty_figure()
            
            # Mapeia nomes e prepara dados
            activities = [self._map_activity_name(r[0]) for r in results]
            counts = [r[1] for r in results]
            
            # Cores por atividade
            color_map = {
                'Download': '#1f77b4',
                'Edit': '#ff7f0e',
                'Execute': '#2ca02c',
                'Self-Assess': '#d62728',
            }
            colors = [color_map.get(act, '#7f7f7f') for act in activities]
            
            # Cria gráfico de barras
            fig = go.Figure(data=[
                go.Bar(
                    x=activities,
                    y=counts,
                    marker_color=colors,
                    text=counts,
                    textposition='auto',
                )
            ])
            
            title = "Activity Frequency"
            if task_id:
                title += f" - Task: {task_id}"
            
            fig.update_layout(
                title_text=title,
                xaxis_title="Activity",
                yaxis_title="Count",
                height=400
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def generate_transition_matrix(
        self,
        task_id: Optional[str] = None,
        output_html: Optional[str] = None
    ) -> go.Figure:
        """
        Gera matriz de transições (heatmap) entre atividades.
        
        Args:
            task_id: Filtrar por tarefa (opcional)
            output_html: Salvar HTML (opcional)
            
        Returns:
            Plotly Figure (heatmap)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = "SELECT event_type, timestamp, case_id FROM events"
            params = []
            
            if task_id:
                query += " WHERE task_id = ?"
                params.append(task_id)
            
            query += " ORDER BY case_id, timestamp ASC"
            
            cursor.execute(query, params)
            events = cursor.fetchall()
            
            if len(events) < 2:
                return self._create_empty_figure()
            
            # Mapeia atividades
            activities = [self._map_activity_name(e[0]) for e in events]
            unique_activities = sorted(set(activities))
            
            # Conta transições
            transitions = defaultdict(int)
            current_case = None
            prev_activity = None
            
            for i, event in enumerate(events):
                case_id = event[2]
                activity = activities[i]
                
                if case_id != current_case:
                    current_case = case_id
                    prev_activity = None
                
                if prev_activity:
                    transitions[(prev_activity, activity)] += 1
                
                prev_activity = activity
            
            # Cria matriz
            matrix = []
            for source in unique_activities:
                row = []
                for target in unique_activities:
                    row.append(transitions.get((source, target), 0))
                matrix.append(row)
            
            # Cria heatmap
            fig = go.Figure(data=go.Heatmap(
                z=matrix,
                x=unique_activities,
                y=unique_activities,
                colorscale='Blues',
                text=matrix,
                texttemplate='%{text}',
                textfont={"size": 10},
                hoverongaps=False
            ))
            
            title = "Activity Transition Matrix"
            if task_id:
                title += f" - Task: {task_id}"
            
            fig.update_layout(
                title_text=title,
                xaxis_title="To Activity",
                yaxis_title="From Activity",
                height=500,
                width=550
            )
            
            if output_html:
                fig.write_html(output_html)
            
            return fig
            
        finally:
            conn.close()
    
    def _create_empty_figure(self) -> go.Figure:
        """Cria figura vazia para casos sem dados."""
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
