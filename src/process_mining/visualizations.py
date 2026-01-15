"""
Visualizações de Process Mining: V1 (Processo Global) e V2 (Trajetória Individual).

Este módulo implementa as visualizações obrigatórias do sistema usando PM4Py nativo:
- V1: Rede de Petri do processo global descoberto
- V2: Trajetória individual de um estudante no processo

Usa PM4Py.visualization para gerar visualizações em SVG com Graphviz.
"""

import structlog
from typing import Dict, Tuple, Optional, List
from collections import Counter

import pm4py
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.log.obj import Trace
from pm4py.visualization.petri_net import visualizer as pn_visualizer

logger = structlog.get_logger()


class ProcessVisualizer:
    """
    Visualizador de processos usando PM4Py nativo com Graphviz.
    
    Implementa V1 e V2 conforme especificação:
    - Usa pm4py.visualization.petri_net para gerar SVGs
    - Notação formal automática (círculos, retângulos, setas)
    - Destaque de trajetórias individuais via decorações
    """
    
    def __init__(self):
        """Inicializa o visualizador."""
        logger.info("[ProcessVisualizer.__init__] - Initialized with PM4Py native visualization")
    
    def _get_color_by_frequency(self, count: int, is_highlighted: bool) -> str:
        """
        Determina cor da transição baseada na frequência de uso.
        
        Args:
            count: Número de execuções
            is_highlighted: Se faz parte da trajetória destacada
            
        Returns:
            Código de cor hexadecimal
        """
        if count == 0:
            return "#FFFFFF"  # Branco: não usado
        elif count <= 5:
            return "#ADD8E6" if is_highlighted else "#F0F0F0"  # Azul claro / Cinza claro: normal
        elif count <= 20:
            return "#4169E1" if is_highlighted else "#D3D3D3"  # Azul royal / Cinza: médio
        elif count <= 50:
            return "#FF8C00"  # Laranja escuro: alto
        else:
            return "#FF0000"  # Vermelho: excessivo (>50x)
    
    def _get_place_color(self, count: int, is_initial: bool = False, is_final: bool = False) -> str:
        """
        Determina cor do lugar (heatmap) baseada em tokens acumulados.
        
        Args:
            count: Número de tokens acumulados (passagens)
            is_initial: Se é lugar inicial
            is_final: Se é lugar final
            
        Returns:
            Código de cor hexadecimal
        """
        # Lugares especiais têm cores fixas
        if is_initial:
            return "#90EE90"  # Verde claro (início)
        if is_final:
            return "#90EE90"  # Verde claro (fim)
        
        # Heatmap baseado em frequência de passagens
        if count == 0:
            return "#FFFFFF"  # Branco: nunca visitado
        elif count <= 5:
            return "#E8F5E9"  # Verde muito claro: pouco
        elif count <= 20:
            return "#FFFACD"  # Amarelo limão: médio
        elif count <= 50:
            return "#FFD700"  # Ouro: alto
        else:
            return "#FF6347"  # Vermelho tomate: excessivo ⚠️
    
    def _calculate_place_tokens(
        self,
        net: PetriNet,
        event_counts: Optional[Dict[str, int]] = None
    ) -> Dict[str, int]:
        """
        Calcula quantas vezes cada lugar foi visitado (tokens acumulados).
        
        Args:
            net: Rede de Petri
            event_counts: Contagem de execuções por atividade
            
        Returns:
            Dicionário {place.name: count}
        """
        if not event_counts:
            return {}
        
        place_tokens = {}
        
        for place in net.places:
            # Encontrar transições que PRODUZEM tokens neste lugar (arestas de saída)
            incoming_transitions = [
                arc.source for arc in net.arcs 
                if arc.target == place and isinstance(arc.source, PetriNet.Transition)
            ]
            
            # Somar execuções das transições de entrada
            tokens = 0
            for transition in incoming_transitions:
                if transition.label:
                    tokens += event_counts.get(transition.label, 0)
            
            place_tokens[place.name if hasattr(place, 'name') else str(place)] = tokens
        
        return place_tokens
    
    def visualize_petri_net(
        self,
        net: PetriNet,
        initial_marking: Marking,
        final_marking: Marking,
        title: str = "V1: Rede de Petri - Processo Global",
        highlighted_trace: Optional[List[str]] = None,
        event_counts: Optional[Dict[str, int]] = None,
        event_mode_counts: Optional[Dict[str, Dict[str, int]]] = None,
        show_place_tokens: bool = True
    ) -> str:
        """
        Visualiza Rede de Petri usando PM4Py nativo (Graphviz) com contagens e tokens.
        
        Args:
            net: Rede de Petri (PM4Py)
            initial_marking: Marcação inicial
            final_marking: Marcação final
            title: Título do gráfico
            highlighted_trace: Lista de labels de transições a destacar (para V2)
            event_counts: Contagem total por atividade {'task_navigation': 9, ...}
            event_mode_counts: Contagem por modo {'test_execution': {'FULL': 59, 'FREE': 1}}
            show_place_tokens: Se True, mostra tokens acumulados nos lugares
            
        Returns:
            String SVG da visualização
        """
        logger.info("[ProcessVisualizer.visualize_petri_net] - Generating Petri Net with PM4Py",
                   places=len(net.places),
                   transitions=len(net.transitions),
                   arcs=len(net.arcs),
                   with_counts=event_counts is not None,
                   show_tokens=show_place_tokens)
        
        # Parâmetros de visualização
        parameters = {
            "format": "svg",
            "rankdir": "LR",  # Left to Right
            "bgcolor": "white",
        }
        
        # Decorar transições com contagens e cores
        decorations = {}
        for transition in net.transitions:
            if not transition.label:
                continue
                
            activity = transition.label
            is_highlighted = highlighted_trace and activity in highlighted_trace
            count = event_counts.get(activity, 0) if event_counts else 0
            
            # Label com contagem total
            if count > 0:
                label = f"{activity}\n({count}x)"
            else:
                label = activity
            
            # Cor baseada em frequência
            color = self._get_color_by_frequency(count, is_highlighted)
            
            # Tooltip com detalhamento por modo
            tooltip = f"{activity}\\nTotal: {count} execução" + ("ões" if count != 1 else "")
            
            if event_mode_counts and activity in event_mode_counts:
                mode_details = event_mode_counts[activity]
                tooltip += "\\n" + "\\n".join([
                    f"• {mode}: {cnt}x ({cnt/count*100:.1f}%)" 
                    for mode, cnt in sorted(mode_details.items(), key=lambda x: -x[1])
                ])
                
                # Adicionar alerta se houver desbalanceamento
                if 'FULL' in mode_details and 'FREE' in mode_details:
                    free_ratio = mode_details.get('FREE', 0) / count
                    if free_ratio < 0.1:
                        tooltip += "\\n⚠️ Poucos testes locais (FREE)"
            
            # Adicionar alerta se loop excessivo
            if count > 50:
                tooltip += "\\n⚠️ Loop excessivo!"
            
            decorations[transition] = {
                "label": label,
                "color": color,
                "tooltip": tooltip
            }
        
        if decorations:
            parameters["decorations"] = decorations
            logger.info("[ProcessVisualizer.visualize_petri_net] - Applied decorations",
                       decorated_transitions=len(decorations))
        
        # Decorar lugares com tokens (heatmap)
        if show_place_tokens and event_counts:
            place_tokens = self._calculate_place_tokens(net, event_counts)
            
            # Criar decorações para lugares
            # Nota: PM4Py não suporta nativamente decorações de lugares via parâmetros,
            # então vamos manipular o Graphviz diretamente
            for place in net.places:
                place_name = place.name if hasattr(place, 'name') else str(place)
                tokens = place_tokens.get(place_name, 0)
                
                is_initial = place in initial_marking
                is_final = place in final_marking
                
                # Determinar cor do lugar
                color = self._get_place_color(tokens, is_initial, is_final)
                
                # Adicionar label com contagem de tokens
                if tokens > 0 and not is_initial and not is_final:
                    # Modificar nome do lugar para incluir contagem
                    if hasattr(place, 'properties'):
                        place.properties['label'] = f"{place_name}\n[{tokens}]"
                    else:
                        place.properties = {'label': f"{place_name}\n[{tokens}]"}
                    
                    # Adicionar propriedades de estilo (via propriedades do objeto)
                    place.properties['fillcolor'] = color
                    place.properties['style'] = 'filled'
            
            logger.info("[ProcessVisualizer.visualize_petri_net] - Applied place tokens",
                       places_with_tokens=len([t for t in place_tokens.values() if t > 0]))
        
        try:
            # Gerar visualização com PM4Py
            gviz = pn_visualizer.apply(net, initial_marking, final_marking, parameters=parameters)
            
            # Converter para SVG string
            svg_bytes = gviz.pipe(format='svg')
            svg_string = svg_bytes.decode('utf-8')
            
            logger.info("[ProcessVisualizer.visualize_petri_net] - SVG generated successfully",
                       svg_size=len(svg_string))
            
            return svg_string
            
        except Exception as e:
            logger.error("[ProcessVisualizer.visualize_petri_net] - Error generating visualization",
                        error=str(e))
            raise
    
    def _calculate_event_counts_from_dfg(
        self, 
        dfg: Dict[Tuple[str, str], int],
        start_activities: Dict[str, int]
    ) -> Dict[str, int]:
        """
        Calcula contagem de eventos a partir do DFG.
        
        Args:
            dfg: DFG {(source, target): frequency}
            start_activities: Atividades iniciais com suas frequências
            
        Returns:
            Contagem por atividade {'MOVE DOWN': 6, 'EXEC FULL': 59, ...}
        """
        # Inicializar com start_activities (garante que primeiro evento seja contado)
        counts = dict(start_activities)
        
        # Adicionar contagens do DFG (apenas TARGET para evitar duplicação em loops)
        for (source, target), freq in dfg.items():
            if target not in ['END', 'end', 'sink']:
                counts[target] = counts.get(target, 0) + freq
        
        return counts
    
    def visualize_global_dfg(
        self,
        dfg: Dict[Tuple[str, str], int],
        start_activities: Dict[str, int],
        end_activities: Dict[str, int],
        title: str = "V1: Processo Global (Rede de Petri)"
    ) -> str:
        """
        V1: Visualiza processo global usando Rede de Petri derivada do DFG.
        
        Converte o DFG em Rede de Petri e visualiza usando notação formal com contagens.
        
        Args:
            dfg: Dicionário {(from_activity, to_activity): frequency}
            start_activities: Atividades iniciais com frequências
            end_activities: Atividades finais com frequências
            title: Título do gráfico
            
        Returns:
            String SVG da visualização
        """
        logger.info("[ProcessVisualizer.visualize_global_dfg] - Generating V1 as Petri Net",
                   transitions=len(dfg),
                   activities=len(set([a for pair in dfg.keys() for a in pair])))
        
        # Converter DFG para Rede de Petri
        net, initial_marking, final_marking = pm4py.convert_to_petri_net(dfg, start_activities, end_activities)
        
        # Calcular contagens de eventos do DFG (inclui start_activities)
        event_counts = self._calculate_event_counts_from_dfg(dfg, start_activities)
        
        # Visualizar usando PM4Py nativo com contagens
        return self.visualize_petri_net(
            net, 
            initial_marking, 
            final_marking, 
            title=title,
            event_counts=event_counts
        )
    
    def _extract_mode_counts_from_trace(self, trace: Trace) -> Dict[str, Dict[str, int]]:
        """
        Extrai contagem de eventos por modo do trace.
        
        Args:
            trace: Trace PM4Py com eventos
            
        Returns:
            Contagem por modo {'test_execution': {'FULL': 59, 'FREE': 1}, ...}
        """
        mode_counts = {}
        for event in trace:
            activity = event.get("concept:name")
            mode = event.get("mode")
            
            if activity and mode:
                if activity not in mode_counts:
                    mode_counts[activity] = {}
                mode_counts[activity][mode] = mode_counts[activity].get(mode, 0) + 1
        
        return mode_counts
    
    def visualize_student_trace(
        self,
        dfg: Dict[Tuple[str, str], int],
        start_activities: Dict[str, int],
        end_activities: Dict[str, int],
        student_trace: Trace,
        student_hash: str,
        task_id: str,
        title: Optional[str] = None
    ) -> str:
        """
        V2: Visualiza trajetória de um estudante usando Rede de Petri com contagens.
        
        Destaca o caminho percorrido pelo estudante com cores e exibe contagens.
        
        Args:
            dfg: DFG global
            start_activities: Atividades iniciais globais
            end_activities: Atividades finais globais
            student_trace: Trace do estudante (PM4Py Trace)
            student_hash: Hash do estudante (para label)
            task_id: ID da tarefa
            title: Título customizado (opcional)
            
        Returns:
            String SVG da visualização
        """
        if title is None:
            title = f"V2: Trajetória Individual - Estudante {student_hash[:8]} - Tarefa {task_id}"
        
        logger.info("[ProcessVisualizer.visualize_student_trace] - Generating V2 as Petri Net",
                   student_hash=student_hash[:8],
                   task_id=task_id,
                   trace_length=len(student_trace))
        
        # Extrai sequência de atividades (labels de transições) do estudante
        highlighted_activities = [event["concept:name"] for event in student_trace]
        
        # Calcular contagens totais
        event_counts = dict(Counter(highlighted_activities))
        
        # Extrair contagens por modo
        event_mode_counts = self._extract_mode_counts_from_trace(student_trace)
        
        logger.info("[ProcessVisualizer.visualize_student_trace] - Extracted counts",
                   total_activities=len(event_counts),
                   activities_with_modes=len(event_mode_counts))
        
        # Converter DFG para Rede de Petri
        net, initial_marking, final_marking = pm4py.convert_to_petri_net(dfg, start_activities, end_activities)
        
        # Visualizar com trajetória destacada e contagens
        return self.visualize_petri_net(
            net, 
            initial_marking, 
            final_marking, 
            title=title,
            highlighted_trace=highlighted_activities,
            event_counts=event_counts,
            event_mode_counts=event_mode_counts
        )
