"""
ConformanceReplayer: Token-based replay para análise de conformidade.

Este módulo implementa replay individual de comportamento de estudantes
em um modelo de processo fixo, calculando métricas de conformidade.
"""

import json
import sqlite3
import structlog
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

import pm4py
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.log.obj import EventLog, Trace, Event

logger = structlog.get_logger()


class ReplayError(Exception):
    """Erro durante replay de conformidade."""
    pass


@dataclass
class ConformanceMetrics:
    """
    Métricas de conformidade de um estudante/tarefa.
    
    Attributes:
        case_id: Identificador do caso
        student_hash: Hash do estudante
        task_id: ID da tarefa
        fitness: Grau de aderência (0.0 a 1.0)
        missing_tokens: Tokens que faltaram durante o replay
        remaining_tokens: Tokens que sobraram ao final
        consumed_tokens: Tokens consumidos com sucesso
        produced_tokens: Tokens produzidos durante o replay
        deviations_count: Número de desvios detectados
        excessive_loops_count: Loops excessivos (>limiar)
        trace_length: Tamanho do trace
        deviations_detail: Lista de desvios específicos
        trace_is_fit: Se o trace é perfeitamente ajustado (fitness=1.0)
        activated_transitions: Lista de transições ativadas durante replay
        reached_marking: Marcação alcançada ao final do replay
        enabled_transitions_in_marking: Transições habilitadas na marcação final
        transitions_with_problems: Transições que causaram problemas
    """
    case_id: str
    student_hash: str
    task_id: str
    fitness: float
    missing_tokens: int
    remaining_tokens: int
    consumed_tokens: int
    produced_tokens: int
    deviations_count: int
    excessive_loops_count: int
    trace_length: int
    deviations_detail: List[Dict[str, Any]]
    trace_is_fit: bool = False
    activated_transitions: List[str] = None
    reached_marking: List[str] = None
    enabled_transitions_in_marking: List[str] = None
    transitions_with_problems: List[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            "case_id": self.case_id,
            "student_hash": self.student_hash,
            "task_id": self.task_id,
            "fitness": self.fitness,
            "missing_tokens": self.missing_tokens,
            "remaining_tokens": self.remaining_tokens,
            "consumed_tokens": self.consumed_tokens,
            "produced_tokens": self.produced_tokens,
            "deviations_count": self.deviations_count,
            "excessive_loops_count": self.excessive_loops_count,
            "trace_length": self.trace_length,
            "deviations_detail": self.deviations_detail,
            "trace_is_fit": self.trace_is_fit,
            "activated_transitions": self.activated_transitions or [],
            "reached_marking": self.reached_marking or [],
            "enabled_transitions_in_marking": self.enabled_transitions_in_marking or [],
            "transitions_with_problems": self.transitions_with_problems or []
        }


class ConformanceReplayer:
    """
    Replayer de conformidade usando token-based replay.
    
    Responsável por:
    1. Buscar eventos ANALYSIS de um estudante/tarefa
    2. Executar replay no modelo fixo (Petri Net)
    3. Calcular métricas de conformidade (fitness, desvios, loops)
    4. Persistir métricas no banco
    """
    
    def __init__(
        self,
        db_path: str,
        net: PetriNet,
        initial_marking: Marking,
        final_marking: Marking,
        loop_threshold: int = 5
    ):
        """
        Inicializa o replayer.
        
        Args:
            db_path: Caminho do banco de dados SQLite
            net: Petri Net do modelo global
            initial_marking: Marcação inicial
            final_marking: Marcação final
            loop_threshold: Limite para detectar loops excessivos (padrão: 5)
            
        Raises:
            ReplayError: Se banco não existe ou modelo inválido
        """
        self.db_path = Path(db_path)
        self.net = net
        self.initial_marking = initial_marking
        self.final_marking = final_marking
        self.loop_threshold = loop_threshold
        
        if not self.db_path.exists():
            raise ReplayError(f"Database not found: {self.db_path}")
        
        if not net or not initial_marking or not final_marking:
            raise ReplayError("Invalid Petri Net model provided")
        
        logger.info("[ConformanceReplayer.__init__] - Initialized",
                   db_path=str(self.db_path),
                   loop_threshold=loop_threshold)
    
    def replay_student_task(
        self,
        student_hash: str,
        task_id: str
    ) -> ConformanceMetrics:
        """
        Executa replay de um estudante em uma tarefa específica.
        
        Args:
            student_hash: Hash do estudante
            task_id: ID da tarefa
            
        Returns:
            ConformanceMetrics com resultados do replay
            
        Raises:
            ReplayError: Se não há eventos ANALYSIS para o caso
        """
        logger.info("[ConformanceReplayer.replay_student_task] - Starting replay",
                   student_hash=student_hash[:8],
                   task_id=task_id)
        
        # 1. Busca eventos ANALYSIS do caso
        events = self._fetch_analysis_events(student_hash, task_id)
        
        if not events:
            raise ReplayError(f"No ANALYSIS events found for student {student_hash[:8]}, task {task_id}")
        
        case_id = events[0]["case_id"]
        
        # 2. Converte para EventLog
        trace = self._events_to_trace(events, case_id)
        
        # 3. Executa token-based replay
        fitness, deviations, extended_metrics = self._token_based_replay(trace)
        
        # 4. Detecta loops excessivos
        loops = self._detect_excessive_loops(events)
        
        # 5. Monta métricas
        metrics = ConformanceMetrics(
            case_id=case_id,
            student_hash=student_hash,
            task_id=task_id,
            fitness=fitness,
            missing_tokens=extended_metrics['missing'],
            remaining_tokens=extended_metrics['remaining'],
            consumed_tokens=extended_metrics['consumed'],
            produced_tokens=extended_metrics['produced'],
            deviations_count=len(deviations),
            excessive_loops_count=len(loops),
            trace_length=len(events),
            deviations_detail=deviations + loops,
            trace_is_fit=extended_metrics.get('trace_is_fit', False),
            activated_transitions=extended_metrics.get('activated_transitions', []),
            reached_marking=extended_metrics.get('reached_marking', []),
            enabled_transitions_in_marking=extended_metrics.get('enabled_transitions_in_marking', []),
            transitions_with_problems=extended_metrics.get('transitions_with_problems', [])
        )
        
        logger.info("[ConformanceReplayer.replay_student_task] - Replay completed",
                   case_id=case_id,
                   fitness=fitness,
                   missing_tokens=extended_metrics['missing'],
                   remaining_tokens=extended_metrics['remaining'],
                   consumed_tokens=extended_metrics['consumed'],
                   produced_tokens=extended_metrics['produced'],
                   deviations=len(deviations),
                   loops=len(loops))
        
        return metrics
    
    def replay_all_students(self, task_id: Optional[str] = None, model_scope: Optional[str] = None) -> List[ConformanceMetrics]:
        """
        Executa replay para todos os estudantes (opcionalmente filtrado por tarefa).
        
        Args:
            task_id: Se fornecido, processa apenas essa tarefa
            model_scope: Tarefa usada no modelo. Se fornecido, garante compatibilidade.
        
        Returns:
            Lista de ConformanceMetrics
            
        Raises:
            ReplayError: Se há incompatibilidade entre model_scope e task_id
        """
        # Validação de compatibilidade
        if model_scope and task_id and model_scope != task_id:
            raise ReplayError(
                f"Incompatibilidade: modelo gerado para tarefa '{model_scope}', "
                f"mas tentando analisar tarefa '{task_id}'"
            )
        
        # Se modelo é específico, força análise apenas daquela tarefa
        analysis_task_id = model_scope or task_id
        
        logger.info("[ConformanceReplayer.replay_all_students] - Starting batch replay",
                   task_id=analysis_task_id,
                   model_scope=model_scope or "global")
        
        # Busca todos os casos ANALYSIS (com filtro se analysis_task_id fornecido)
        cases = self._fetch_analysis_cases(analysis_task_id)
        
        logger.info("[ConformanceReplayer.replay_all_students] - Cases found",
                   total=len(cases))
        
        results = []
        
        for case in cases:
            try:
                metrics = self.replay_student_task(
                    student_hash=case["student_hash"],
                    task_id=case["task_id"]
                )
                results.append(metrics)
            except ReplayError as e:
                logger.warning("[ConformanceReplayer.replay_all_students] - Replay failed",
                              case_id=case["case_id"],
                              error=str(e))
                continue
        
        logger.info("[ConformanceReplayer.replay_all_students] - Batch replay completed",
                   successful=len(results),
                   total=len(cases))
        
        return results
    
    def save_conformance_metrics(self, metrics: ConformanceMetrics) -> None:
        """
        Persiste métricas de conformidade no banco (tabela metrics).
        
        Args:
            metrics: Métricas calculadas
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Fitness
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_fitness_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_fitness",
                metrics.fitness,
                None
            ))
            
            # Desvios
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_deviations_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_deviations",
                metrics.deviations_count,
                json.dumps({"detail": metrics.deviations_detail})
            ))
            
            # Loops excessivos
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_loops_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_excessive_loops",
                metrics.excessive_loops_count,
                None
            ))
            
            # Missing Tokens
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_missing_tokens_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_missing_tokens",
                metrics.missing_tokens,
                None
            ))
            
            # Remaining Tokens
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_remaining_tokens_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_remaining_tokens",
                metrics.remaining_tokens,
                None
            ))
            
            # Consumed Tokens
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_consumed_tokens_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_consumed_tokens",
                metrics.consumed_tokens,
                None
            ))
            
            # Produced Tokens
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_produced_tokens_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_produced_tokens",
                metrics.produced_tokens,
                None
            ))
            
            # Trace Length
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_trace_length_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_trace_length",
                metrics.trace_length,
                None
            ))
            
            # Trace Is Fit (novo)
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_trace_is_fit_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_trace_is_fit",
                1 if metrics.trace_is_fit else 0,
                None
            ))
            
            # Activated Transitions (novo)
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_activated_transitions_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_activated_transitions",
                len(metrics.activated_transitions) if metrics.activated_transitions else 0,
                json.dumps(metrics.activated_transitions or [])
            ))
            
            # Reached Marking (novo)
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_reached_marking_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_reached_marking",
                len(metrics.reached_marking) if metrics.reached_marking else 0,
                json.dumps(metrics.reached_marking or [])
            ))
            
            # Enabled Transitions in Marking (novo)
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_enabled_transitions_in_marking_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_enabled_transitions_in_marking",
                len(metrics.enabled_transitions_in_marking) if metrics.enabled_transitions_in_marking else 0,
                json.dumps(metrics.enabled_transitions_in_marking or [])
            ))
            
            # Transitions with Problems (novo)
            cursor.execute("""
                INSERT OR REPLACE INTO metrics (
                    id, case_id, student_hash, task_id, metric_name, metric_value, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                f"conformance_transitions_with_problems_{metrics.case_id}",
                metrics.case_id,
                metrics.student_hash,
                metrics.task_id,
                "conformance_transitions_with_problems",
                len(metrics.transitions_with_problems) if metrics.transitions_with_problems else 0,
                json.dumps(metrics.transitions_with_problems or [])
            ))
            
            conn.commit()
            
            logger.info("[ConformanceReplayer.save_conformance_metrics] - Metrics saved",
                       case_id=metrics.case_id,
                       fitness=metrics.fitness,
                       missing_tokens=metrics.missing_tokens,
                       remaining_tokens=metrics.remaining_tokens,
                       consumed_tokens=metrics.consumed_tokens,
                       produced_tokens=metrics.produced_tokens)
        
        except sqlite3.Error as e:
            conn.rollback()
            logger.error("[ConformanceReplayer.save_conformance_metrics] - Save failed",
                        case_id=metrics.case_id,
                        error=str(e))
            raise ReplayError(f"Failed to save metrics: {e}") from e
        finally:
            conn.close()
    
    def _fetch_analysis_events(
        self,
        student_hash: str,
        task_id: str
    ) -> List[Dict[str, Any]]:
        """
        Busca eventos ANALYSIS de um estudante/tarefa.
        
        Returns:
            Lista de eventos ordenados por timestamp
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, case_id, student_hash, student_name, task_id, activity,
                event_type, timestamp, metadata
            FROM analysis_events
            WHERE student_hash = ?
              AND task_id = ?
            ORDER BY timestamp ASC
        """, (student_hash, task_id))
        
        rows = cursor.fetchall()
        conn.close()
        
        # Extrair modo do metadata JSON
        events = []
        for row in rows:
            event = dict(row)
            # Tentar extrair modo do metadata
            if event.get('metadata'):
                try:
                    import json
                    metadata = json.loads(event['metadata'])
                    event['mode'] = metadata.get('mode')
                except:
                    event['mode'] = None
            else:
                event['mode'] = None
            events.append(event)
        
        return events
    
    def _fetch_analysis_cases(self, task_id: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Busca todos os casos (student + task) do dataset ANALYSIS.
        
        Returns:
            Lista de dicionários {case_id, student_hash, task_id}
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if task_id:
            cursor.execute("""
                SELECT DISTINCT case_id, student_hash, task_id
                FROM analysis_events
                WHERE task_id = ?
            """, (task_id,))
        else:
            cursor.execute("""
                SELECT DISTINCT case_id, student_hash, task_id
                FROM analysis_events
            """)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def _events_to_trace(self, events: List[Dict[str, Any]], case_id: str) -> Trace:
        """
        Converte lista de eventos em PM4Py Trace.
        
        Returns:
            Trace para replay
        """
        trace = Trace()
        trace.attributes["concept:name"] = case_id
        
        if events:
            trace.attributes["student_hash"] = events[0]["student_hash"]
            trace.attributes["task_id"] = events[0]["task_id"]
        
        for evt in events:
            pm4py_event = Event()
            pm4py_event["concept:name"] = evt["activity"]
            pm4py_event["time:timestamp"] = datetime.fromisoformat(evt["timestamp"])
            pm4py_event["case:id"] = case_id
            pm4py_event["event_type"] = evt["event_type"]
            
            # Incluir modo do evento (se disponível)
            if "mode" in evt and evt["mode"]:
                pm4py_event["mode"] = evt["mode"]
            
            trace.append(pm4py_event)
        
        return trace
    
    def _token_based_replay(self, trace: Trace) -> Tuple[float, List[Dict[str, Any]], Dict[str, Any]]:
        """
        Executa token-based replay usando PM4Py.
        
        Returns:
            Tupla (fitness, deviations_list, extended_metrics)
            onde extended_metrics inclui: token_metrics + informações extras do replay
            
        IMPORTANTE: Os valores de consumed/produced podem incluir transições skip (τ) invisíveis
        adicionadas pelo Inductive Miner. O fitness retornado pelo PM4Py já leva isso em conta.
        """
        # Cria EventLog com único trace
        log = EventLog([trace])
        
        logger.info("[ConformanceReplayer._token_based_replay] - Starting replay",
                   trace_length=len(trace),
                   case_id=trace.attributes.get("concept:name", "unknown"))
        
        # Executa token-based replay
        replay_result = pm4py.conformance_diagnostics_token_based_replay(
            log,
            self.net,
            self.initial_marking,
            self.final_marking
        )

        
        if not replay_result:
            logger.warning("[ConformanceReplayer._token_based_replay] - Empty replay result")
            extended_metrics = {
                'missing': 0, 'remaining': 0, 'consumed': 0, 'produced': 0,
                'trace_is_fit': False, 'activated_transitions': [], 'reached_marking': [],
                'enabled_transitions_in_marking': [], 'transitions_with_problems': []
            }
            return 0.0, [], extended_metrics
        
        # Extrai resultado do primeiro (único) trace
        trace_result = replay_result[0]
        
        # Log do resultado RAW do PM4Py para debug
        logger.info("[ConformanceReplayer._token_based_replay] - PM4Py result",
                   trace_is_fit=trace_result.get('trace_is_fit'),
                   trace_fitness=trace_result.get('trace_fitness'),
                   missing_tokens=trace_result.get('missing_tokens'),
                   remaining_tokens=trace_result.get('remaining_tokens'),
                   consumed_tokens=trace_result.get('consumed_tokens'),
                   produced_tokens=trace_result.get('produced_tokens'))
        
        # Extrair métricas do PM4Py (sempre retorna dict)
        fitness = float(trace_result.get('trace_fitness', 0.0))
        missing = int(trace_result.get('missing_tokens', 0))
        remaining = int(trace_result.get('remaining_tokens', 0))
        consumed = int(trace_result.get('consumed_tokens', 0))
        produced = int(trace_result.get('produced_tokens', 0))
        
        # Extrair informações extras do replay
        trace_is_fit = bool(trace_result.get('trace_is_fit', False))
        
        # Função auxiliar para converter transições em strings legíveis
        def transition_to_string(trans):
            """Converte objeto Transition ou tupla em string."""
            if isinstance(trans, tuple) and len(trans) >= 2:
                # Tupla (Transition, label)
                return trans[1] if trans[1] else str(trans[0].name if hasattr(trans[0], 'name') else trans[0])
            elif hasattr(trans, 'label') and trans.label:
                # Objeto Transition com label
                return trans.label
            elif hasattr(trans, 'name'):
                # Objeto Transition com name
                return trans.name
            else:
                # Fallback
                return str(trans)
        
        # Activated transitions: converter para strings legíveis
        activated_trans = trace_result.get('activated_transitions', [])
        activated_transitions_str = [transition_to_string(trans) for trans in activated_trans] if activated_trans else []
        
        # Reached marking: converter para lista de strings
        reached_marking = trace_result.get('reached_marking', [])
        reached_marking_str = [str(mark) for mark in reached_marking] if reached_marking else []
        
        # Enabled transitions in marking: converter para lista de strings
        enabled_trans = trace_result.get('enabled_transitions_in_marking', set())
        enabled_transitions_str = [transition_to_string(trans) for trans in enabled_trans] if enabled_trans else []
        
        # Transitions with problems: converter para lista de strings
        problem_trans = trace_result.get('transitions_with_problems', [])
        transitions_with_problems_str = [transition_to_string(trans) for trans in problem_trans] if problem_trans else []
        
        # IMPORTANTE: consumed e produced incluem transições skip (τ) invisíveis!
        # Não usar essas métricas diretamente para interpretação.
        # O fitness já está correto (calculado pelo PM4Py considerando os skips)
        
        extended_metrics = {
            'missing': missing,
            'remaining': remaining,
            'consumed': consumed,
            'produced': produced,
            'trace_is_fit': trace_is_fit,
            'activated_transitions': activated_transitions_str,
            'reached_marking': reached_marking_str,
            'enabled_transitions_in_marking': enabled_transitions_str,
            'transitions_with_problems': transitions_with_problems_str
        }
        
        # Desvios: missing + remaining tokens
        deviations = []
        
        if missing > 0:
            deviations.append({
                "type": "missing_tokens",
                "count": missing,
                "description": f"{missing} transições esperadas não foram ativadas (pode incluir atividades obrigatórias não executadas)"
            })
        
        if remaining > 0:
            deviations.append({
                "type": "remaining_tokens",
                "count": remaining,
                "description": f"{remaining} tokens restantes (processo não finalizou no estado esperado)"
            })
        
        logger.info("[ConformanceReplayer._token_based_replay] - Replay completed",
                   fitness=fitness,
                   is_fit=trace_is_fit)
        
        return round(fitness, 4), deviations, extended_metrics
    
    def _detect_excessive_loops(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detecta loops excessivos (repetições consecutivas acima do limiar).
        
        Returns:
            Lista de loops detectados
        """
        loops = []
        
        if len(events) < 2:
            return loops
        
        current_activity = events[0]["activity"]
        consecutive_count = 1
        
        for i in range(1, len(events)):
            activity = events[i]["activity"]
            
            if activity == current_activity:
                consecutive_count += 1
            else:
                # Verifica se excedeu limiar
                if consecutive_count > self.loop_threshold:
                    loops.append({
                        "type": "excessive_loop",
                        "activity": current_activity,
                        "count": consecutive_count,
                        "threshold": self.loop_threshold,
                        "description": f"Atividade '{current_activity}' repetida {consecutive_count} vezes consecutivas"
                    })
                
                # Reset para nova atividade
                current_activity = activity
                consecutive_count = 1
        
        # Verifica último grupo
        if consecutive_count > self.loop_threshold:
            loops.append({
                "type": "excessive_loop",
                "activity": current_activity,
                "count": consecutive_count,
                "threshold": self.loop_threshold,
                "description": f"Atividade '{current_activity}' repetida {consecutive_count} vezes consecutivas"
            })
        
        return loops
