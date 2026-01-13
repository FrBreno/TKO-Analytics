"""
Conformance Checker for TKO Analytics.

Este módulo verifica se o comportamento dos estudantes está em conformidade
com modelos de processo ideais/normativos definidos para as tarefas.

Métricas calculadas:
- Fitness: capacidade do modelo reproduzir o log observado
- Precision: evita comportamentos não observados
- Generalization: evita overfitting
- Simplicity: complexidade do modelo
"""

import structlog
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass
from pathlib import Path

try:
    import pm4py
    from pm4py.objects.log.obj import EventLog
    from pm4py.objects.petri_net.obj import PetriNet, Marking
    from pm4py.objects.petri_net.utils import petri_utils
    PM4PY_AVAILABLE = True
except ImportError:
    PM4PY_AVAILABLE = False
    EventLog = None
    PetriNet = None
    Marking = None

logger = structlog.get_logger()


class ConformanceCheckingError(Exception):
    """Exceção para erros de conformance checking."""
    pass


@dataclass
class ConformanceResult:
    """Resultado da análise de conformidade."""
    
    # Identificação
    student_hash: Optional[str] = None
    task_id: Optional[str] = None
    
    # Métricas principais
    fitness: float = 0.0
    precision: float = 0.0
    
    # Métricas adicionais (se disponíveis)
    generalization: Optional[float] = None
    simplicity: Optional[float] = None
    
    # Diagnóstico de desvios
    num_deviations: int = 0
    deviation_types: List[str] = None
    
    # Classificação de conformidade
    @property
    def conformance_level(self) -> str:
        """
        Classifica o nível de conformidade:
        - Excelente: fitness >= 95% e precision >= 80%
        - Bom: fitness >= 85% e precision >= 60%
        - Regular: fitness >= 70% e precision >= 40%
        - Baixo: fitness < 70% ou precision < 40%
        """
        if self.fitness >= 0.95 and self.precision >= 0.80:
            return "Excelente"
        elif self.fitness >= 0.85 and self.precision >= 0.60:
            return "Bom"
        elif self.fitness >= 0.70 and self.precision >= 0.40:
            return "Regular"
        else:
            return "Baixo"
    
    def __str__(self) -> str:
        """Representação string do resultado."""
        lines = [
            f"Conformance Analysis Result",
            f"",
            f"Conformance Level: {self.conformance_level}",
            f"",
            f"Metrics:",
            f"   * Fitness: {self.fitness:.2%}",
            f"   * Precision: {self.precision:.2%}",
        ]
        
        if self.generalization is not None:
            lines.append(f"   * Generalization: {self.generalization:.2%}")
        if self.simplicity is not None:
            lines.append(f"   * Simplicity: {self.simplicity:.2%}")
        
        if self.num_deviations > 0:
            lines.extend([
                f"",
                f"Deviations: {self.num_deviations} detected",
            ])
            if self.deviation_types:
                lines.append(f"Types: {', '.join(self.deviation_types)}")
        
        return "\n".join(lines)


class ConformanceChecker:
    """Verificador de conformidade de processos educacionais TKO."""
    
    def __init__(self):
        """Inicializa o conformance checker."""
        if not PM4PY_AVAILABLE:
            raise ConformanceCheckingError(
                "PM4Py não está instalado. Execute: pip install pm4py"
            )
        
        self.ideal_models: Dict[str, Tuple[PetriNet, Marking, Marking]] = {}
        
        logger.info("[ConformanceChecker.__init__] - conformance_checker_initialized")
    
    def define_ideal_tko_model(self) -> Tuple[PetriNet, Marking, Marking]:
        """
        Define o modelo de processo IDEAL para TKO.
        
        Processo esperado:
        1. DOWN - Baixar tarefa
        2. (EDIT → EXEC)* - Ciclo de edição e execução (iterativo)
        3. SELF - Autoavaliação final
        
        Returns:
            Tupla (petri_net, initial_marking, final_marking)
        """
        logger.info("[ConformanceChecker.define_ideal_tko_model] - creating_ideal_model")
        
        # Cria rede de Petri
        net = PetriNet("TKO_Ideal_Process")
        
        # Places
        start = PetriNet.Place("start")
        after_down = PetriNet.Place("after_down")
        after_edit = PetriNet.Place("after_edit")
        after_exec = PetriNet.Place("after_exec")
        end = PetriNet.Place("end")
        
        net.places.add(start)
        net.places.add(after_down)
        net.places.add(after_edit)
        net.places.add(after_exec)
        net.places.add(end)
        
        # Transitions (atividades)
        down_task = PetriNet.Transition("down_task", label="Download Task")
        edit_code = PetriNet.Transition("edit_code", label="Edit Code")
        exec_code = PetriNet.Transition("exec_code", label="Execute Code")
        self_assess = PetriNet.Transition("self_assess", label="Self-Assessment")
        
        # Transição invisível para permitir múltiplos ciclos edit/exec
        skip_cycle = PetriNet.Transition("skip_cycle", label=None)  # transição invisível
        
        net.transitions.add(down_task)
        net.transitions.add(edit_code)
        net.transitions.add(exec_code)
        net.transitions.add(self_assess)
        net.transitions.add(skip_cycle)
        
        # Arcos (fluxo)
        # START → DOWN
        petri_utils.add_arc_from_to(start, down_task, net)
        petri_utils.add_arc_from_to(down_task, after_down, net)
        
        # DOWN → EDIT
        petri_utils.add_arc_from_to(after_down, edit_code, net)
        petri_utils.add_arc_from_to(edit_code, after_edit, net)
        
        # EDIT → EXEC
        petri_utils.add_arc_from_to(after_edit, exec_code, net)
        petri_utils.add_arc_from_to(exec_code, after_exec, net)
        
        # EXEC → EDIT (ciclo iterativo)
        petri_utils.add_arc_from_to(after_exec, edit_code, net)
        
        # EXEC → SELF (pode pular mais iterações)
        petri_utils.add_arc_from_to(after_exec, skip_cycle, net)
        petri_utils.add_arc_from_to(skip_cycle, after_down, net)  # volta para before_self
        
        # EXEC → SELF (finalizar)
        petri_utils.add_arc_from_to(after_exec, self_assess, net)
        petri_utils.add_arc_from_to(self_assess, end, net)
        
        # Markings
        initial_marking = Marking()
        initial_marking[start] = 1
        
        final_marking = Marking()
        final_marking[end] = 1
        
        logger.info(
            "[ConformanceChecker.define_ideal_tko_model] - ideal_model_created",
            places=len(net.places),
            transitions=len(net.transitions),
            arcs=len(net.arcs)
        )
        
        return net, initial_marking, final_marking
    
    def register_ideal_model(
        self,
        task_id: str,
        petri_net: PetriNet,
        initial_marking: Marking,
        final_marking: Marking
    ) -> None:
        """
        Registra um modelo ideal para uma tarefa específica.
        
        Args:
            task_id: ID da tarefa
            petri_net: Rede de Petri do modelo ideal
            initial_marking: Marcação inicial
            final_marking: Marcação final
        """
        self.ideal_models[task_id] = (petri_net, initial_marking, final_marking)
        
        logger.info(
            "[ConformanceChecker.register_ideal_model] - model_registered",
            task_id=task_id,
            places=len(petri_net.places),
            transitions=len(petri_net.transitions)
        )
    
    def check_conformance(
        self,
        log: EventLog,
        model: Optional[Tuple[PetriNet, Marking, Marking]] = None,
        task_id: Optional[str] = None
    ) -> ConformanceResult:
        """
        Verifica conformidade do log contra um modelo.
        
        Args:
            log: Event log (PM4Py)
            model: Modelo (petri_net, im, fm) - se None, usa modelo ideal TKO
            task_id: ID da tarefa (para buscar modelo registrado)
            
        Returns:
            ConformanceResult com métricas
        """
        logger.info(
            "[ConformanceChecker.check_conformance] - checking_conformance",
            traces=len(log),
            task_id=task_id
        )
        
        # Determina qual modelo usar
        if model is not None:
            net, im, fm = model
        elif task_id and task_id in self.ideal_models:
            net, im, fm = self.ideal_models[task_id]
        else:
            # Usa modelo ideal padrão TKO
            net, im, fm = self.define_ideal_tko_model()
        
        try:
            # Fitness: reproduzibilidade do log pelo modelo
            fitness_result = pm4py.fitness_token_based_replay(log, net, im, fm)
            fitness = fitness_result['log_fitness']
            
            # Precision: especificidade do modelo
            precision = pm4py.precision_token_based_replay(log, net, im, fm)
            
            # Identifica desvios
            alignments = pm4py.conformance_diagnostics_token_based_replay(log, net, im, fm)
            
            # Conta desvios
            num_deviations = 0
            deviation_types = set()
            
            for alignment in alignments:
                if 'alignment' in alignment:
                    for move in alignment['alignment']:
                        # move = ((log_move, model_move), cost)
                        log_move, model_move = move[0]
                        
                        # Skip move (modelo avança sem log)
                        if log_move == '>>' and model_move != '>>':
                            num_deviations += 1
                            deviation_types.add("skip")
                        
                        # Insert move (log avança sem modelo)
                        elif log_move != '>>' and model_move == '>>':
                            num_deviations += 1
                            deviation_types.add("insert")
            
            result = ConformanceResult(
                task_id=task_id,
                fitness=fitness,
                precision=precision,
                num_deviations=num_deviations,
                deviation_types=list(deviation_types) if deviation_types else []
            )
            
            logger.info(
                "[ConformanceChecker.check_conformance] - conformance_computed",
                task_id=task_id,
                fitness=fitness,
                precision=precision,
                deviations=num_deviations,
                level=result.conformance_level
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "[ConformanceChecker.check_conformance] - conformance_check_failed",
                error=str(e),
                task_id=task_id
            )
            raise ConformanceCheckingError(f"Falha no conformance checking: {e}")
    
    def check_conformance_from_xes(
        self,
        xes_path: str,
        model: Optional[Tuple[PetriNet, Marking, Marking]] = None,
        task_id: Optional[str] = None
    ) -> ConformanceResult:
        """
        Verifica conformidade a partir de arquivo XES.
        
        Args:
            xes_path: Caminho para arquivo XES
            model: Modelo (opcional)
            task_id: ID da tarefa (opcional)
            
        Returns:
            ConformanceResult
        """
        xes_file = Path(xes_path)
        if not xes_file.exists():
            raise ConformanceCheckingError(f"Arquivo XES não encontrado: {xes_path}")
        
        logger.info(
            "[ConformanceChecker.check_conformance_from_xes] - loading_xes",
            path=xes_path
        )
        
        log = pm4py.read_xes(str(xes_file))
        
        return self.check_conformance(log, model=model, task_id=task_id)
    
    def batch_check_conformance(
        self,
        logs: Dict[str, EventLog],
        task_id: Optional[str] = None
    ) -> Dict[str, ConformanceResult]:
        """
        Verifica conformidade para múltiplos logs (e.g., múltiplos estudantes).
        
        Args:
            logs: Dicionário {identifier: EventLog}
            task_id: ID da tarefa (para buscar modelo registrado)
            
        Returns:
            Dicionário {identifier: ConformanceResult}
        """
        logger.info(
            "[ConformanceChecker.batch_check_conformance] - batch_checking",
            num_logs=len(logs),
            task_id=task_id
        )
        
        results = {}
        
        for identifier, log in logs.items():
            try:
                result = self.check_conformance(log, task_id=task_id)
                result.student_hash = identifier
                results[identifier] = result
            except Exception as e:
                logger.error(
                    "[ConformanceChecker.batch_check_conformance] - check_failed",
                    identifier=identifier,
                    error=str(e)
                )
                # Continua para os próximos
        
        logger.info(
            "[ConformanceChecker.batch_check_conformance] - batch_completed",
            successful=len(results),
            total=len(logs)
        )
        
        return results
    
    def compare_conformance(
        self,
        results: Dict[str, ConformanceResult]
    ) -> Dict[str, any]:
        """
        Compara resultados de conformidade de múltiplos estudantes.
        
        Args:
            results: Dicionário {student_hash: ConformanceResult}
            
        Returns:
            Dicionário com estatísticas comparativas
        """
        if not results:
            return {}
        
        fitness_values = [r.fitness for r in results.values()]
        precision_values = [r.precision for r in results.values()]
        
        # Estatísticas
        stats = {
            'num_students': len(results),
            'fitness': {
                'avg': sum(fitness_values) / len(fitness_values),
                'min': min(fitness_values),
                'max': max(fitness_values),
            },
            'precision': {
                'avg': sum(precision_values) / len(precision_values),
                'min': min(precision_values),
                'max': max(precision_values),
            },
            'conformance_levels': {}
        }
        
        # Conta distribuição de níveis
        for result in results.values():
            level = result.conformance_level
            stats['conformance_levels'][level] = stats['conformance_levels'].get(level, 0) + 1
        
        # Identifica outliers
        stats['best_students'] = [
            sid for sid, r in results.items()
            if r.fitness >= stats['fitness']['avg'] and r.precision >= stats['precision']['avg']
        ]
        
        stats['struggling_students'] = [
            sid for sid, r in results.items()
            if r.fitness < stats['fitness']['avg'] * 0.8 or r.precision < stats['precision']['avg'] * 0.8
        ]
        
        logger.info(
            "[ConformanceChecker.compare_conformance] - comparison_completed",
            num_students=len(results),
            avg_fitness=stats['fitness']['avg'],
            avg_precision=stats['precision']['avg']
        )
        
        return stats
