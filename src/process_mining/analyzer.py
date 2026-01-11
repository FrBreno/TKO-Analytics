"""
Analisador de processos usando PM4Py.

Funcionalidades:
- Importação de logs XES
- Process discovery (Inductive Miner, Heuristic Miner)
- Análise de conformidade (fitness, precision)
- Análise de variantes de processo
- Estatísticas de execução
"""

import structlog
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional

try:
    import pm4py
    from pm4py.objects.log.obj import EventLog
    from pm4py.objects.petri_net.obj import PetriNet, Marking
    PM4PY_AVAILABLE = True
except ImportError:
    PM4PY_AVAILABLE = False
    EventLog = Any
    PetriNet = Any
    Marking = Any

logger = structlog.get_logger()


class ProcessMiningError(Exception):
    """Exceção para erros de Process Mining."""
    pass


@dataclass
class ProcessAnalysisResult:
    """Resultado da análise de processo."""
    
    # Estatísticas do log
    num_traces: int
    num_events: int
    num_activities: int
    num_resources: int
    
    # Variantes de processo
    num_variants: int
    top_variants: List[Tuple[str, int]]  # (variant_path, count)
    
    # Métricas de conformidade (se modelo disponível)
    fitness: Optional[float] = None
    precision: Optional[float] = None
    
    # Estatísticas temporais
    avg_trace_duration_seconds: Optional[float] = None
    median_trace_duration_seconds: Optional[float] = None
    
    # Modelos descobertos (metadata)
    model_type: Optional[str] = None
    model_complexity: Optional[Dict[str, int]] = None
    
    def __str__(self) -> str:
        """Representação string do resultado."""
        lines = [
            f"Process Analysis Results",
            f"",
            f"Log Statistics:",
            f"   • Traces: {self.num_traces}",
            f"   • Events: {self.num_events}",
            f"   • Activities: {self.num_activities}",
            f"   • Resources: {self.num_resources}",
            f"",
            f"Process Variants:",
            f"   • Total variants: {self.num_variants}",
        ]
        
        if self.top_variants:
            lines.append(f"   • Top 5 variants:")
            for variant, count in self.top_variants[:5]:
                percentage = (count / self.num_traces) * 100
                lines.append(f"      - {count} traces ({percentage:.1f}%): {variant}")
        
        if self.fitness is not None and self.precision is not None:
            lines.extend([
                f"",
                f"Conformance Metrics:",
                f"   • Fitness: {self.fitness:.2%}",
                f"   • Precision: {self.precision:.2%}",
            ])
        
        if self.avg_trace_duration_seconds:
            lines.extend([
                f"",
                f"Temporal Statistics:",
                f"   • Avg duration: {self.avg_trace_duration_seconds:.1f}s",
                f"   • Median duration: {self.median_trace_duration_seconds:.1f}s",
            ])
        
        if self.model_type:
            lines.extend([
                f"",
                f"Discovered Model:",
                f"   • Type: {self.model_type}",
            ])
            if self.model_complexity:
                lines.append(f"   • Complexity: {self.model_complexity}")
        
        return "\n".join(lines)


class ProcessAnalyzer:
    """Analisador de processos educacionais usando PM4Py."""
    
    def __init__(self):
        """Inicializa o analisador."""
        if not PM4PY_AVAILABLE:
            raise ProcessMiningError(
                "PM4Py não está instalado. Execute: pip install pm4py"
            )
        
        self.log: Optional[EventLog] = None
        self.petri_net: Optional[PetriNet] = None
        self.initial_marking: Optional[Marking] = None
        self.final_marking: Optional[Marking] = None
        
        logger.info("[ProcessAnalyzer.__init__] - process_analyzer_initialized")
    
    def load_xes(self, xes_path: str) -> EventLog:
        """
        Carrega um arquivo XES.
        
        Args:
            xes_path: Caminho para o arquivo XES
            
        Returns:
            EventLog do PM4Py
            
        Raises:
            ProcessMiningError: Se o arquivo não puder ser carregado
        """
        xes_file = Path(xes_path)
        if not xes_file.exists():
            raise ProcessMiningError(f"Arquivo XES não encontrado: {xes_path}")
        
        logger.info("[ProcessAnalyzer.load_xes] - loading_xes", path=xes_path)
        
        try:
            self.log = pm4py.read_xes(str(xes_file))
            
            num_traces = len(self.log)
            num_events = sum(len(trace) for trace in self.log)
            
            logger.info(
                "[ProcessAnalyzer.load_xes] - xes_loaded_successfully",
                traces=num_traces,
                events=num_events,
                path=xes_path
            )
            
            return self.log
            
        except Exception as e:
            logger.error("[ProcessAnalyzer.load_xes] - xes_load_failed", error=str(e), path=xes_path)
            raise ProcessMiningError(f"Falha ao carregar XES: {e}")
    
    def discover_process_inductive(self) -> Tuple[PetriNet, Marking, Marking]:
        """
        Descobre modelo de processo usando Inductive Miner.
        
        Returns:
            Tupla (petri_net, initial_marking, final_marking)
            
        Raises:
            ProcessMiningError: Se log não foi carregado ou descoberta falhou
        """
        if self.log is None:
            raise ProcessMiningError("Carregue um log XES primeiro usando load_xes()")
        
        logger.info("[ProcessAnalyzer.discover_process_inductive] - discovering_process", algorithm="inductive_miner")
        
        try:
            net, im, fm = pm4py.discover_petri_net_inductive(self.log)
            
            self.petri_net = net
            self.initial_marking = im
            self.final_marking = fm
            
            logger.info(
                "[ProcessAnalyzer.discover_process_inductive] - process_discovered",
                places=len(net.places),
                transitions=len(net.transitions),
                arcs=len(net.arcs)
            )
            
            return net, im, fm
            
        except Exception as e:
            logger.error("[ProcessAnalyzer.discover_process_inductive] - process_discovery_failed", error=str(e))
            raise ProcessMiningError(f"Falha na descoberta de processo: {e}")
    
    def analyze_variants(self, top_n: int = 10) -> Dict[str, Any]:
        """
        Analisa variantes de processo no log.
        
        Args:
            top_n: Número de variantes top para retornar
            
        Returns:
            Dicionário com estatísticas de variantes
            
        Raises:
            ProcessMiningError: Se log não foi carregado
        """
        if self.log is None:
            raise ProcessMiningError("Carregue um log XES primeiro usando load_xes()")
        
        logger.info("[ProcessAnalyzer.analyze_variants] - analyzing_variants", top_n=top_n)
        
        try:
            variants = pm4py.get_variants(self.log)
            
            # PM4Py pode retornar {variant_tuple: count} ou {variant_tuple: [traces]}
            # Normaliza para (variant, count)
            variant_counts = {}
            for variant, value in variants.items():
                if isinstance(value, int):
                    variant_counts[variant] = value
                else:
                    variant_counts[variant] = len(value)
            
            # Ordena por frequência
            sorted_variants = sorted(
                variant_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Formata variantes como strings
            variant_list = [
                (str(variant), count)
                for variant, count in sorted_variants[:top_n]
            ]
            
            result = {
                'total_variants': len(variants),
                'top_variants': variant_list,
                'variant_distribution': {
                    str(v): c for v, c in sorted_variants
                }
            }
            
            logger.info(
                "[ProcessAnalyzer.analyze_variants] - variants_analyzed",
                total=len(variants),
                top_n=min(top_n, len(variants))
            )
            
            return result
            
        except Exception as e:
            logger.error("[ProcessAnalyzer.analyze_variants] - variant_analysis_failed", error=str(e))
            raise ProcessMiningError(f"Falha na análise de variantes: {e}")
    
    def compute_conformance(self) -> Dict[str, float]:
        """
        Calcula métricas de conformidade (fitness e precision).
        
        Requer que um modelo tenha sido descoberto com discover_process_inductive().
        
        Returns:
            Dicionário com fitness e precision
            
        Raises:
            ProcessMiningError: Se log ou modelo não disponível
        """
        if self.log is None:
            raise ProcessMiningError("Carregue um log XES primeiro usando load_xes()")
        
        if self.petri_net is None:
            raise ProcessMiningError(
                "Descubra um modelo primeiro usando discover_process_inductive()"
            )
        
        logger.info("[ProcessAnalyzer.compute_conformance] - computing_conformance")
        
        try:
            # Fitness: quantos traces do log são reproduzíveis pelo modelo
            fitness = pm4py.fitness_token_based_replay(
                self.log,
                self.petri_net,
                self.initial_marking,
                self.final_marking
            )
            
            # Precision: quão preciso é o modelo (evita comportamentos não observados)
            precision = pm4py.precision_token_based_replay(
                self.log,
                self.petri_net,
                self.initial_marking,
                self.final_marking
            )
            
            result = {
                'fitness': fitness['log_fitness'],
                'precision': precision
            }
            
            logger.info(
                "[ProcessAnalyzer.compute_conformance] - conformance_computed",
                fitness=result['fitness'],
                precision=result['precision']
            )
            
            return result
            
        except Exception as e:
            logger.error("[ProcessAnalyzer.compute_conformance] - conformance_computation_failed", error=str(e))
            raise ProcessMiningError(f"Falha no cálculo de conformidade: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtém estatísticas básicas do log.
        
        Returns:
            Dicionário com estatísticas
            
        Raises:
            ProcessMiningError: Se log não foi carregado
        """
        if self.log is None:
            raise ProcessMiningError("Carregue um log XES primeiro usando load_xes()")
        
        try:
            num_traces = len(self.log)
            num_events = sum(len(trace) for trace in self.log)
            
            # Coleta atividades únicas
            activities = set()
            resources = set()
            
            for trace in self.log:
                for event in trace:
                    # PM4Py usa objetos Event com __getitem__
                    try:
                        if 'concept:name' in event:
                            activities.add(event['concept:name'])
                        if 'org:resource' in event:
                            resources.add(event['org:resource'])
                    except (TypeError, KeyError):
                        # Formato alternativo: atributos diretos
                        pass
            
            # Durações de traces
            durations = []
            for trace in self.log:
                if len(trace) >= 2:
                    try:
                        start = trace[0]['time:timestamp']
                        end = trace[-1]['time:timestamp']
                        duration = (end - start).total_seconds()
                        durations.append(duration)
                    except (TypeError, KeyError, AttributeError):
                        # Não foi possível calcular duração para este trace
                        pass
            
            avg_duration = sum(durations) / len(durations) if durations else 0
            median_duration = sorted(durations)[len(durations) // 2] if durations else 0
            
            return {
                'num_traces': num_traces,
                'num_events': num_events,
                'num_activities': len(activities),
                'num_resources': len(resources),
                'activities': sorted(activities),
                'resources': sorted(resources),
                'avg_trace_duration_seconds': avg_duration,
                'median_trace_duration_seconds': median_duration,
            }
            
        except Exception as e:
            logger.error("[ProcessAnalyzer.get_statistics] - statistics_computation_failed", error=str(e))
            raise ProcessMiningError(f"Falha ao calcular estatísticas: {e}")
    
    def analyze(
        self,
        xes_path: str,
        discover_model: bool = True,
        compute_conformance: bool = True,
        top_variants: int = 10
    ) -> ProcessAnalysisResult:
        """
        Análise completa de processo.
        
        Args:
            xes_path: Caminho para arquivo XES
            discover_model: Se deve descobrir modelo de processo
            compute_conformance: Se deve calcular conformidade (requer discover_model=True)
            top_variants: Número de variantes top para retornar
            
        Returns:
            ProcessAnalysisResult com todas as análises
        """
        logger.info(
            "[ProcessAnalyzer.analyze] - starting_full_analysis",
            xes_path=xes_path,
            discover_model=discover_model,
            compute_conformance=compute_conformance
        )
        
        # Carrega log
        self.load_xes(xes_path)
        
        # Estatísticas básicas
        stats = self.get_statistics()
        
        # Análise de variantes
        variants = self.analyze_variants(top_n=top_variants)
        
        # Descoberta de modelo
        model_type = None
        model_complexity = None
        
        if discover_model:
            net, im, fm = self.discover_process_inductive()
            model_type = "Petri Net (Inductive Miner)"
            model_complexity = {
                'places': len(net.places),
                'transitions': len(net.transitions),
                'arcs': len(net.arcs)
            }
        
        # Conformidade
        fitness = None
        precision = None
        
        if compute_conformance and discover_model:
            conformance = self.compute_conformance()
            fitness = conformance['fitness']
            precision = conformance['precision']
        
        # Monta resultado
        result = ProcessAnalysisResult(
            num_traces=stats['num_traces'],
            num_events=stats['num_events'],
            num_activities=stats['num_activities'],
            num_resources=stats['num_resources'],
            num_variants=variants['total_variants'],
            top_variants=variants['top_variants'],
            fitness=fitness,
            precision=precision,
            avg_trace_duration_seconds=stats['avg_trace_duration_seconds'],
            median_trace_duration_seconds=stats['median_trace_duration_seconds'],
            model_type=model_type,
            model_complexity=model_complexity
        )
        
        logger.info("[ProcessAnalyzer.analyze] - full_analysis_completed")
        
        return result
    
    def save_model_visualization(self, output_path: str, format: str = 'png') -> None:
        """
        Salva visualização do modelo descoberto.
        
        Args:
            output_path: Caminho para salvar a visualização
            format: Formato da imagem ('png', 'svg', 'pdf')
            
        Raises:
            ProcessMiningError: Se modelo não foi descoberto
        """
        if self.petri_net is None:
            raise ProcessMiningError(
                "Descubra um modelo primeiro usando discover_process_inductive()"
            )
        
        try:
            pm4py.save_vis_petri_net(
                self.petri_net,
                self.initial_marking,
                self.final_marking,
                output_path,
                variant=format
            )
            
            logger.info(
                "[ProcessAnalyzer.save_model_visualization] - model_visualization_saved",
                path=output_path,
                format=format
            )
            
        except Exception as e:
            logger.error("[ProcessAnalyzer.save_model_visualization] - model_visualization_failed", error=str(e))
            raise ProcessMiningError(f"Falha ao salvar visualização: {e}")
