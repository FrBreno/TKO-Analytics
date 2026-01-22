"""
Behavioral Pattern Detector for TKO Analytics.

Este módulo detecta padrões comportamentais dos estudantes a partir
dos eventos TKO, incluindo:

- Cramming: trabalho concentrado nas últimas horas (procrastinação extrema)
- Trial-and-Error: muitas execuções sem edição significativa
- Procrastination: gaps longos entre sessões
- Code Thrashing: muitas edições sem testar (execução)
- Success Trajectory: padrão de evolução até o sucesso
"""

import structlog
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

logger = structlog.get_logger()


@dataclass
class BehavioralPattern:
    """Representa um padrão comportamental detectado."""
    
    # Identificação
    student_hash: str
    task_id: str
    pattern_type: str  # cramming, trial_and_error, procrastination, code_thrashing
    
    # Confiança da detecção (0.0 a 1.0)
    confidence: float
    
    # Evidências que suportam a detecção
    evidence: Dict[str, any]
    
    # Timestamp da detecção
    detected_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Valida após inicialização."""
        if self.detected_at is None:
            self.detected_at = datetime.now()
        
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")
    
    def __str__(self) -> str:
        """Representação string do padrão."""
        pattern_names = {
            'cramming': 'Cramming (Procrastinação Extrema)',
            'trial_and_error': 'Trial-and-Error Excessivo',
            'procrastination': 'Procrastinação',
            'code_thrashing': 'Code Thrashing (Edição Desordenada)',
            'success_trajectory': 'Trajetória de Sucesso',
        }
        
        name = pattern_names.get(self.pattern_type, self.pattern_type)
        
        lines = [
            f"Pattern: {name}",
            f"Student: {self.student_hash[:8]}...",
            f"Task: {self.task_id}",
            f"Confidence: {self.confidence:.1%}",
            f"Evidence:",
        ]
        
        for key, value in self.evidence.items():
            lines.append(f"   * {key}: {value}")
        
        return "\n".join(lines)


class BehavioralPatternDetector:
    """Detector de padrões comportamentais a partir de eventos TKO."""
    
    def __init__(self, db_path: str):
        """
        Inicializa o detector.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
        """
        self.db_path = db_path
        
        logger.info(
            "[BehavioralPatternDetector.__init__] - detector_initialized",
            db_path=db_path
        )
    
    def detect_cramming(
        self,
        student_hash: str,
        task_id: str,
        threshold: float = 0.70
    ) -> Optional[BehavioralPattern]:
        """
        Detecta cramming: trabalho concentrado nas últimas 48 horas.
        
        Critério: Se 70%+ do trabalho (por tempo ou eventos) aconteceu
        nas últimas 48h antes do último evento.
        
        Args:
            student_hash: Hash do estudante
            task_id: ID da tarefa
            threshold: Limiar para detecção (padrão: 70%)
            
        Returns:
            BehavioralPattern se detectado, None caso contrário
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca eventos da tarefa
            cursor.execute("""
                SELECT timestamp, duration_seconds
                FROM events
                WHERE student_hash = ? AND task_id = ?
                ORDER BY timestamp ASC
            """, (student_hash, task_id))
            
            events = cursor.fetchall()
            
            if len(events) < 5:
                # Poucos eventos, não é possível detectar cramming
                return None
            
            # Parse timestamps
            timestamps = []
            for event in events:
                try:
                    ts = datetime.fromisoformat(event[0].replace('Z', '+00:00'))
                except:
                    ts = datetime.strptime(event[0], '%Y-%m-%d %H:%M:%S')
                timestamps.append(ts)
            
            first_ts = timestamps[0]
            last_ts = timestamps[-1]
            
            # Define janela de 48h antes do último evento
            cramming_window_start = last_ts - timedelta(hours=48)
            
            # Conta eventos na janela de cramming
            events_in_window = sum(1 for ts in timestamps if ts >= cramming_window_start)
            cramming_ratio = events_in_window / len(timestamps)
            
            if cramming_ratio >= threshold:
                # Cramming detectado!
                evidence = {
                    'total_events': len(timestamps),
                    'events_in_last_48h': events_in_window,
                    'cramming_ratio': f"{cramming_ratio:.1%}",
                    'first_event': first_ts.isoformat(),
                    'last_event': last_ts.isoformat(),
                    'total_duration_hours': (last_ts - first_ts).total_seconds() / 3600,
                }
                
                pattern = BehavioralPattern(
                    student_hash=student_hash,
                    task_id=task_id,
                    pattern_type='cramming',
                    confidence=cramming_ratio,
                    evidence=evidence
                )
                
                logger.info(
                    "[BehavioralPatternDetector.detect_cramming] - cramming_detected",
                    student_hash=student_hash[:8],
                    task_id=task_id,
                    ratio=cramming_ratio
                )
                
                return pattern
            
            return None
            
        finally:
            conn.close()
    
    def detect_trial_and_error(
        self,
        student_hash: str,
        task_id: str,
        min_execs: int = 10,
        low_success_rate: float = 0.3
    ) -> Optional[BehavioralPattern]:
        """
        Detecta trial-and-error: muitas execuções com baixa taxa de sucesso.
        
        Critério: Muitas execuções (>= min_execs) com sucesso abaixo de low_success_rate.
        
        Args:
            student_hash: Hash do estudante
            task_id: ID da tarefa
            min_execs: Mínimo de execuções para considerar
            low_success_rate: Taxa de sucesso considerada baixa (padrão: 30%)
            
        Returns:
            BehavioralPattern se detectado, None caso contrário
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Busca eventos EXEC (ExecEvent)
            cursor.execute("""
                SELECT metadata
                FROM events
                WHERE student_hash = ? AND task_id = ?
                  AND (event_type = 'ExecEvent' OR event_type = 'exec')
                ORDER BY timestamp ASC
            """, (student_hash, task_id))
            
            exec_events = cursor.fetchall()
            
            if len(exec_events) < min_execs:
                # Poucas execuções
                return None
            
            # Conta sucessos (result=True no metadata)
            import json
            successes = 0
            
            for event in exec_events:
                try:
                    metadata = json.loads(event['metadata']) if event['metadata'] else {}
                    if metadata.get('result') is True:
                        successes += 1
                except:
                    pass
            
            success_rate = successes / len(exec_events) if exec_events else 0
            
            if success_rate <= low_success_rate:
                # Trial-and-error detectado!
                evidence = {
                    'total_executions': len(exec_events),
                    'successful_executions': successes,
                    'success_rate': f"{success_rate:.1%}",
                    'failures': len(exec_events) - successes,
                }
                
                # Confiança aumenta com mais execuções e menor taxa de sucesso
                confidence = min(0.9, (len(exec_events) / 20) * (1 - success_rate))
                
                pattern = BehavioralPattern(
                    student_hash=student_hash,
                    task_id=task_id,
                    pattern_type='trial_and_error',
                    confidence=confidence,
                    evidence=evidence
                )
                
                logger.info(
                    "[BehavioralPatternDetector.detect_trial_and_error] - trial_and_error_detected",
                    student_hash=student_hash[:8],
                    task_id=task_id,
                    execs=len(exec_events),
                    success_rate=success_rate
                )
                
                return pattern
            
            return None
            
        finally:
            conn.close()
    
    def detect_procrastination(
        self,
        student_hash: str,
        task_id: str,
        long_gap_hours: float = 72.0
    ) -> Optional[BehavioralPattern]:
        """
        Detecta procrastinação: gaps longos entre sessões de trabalho.
        
        Critério: Pelo menos um gap de >= long_gap_hours entre eventos consecutivos.
        
        Args:
            student_hash: Hash do estudante
            task_id: ID da tarefa
            long_gap_hours: Duração considerada como gap longo (padrão: 72h = 3 dias)
            
        Returns:
            BehavioralPattern se detectado, None caso contrário
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT timestamp
                FROM events
                WHERE student_hash = ? AND task_id = ?
                ORDER BY timestamp ASC
            """, (student_hash, task_id))
            
            events = cursor.fetchall()
            
            if len(events) < 2:
                return None
            
            # Parse timestamps
            timestamps = []
            for event in events:
                try:
                    ts = datetime.fromisoformat(event[0].replace('Z', '+00:00'))
                except:
                    ts = datetime.strptime(event[0], '%Y-%m-%d %H:%M:%S')
                timestamps.append(ts)
            
            # Calcula gaps entre eventos consecutivos
            gaps = []
            for i in range(1, len(timestamps)):
                gap = (timestamps[i] - timestamps[i-1]).total_seconds() / 3600  # em horas
                gaps.append(gap)
            
            # Encontra gaps longos
            long_gaps = [g for g in gaps if g >= long_gap_hours]
            
            if long_gaps:
                # Procrastinação detectada!
                max_gap = max(long_gaps)
                avg_gap = sum(gaps) / len(gaps)
                
                evidence = {
                    'total_events': len(timestamps),
                    'num_long_gaps': len(long_gaps),
                    'max_gap_hours': round(max_gap, 1),
                    'avg_gap_hours': round(avg_gap, 1),
                    'total_duration_days': round((timestamps[-1] - timestamps[0]).total_seconds() / 86400, 1),
                }
                
                # Confiança baseada em quantos gaps longos e quão longos
                confidence = min(0.95, len(long_gaps) * 0.2 + (max_gap / (long_gap_hours * 2)))
                
                pattern = BehavioralPattern(
                    student_hash=student_hash,
                    task_id=task_id,
                    pattern_type='procrastination',
                    confidence=confidence,
                    evidence=evidence
                )
                
                logger.info(
                    "[BehavioralPatternDetector.detect_procrastination] - procrastination_detected",
                    student_hash=student_hash[:8],
                    task_id=task_id,
                    long_gaps=len(long_gaps),
                    max_gap=max_gap
                )
                
                return pattern
            
            return None
            
        finally:
            conn.close()
    
    def detect_code_thrashing(
        self,
        student_hash: str,
        task_id: str,
        edit_exec_ratio_threshold: float = 5.0
    ) -> Optional[BehavioralPattern]:
        """
        Detecta code thrashing: muitas edições sem execução (não testa).
        
        Critério: Ratio edit/exec >= edit_exec_ratio_threshold.
        
        Args:
            student_hash: Hash do estudante
            task_id: ID da tarefa
            edit_exec_ratio_threshold: Ratio mínimo para detecção (padrão: 5.0)
            
        Returns:
            BehavioralPattern se detectado, None caso contrário
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Conta eventos MOVE (edições) e EXEC (execuções)
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN event_type IN ('MoveEvent', 'move') THEN 1 ELSE 0 END) as move_count,
                    SUM(CASE WHEN event_type IN ('ExecEvent', 'exec') THEN 1 ELSE 0 END) as exec_count
                FROM events
                WHERE student_hash = ? AND task_id = ?
            """, (student_hash, task_id))
            
            result = cursor.fetchone()
            move_count = result[0] or 0
            exec_count = result[1] or 0
            
            if exec_count == 0:
                # Sem execuções - não é code thrashing, é falta de teste completo
                return None
            
            edit_exec_ratio = move_count / exec_count if exec_count > 0 else 0
            
            if edit_exec_ratio >= edit_exec_ratio_threshold:
                # Code thrashing detectado!
                evidence = {
                    'edit_count': move_count,
                    'exec_count': exec_count,
                    'edit_exec_ratio': round(edit_exec_ratio, 2),
                    'interpretation': f"Edita {edit_exec_ratio:.1f}x mais do que executa",
                }
                
                # Confiança aumenta com ratio maior
                confidence = min(0.95, edit_exec_ratio / (edit_exec_ratio_threshold * 2))
                
                pattern = BehavioralPattern(
                    student_hash=student_hash,
                    task_id=task_id,
                    pattern_type='code_thrashing',
                    confidence=confidence,
                    evidence=evidence
                )
                
                logger.info(
                    "[BehavioralPatternDetector.detect_code_thrashing] - code_thrashing_detected",
                    student_hash=student_hash[:8],
                    task_id=task_id,
                    ratio=edit_exec_ratio
                )
                
                return pattern
            
            return None
            
        finally:
            conn.close()
    
    def detect_all_patterns(
        self,
        student_hash: str,
        task_id: str
    ) -> List[BehavioralPattern]:
        """
        Detecta TODOS os padrões para um estudante/tarefa.
        
        Args:
            student_hash: Hash do estudante
            task_id: ID da tarefa
            
        Returns:
            Lista de BehavioralPattern detectados
        """
        logger.info(
            "[BehavioralPatternDetector.detect_all_patterns] - detecting_all",
            student_hash=student_hash[:8],
            task_id=task_id
        )
        
        patterns = []
        
        # Testa cada detector
        detectors = [
            self.detect_cramming,
            self.detect_trial_and_error,
            self.detect_procrastination,
            self.detect_code_thrashing,
        ]
        
        for detector in detectors:
            try:
                pattern = detector(student_hash, task_id)
                if pattern:
                    patterns.append(pattern)
            except Exception as e:
                logger.error(
                    "[BehavioralPatternDetector.detect_all_patterns] - detector_failed",
                    detector=detector.__name__,
                    error=str(e)
                )
        
        logger.info(
            "[BehavioralPatternDetector.detect_all_patterns] - detection_completed",
            student_hash=student_hash[:8],
            task_id=task_id,
            patterns_found=len(patterns)
        )
        
        return patterns
    
    def batch_detect_patterns(
        self,
        student_hashes: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, List[BehavioralPattern]]]:
        """
        Detecta padrões para múltiplos estudantes em batch.
        
        Args:
            student_hashes: Lista de hashes (se None, processa todos)
            
        Returns:
            Dicionário {student_hash: {task_id: [patterns]}}
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Busca combinações (student, task)
            if student_hashes:
                placeholders = ','.join('?' * len(student_hashes))
                cursor.execute(f"""
                    SELECT DISTINCT student_hash, task_id
                    FROM events
                    WHERE student_hash IN ({placeholders})
                """, student_hashes)
            else:
                cursor.execute("""
                    SELECT DISTINCT student_hash, task_id
                    FROM events
                """)
            
            combinations = cursor.fetchall()
            
            logger.info(
                "[BehavioralPatternDetector.batch_detect_patterns] - batch_starting",
                combinations=len(combinations)
            )
            
            results = {}
            
            for student_hash, task_id in combinations:
                patterns = self.detect_all_patterns(student_hash, task_id)
                
                if student_hash not in results:
                    results[student_hash] = {}
                
                results[student_hash][task_id] = patterns
            
            # Estatísticas
            total_patterns = sum(
                len(patterns)
                for student_data in results.values()
                for patterns in student_data.values()
            )
            
            logger.info(
                "[BehavioralPatternDetector.batch_detect_patterns] - batch_completed",
                students=len(results),
                combinations=len(combinations),
                total_patterns=total_patterns
            )
            
            return results
            
        finally:
            conn.close()
    
    def save_patterns_to_db(self, patterns: List[BehavioralPattern]) -> int:
        """
        Salva padrões detectados no banco de dados.
        
        Args:
            patterns: Lista de padrões a salvar
            
        Returns:
            Número de padrões salvos
        """
        import json
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            saved = 0
            
            for pattern in patterns:
                # Gera case_id do estudante/tarefa
                cursor.execute("""
                    SELECT case_id FROM events
                    WHERE student_hash = ? AND task_id = ?
                    LIMIT 1
                """, (pattern.student_hash, pattern.task_id))
                
                result = cursor.fetchone()
                if not result:
                    logger.warning(
                        "[BehavioralPatternDetector.save_patterns_to_db] - case_id_not_found",
                        student_hash=pattern.student_hash[:8],
                        task_id=pattern.task_id
                    )
                    continue
                
                case_id = result[0]
                
                # Insere padrão
                cursor.execute("""
                    INSERT INTO behavioral_patterns (
                        id, case_id, student_hash, task_id,
                        pattern_type, confidence, evidence, detected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    f"pattern_{pattern.student_hash[:8]}_{pattern.task_id}_{pattern.pattern_type}",
                    case_id,
                    pattern.student_hash,
                    pattern.task_id,
                    pattern.pattern_type,
                    pattern.confidence,
                    json.dumps(pattern.evidence),
                    pattern.detected_at.isoformat() if pattern.detected_at else datetime.now().isoformat()
                ))
                
                saved += 1
            
            conn.commit()
            
            logger.info(
                "[BehavioralPatternDetector.save_patterns_to_db] - patterns_saved",
                saved=saved,
                total=len(patterns)
            )
            
            return saved
            
        finally:
            conn.close()
