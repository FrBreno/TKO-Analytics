"""
Módulo de validação para verificações de integridade dos dados do TKO.

Gera avisos detalhados para casos excepcionais que requerem intervenção humana.
"""

from typing import List
from .scanner import ClassroomScan, StudentRepo


class DataValidator:
    """
    Valida dados do TKO e gera avisos detalhados.
    """
    
    @staticmethod
    def validate_scan(scan: ClassroomScan) -> List[str]:
        """
        Valida resultados da varredura e retorna lista de avisos.
        
        Args:
            scan: ClassroomScan para validar
            
        Returns:
            Lista de mensagens de aviso
        """
        warnings = []
        
        # Verificar se nenhuma turma foi encontrada
        if not scan.turmas:
            warnings.append(
                f"Nenhuma turma encontrada em {scan.root_path}. "
                "Esperava-se diretórios com padrão '-bloco-'."
            )
            return warnings
        
        # Verificar se não há repositórios válidos
        if scan.valid_repos == 0:
            warnings.append(
                f"Nenhum repositório válido encontrado (0 de {scan.total_repos}). "
                "Verifique se os repositórios dos estudantes contêm diretórios .tko/."
            )
        
        # Verificar taxa de sucesso baixa
        if scan.total_repos > 0:
            success_rate = scan.valid_repos / scan.total_repos
            if success_rate < 0.5:
                warnings.append(
                    f"Taxa de sucesso baixa: {scan.valid_repos}/{scan.total_repos} "
                    f"({success_rate:.1%}) repositórios têm dados .tko/. "
                    "Muitos estudantes podem não ter enviado trabalhos."
                )
        
        # Adicionar avisos no nível da varredura
        warnings.extend(scan.warnings)
        
        return warnings
    
    @staticmethod
    def validate_student(student: StudentRepo) -> List[str]:
        """
        Validar dados individuais do estudante.
        
        Retorna lista de avisos específicos deste estudante.
        """
        warnings = []
        
        if not student.valid:
            warnings.append(
                f"{student.username}: Nenhum diretório .tko/ encontrado em {student.repo_path.name}"
            )
            return warnings
        
        # Verificar diretório de log vazio
        log_dir = student.tko_dir / 'log'
        if not log_dir.exists():
            warnings.append(
                f"{student.username}: Diretório log/ ausente em .tko/"
            )
        elif not list(log_dir.glob('*.log')):
            warnings.append(
                f"{student.username}: Nenhum arquivo de log encontrado em .tko/log/"
            )
        
        # Verificar ausência do repository.yaml
        repo_yaml = student.tko_dir / 'repository.yaml'
        if not repo_yaml.exists():
            warnings.append(
                f"{student.username}: Arquivo repository.yaml ausente em .tko/"
            )
        
        # Adicionar aviso específico do estudante se presente
        if student.warning:
            warnings.append(f"{student.username}: {student.warning}")
        
        return warnings
    
    @staticmethod
    def generate_report(scan: ClassroomScan) -> str:
        """
        Gerar relatório abrangente de validação.
        
        Retorna relatório de texto formatado.
        """
        lines = []
        lines.append("=" * 60)
        lines.append("RELATÓRIO DE VALIDAÇÃO DE DADOS TKO")
        lines.append("=" * 60)
        lines.append("")
        
        lines.append("RESUMO:")
        lines.append(f"  Caminho Raiz: {scan.root_path}")
        lines.append(f"  Turmas: {len(scan.turmas)}")
        lines.append(f"  Total de Estudantes: {scan.total_students}")
        lines.append(f"  Repositórios Válidos: {scan.valid_repos}/{scan.total_repos}")
        
        if scan.total_repos > 0:
            success_rate = scan.valid_repos / scan.total_repos
            lines.append(f"  Taxa de Sucesso: {success_rate:.1%}")
        
        lines.append("")
        
        if scan.warnings:
            lines.append(f"AVISOS ({len(scan.warnings)}):")
            
            missing_tko = [w for w in scan.warnings if "No .tko/" in w]
            unusual_subdir = [w for w in scan.warnings if "Unusual subdirectory" in w]
            multiple_tko = [w for w in scan.warnings if "Multiple .tko/" in w]
            root_tko = [w for w in scan.warnings if "repository root" in w]
            other = [w for w in scan.warnings if w not in missing_tko + unusual_subdir + multiple_tko + root_tko]
            
            if missing_tko:
                lines.append(f"  - Diretório .tko/ ausente: {len(missing_tko)} estudantes")
                for w in missing_tko[:3]:  # Mostrar os primeiros 3 avisos apenas
                    lines.append(f"    - {w}")
                if len(missing_tko) > 3:
                    lines.append(f"    ... e mais {len(missing_tko) - 3}")
            
            if unusual_subdir:
                lines.append(f"  - Nome de subdiretório incomum: {len(unusual_subdir)} estudantes")
                for w in unusual_subdir[:3]:
                    lines.append(f"    - {w}")
                if len(unusual_subdir) > 3:
                    lines.append(f"    ... e mais {len(unusual_subdir) - 3}")
            
            if multiple_tko:
                lines.append(f"  - Múltiplos diretórios .tko/: {len(multiple_tko)} estudantes")
                for w in multiple_tko:
                    lines.append(f"    - {w}")
            
            if root_tko:
                lines.append(f"  - .tko/ na raiz do repositório: {len(root_tko)} estudantes")
                for w in root_tko[:3]:
                    lines.append(f"    - {w}")
                if len(root_tko) > 3:
                    lines.append(f"    ... e mais {len(root_tko) - 3}")
            
            if other:
                lines.append(f"  - Outros avisos: {len(other)}")
                for w in other:
                    lines.append(f"    - {w}")
        else:
            lines.append("AVISOS: Nenhum")
        
        lines.append("")
        
        lines.append("DETALHAMENTO:")
        for turma in scan.turmas:
            lines.append(f"  {turma.name}:")
            for block in turma.blocks:
                valid = sum(1 for s in block.students if s.valid)
                total = len(block.students)
                lines.append(f"    {block.name}: {valid}/{total} repositórios válidos")
        
        lines.append("")
        lines.append("=" * 60)
        
        return "\n".join(lines)
