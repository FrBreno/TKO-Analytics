"""
Módulo de varredura para descobrir a estrutura de salas de aula TKO.

Navega pela estrutura hierárquica de diretórios para identificar:
- Turmas (coortes de sala de aula)
- Blocos (grupos de submissões, ex: bloco-a, bloco-b)
- Repositórios de estudantes
- Subdiretórios TKO (.tko/)

Lida com variações na estrutura de diretórios e gera avisos para casos excepcionais.
"""

import re
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class StudentRepo:
    """Representa o repositório de um estudante com dados TKO."""
    username: str
    repo_path: Path
    tko_subdir: Optional[Path] = None
    tko_dir: Optional[Path] = None
    block: str = ""
    valid: bool = False
    warning: Optional[str] = None


@dataclass
class Block:
    """Representa um bloco de submissões de estudantes (ex: bloco-a)."""
    name: str
    path: Path
    students: List[StudentRepo] = field(default_factory=list)


@dataclass
class Turma:
    """Representa uma turma."""
    name: str
    path: Path
    blocks: List[Block] = field(default_factory=list)


@dataclass
class ClassroomScan:
    """Resultados da varredura de uma estrutura de diretórios de sala de aula."""
    root_path: Path
    turmas: List[Turma] = field(default_factory=list)
    total_students: int = 0
    total_repos: int = 0
    valid_repos: int = 0
    warnings: List[str] = field(default_factory=list)


class ClassroomScanner:
    """
    Varre a estrutura de diretórios para descobrir dados de salas de aula TKO.
    
    Uso:
        scanner = ClassroomScanner()
        scan = scanner.scan_directory(Path("D:/turmas/2024_2/"))
        print(f"Encontrados {scan.valid_repos} repositórios válidos de {scan.total_repos}")
    """
    
    # Nomes conhecidos de subdiretórios TKO (em ordem de prioridade)
    KNOWN_SUBDIRS = ['myrep', 'repo', 'poo', 'fup', 'ed', 'arcade']
    
    # Padrão para identificar diretórios de bloco
    BLOCK_PATTERN = re.compile(r'.*bloco-([a-z])-submissions?', re.IGNORECASE)
    
    def __init__(self):
        self.warnings = []
    
    def scan_directory(self, root_path: Path) -> ClassroomScan:
        """
        Varre o diretório raiz para descobrir a estrutura da sala de aula.
        
        Args:
            root_path: Diretório raiz contendo turmas
            
        Returns:
            ClassroomScan com estrutura descoberta e avisos
        """
        self.warnings = []
        scan = ClassroomScan(root_path=root_path)
        
        if not root_path.exists():
            self.warnings.append(f"Caminho raiz não existe: {root_path}")
            scan.warnings = self.warnings
            return scan
        
        # Procura por diretórios de turma
        turmas = self._find_turmas(root_path)
        
        for turma in turmas:
            # Encontra blocos dentro da turma
            blocks = self._find_blocks(turma.path)
            
            for block in blocks:
                # Encontra repositórios de estudantes dentro do bloco
                students = self._find_students(block.path, block.name)
                block.students = students
                
                scan.total_students += len(students)
                scan.total_repos += len(students)
                scan.valid_repos += sum(1 for s in students if s.valid)
            
            turma.blocks = blocks
            scan.turmas.append(turma)
        
        scan.warnings = self.warnings
        return scan
    
    def _find_turmas(self, root_path: Path) -> List[Turma]:
        """Encontra todos os diretórios de turma no caminho raiz."""
        turmas = []
        
        # Verifica se o diretório raiz é em si uma turma (contém submissões de bloco)
        if self._is_turma_dir(root_path):
            turmas.append(Turma(name=root_path.name, path=root_path))
            return turmas
        
        # Caso contrário, varre os subdiretórios
        for item in root_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                if self._is_turma_dir(item):
                    turmas.append(Turma(name=item.name, path=item))
        
        if not turmas:
            self.warnings.append(f"Nenhum diretório de turma encontrado em {root_path}")
        
        return turmas
    
    def _is_turma_dir(self, path: Path) -> bool:
        """Verifica se o diretório contém submissões de bloco."""
        for item in path.iterdir():
            if item.is_dir() and self.BLOCK_PATTERN.match(item.name):
                return True
        return False
    
    def _find_blocks(self, turma_path: Path) -> List[Block]:
        """Encontra todos os diretórios de bloco na turma."""
        blocks = []
        
        for item in turma_path.iterdir():
            if item.is_dir():
                match = self.BLOCK_PATTERN.match(item.name)
                if match:
                    block_letter = match.group(1).upper()
                    blocks.append(Block(name=f"Bloco {block_letter}", path=item))
        
        return blocks
    
    def _find_students(self, block_path: Path, block_name: str) -> List[StudentRepo]:
        """Encontra todos os repositórios de estudantes no bloco."""
        students = []
        
        for item in block_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Extrai o nome de usuário do nome do repositório
                username = self._extract_username(item.name)
                
                student = StudentRepo(
                    username=username,
                    repo_path=item,
                    block=block_name
                )
                
                # Tenta encontrar o subdiretório TKO
                self._find_tko_subdir(student)
                
                students.append(student)
        
        return students
    
    def _extract_username(self, repo_name: str) -> str:
        """
        Extrai o nome de usuário do nome do repositório.
        
        Exemplos:
            poo-dd-cd-bloco-a-F0NSII -> F0NSII
            bloco-b-student123 -> student123
        """
        # Tenta encontrar o nome de usuário após o último hífen
        parts = repo_name.split('-')
        if len(parts) >= 2:
            return parts[-1]
        return repo_name
    
    def _find_tko_subdir(self, student: StudentRepo) -> None:
        """
        Encontra o subdiretório TKO dentro do repositório do estudante.
        
        Atualiza o objeto student com tko_subdir, tko_dir, valid e warning.
        """
        repo_path = student.repo_path
        
        # Verifica nomes de subdiretórios conhecidos
        for subdir_name in self.KNOWN_SUBDIRS:
            candidate = repo_path / subdir_name
            if candidate.is_dir() and (candidate / '.tko').exists():
                student.tko_subdir = candidate
                student.tko_dir = candidate / '.tko'
                student.valid = True
                return
        
        # Procura por qualquer diretório contendo .tko/
        subdirs_with_tko = []
        for item in repo_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                if (item / '.tko').exists():
                    subdirs_with_tko.append(item)
        
        if len(subdirs_with_tko) == 1:
            student.tko_subdir = subdirs_with_tko[0]
            student.tko_dir = subdirs_with_tko[0] / '.tko'
            student.valid = True
            student.warning = f"Nome de subdiretório incomum: {subdirs_with_tko[0].name}"
            self.warnings.append(f"{student.username}: {student.warning}")
            return
        
        if len(subdirs_with_tko) > 1:
            # Múltiplos subdiretórios com .tko/ - usa o primeiro mas avisa
            student.tko_subdir = subdirs_with_tko[0]
            student.tko_dir = subdirs_with_tko[0] / '.tko'
            student.valid = True
            subdir_names = [s.name for s in subdirs_with_tko]
            student.warning = f"Múltiplos diretórios .tko/ encontrados: {subdir_names}"
            self.warnings.append(f"{student.username}: {student.warning}")
            return
        
        # Verifica se .tko/ está na raiz do repositório
        if (repo_path / '.tko').exists():
            student.tko_subdir = repo_path
            student.tko_dir = repo_path / '.tko'
            student.valid = True
            student.warning = ".tko/ encontrado na raiz do repositório (sem subdiretório)"
            self.warnings.append(f"{student.username}: {student.warning}")
            return
        
        # Nenhum .tko/ encontrado
        student.valid = False
        student.warning = "Nenhum diretório .tko/ encontrado"
        self.warnings.append(f"{student.username}: {student.warning}")
