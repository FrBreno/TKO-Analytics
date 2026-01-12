"""
Testes de integração para os dados do TKO.

Cria estrutura mínima de turma para testes.
"""

import pytest


@pytest.fixture
def minimal_classroom(tmp_path):
    """
    Cria a estrutura mínima de turma (esperada) para testes.
    
    Estrutura:
        tmp_path/
        └── turma_test/
            └── bloco-a-submissions/
                ├── bloco-a-student1/
                │   └── poo/
                │       └── .tko/
                │           ├── log/
                │           │   └── 2025-01-10.log
                │           ├── repository.yaml
                │           └── track/
                │               └── toalha/
                │                   ├── draft.py.json
                │                   └── track.csv
                ├── bloco-a-student2/
                │   └── myrep/
                │       └── .tko/
                │           ├── log/
                │           │   └── 2025-01-11.log
                │           └── repository.yaml
                └── bloco-a-student3/
                    └── README.md  (sem .tko/)
    """
    # Criar diretório da turma
    turma = tmp_path / "turma_test"
    turma.mkdir()
    
    # Criar diretório do bloco
    bloco_a = turma / "bloco-a-submissions"
    bloco_a.mkdir()
    
    # Subdiretório poo/ com dados completos
    student1 = bloco_a / "bloco-a-student1"
    student1_poo = student1 / "poo"
    student1_tko = student1_poo / ".tko"
    student1_log = student1_tko / "log"
    student1_track = student1_tko / "track" / "toalha"
    
    student1_log.mkdir(parents=True)
    student1_track.mkdir(parents=True)
    
    # Criar arquivo de log genérico
    log_content = """
        2025-01-10 10:00:00, MOVE, v:1, k:toalha, mode:DOWN
        2025-01-10 10:00:05, MOVE, v:1, k:toalha, mode:PICK
        2025-01-10 10:05:30, EXEC, v:1, k:toalha, mode:LOCK, rate:100, size:25
        2025-01-10 10:30:00, SELF, v:1, k:toalha, rate:100, human:nao, iagen:copilot, guide:readme, other:google, alone:8, study:45
    """
    (student1_log / "2025-01-10.log").write_text(log_content)
    
    # Criar repository.yaml genérico
    repo_yaml = """
        version: '0.2'
        tasks:
        toalha: '{rate:100, human:nao, iagen:copilot, guide:readme, other:google, alone:8, study:45}'
        lang: py    
    """
    (student1_tko / "repository.yaml").write_text(repo_yaml)
    
    # Criar dados genéricos de rastreamento
    draft_json = """
        {
            "content": "def toalha():\\n    return 42\\n",
            "language": "python"
        }
    """
    (student1_track / "draft.py.json").write_text(draft_json)
    
    track_csv = """
        1704884400
        1704884700
        1704885000
    """
    (student1_track / "track.csv").write_text(track_csv)

    # Subdiretório myrep/ (variação)
    student2 = bloco_a / "bloco-a-student2"
    student2_myrep = student2 / "myrep"
    student2_tko = student2_myrep / ".tko"
    student2_log = student2_tko / "log"
    
    student2_log.mkdir(parents=True)
    
    # Criar arquivo de log com eventos diferentes
    log_content2 = """
        2025-01-11 14:00:00, MOVE, v:1, k:animal, mode:DOWN
        2025-01-11 14:00:10, MOVE, v:1, k:animal, mode:PICK
        2025-01-11 14:30:00, EXEC, v:1, k:animal, mode:LOCK, rate:80, size:42
    """
    (student2_log / "2025-01-11.log").write_text(log_content2)
    
    # Criar repository.yaml
    repo_yaml2 = """
        version: '0.2'
        tasks:
        animal: '{rate:80, guide:sena_e_readme, alone:10, study:60}'
        lang: py
    """
    (student2_tko / "repository.yaml").write_text(repo_yaml2)
    
    # Sem .tko/ (caso de exceção)
    student3 = bloco_a / "bloco-a-student3"
    student3.mkdir()
    (student3 / "README.md").write_text("# Estudante 3 - Sem dados TKO")
    
    # Retorna diretório da turma
    return turma


@pytest.fixture
def minimal_classroom_root(tmp_path):
    """Igual a minimal_classroom mas retorna o caminho raiz."""
    
    # Criar diretório da turma
    turma = tmp_path / "turma_test"
    turma.mkdir()
    
    # Criar diretório do bloco
    bloco_a = turma / "bloco-a-submissions"
    bloco_a.mkdir()
    
    # Subdiretório poo/ com dados completos
    student1 = bloco_a / "bloco-a-student1"
    student1_poo = student1 / "poo"
    student1_tko = student1_poo / ".tko"
    student1_log = student1_tko / "log"
    student1_track = student1_tko / "track" / "toalha"
    
    student1_log.mkdir(parents=True)
    student1_track.mkdir(parents=True)
    
    # Criar arquivo de log
    log_content = """
        2025-01-10 10:00:00, MOVE, v:1, k:toalha, mode:DOWN
        2025-01-10 10:00:05, MOVE, v:1, k:toalha, mode:PICK
        2025-01-10 10:05:30, EXEC, v:1, k:toalha, mode:LOCK, rate:100, size:25
        2025-01-10 10:30:00, SELF, v:1, k:toalha, rate:100, human:nao, iagen:copilot, guide:readme, other:google, alone:8, study:45
    """
    (student1_log / "2025-01-10.log").write_text(log_content)
    
    # Criar repository.yaml
    repo_yaml = """
        version: '0.2'
        tasks:
        toalha: '{rate:100, human:nao, iagen:copilot, guide:readme, other:google, alone:8, study:45}'
        lang: py
    """
    (student1_tko / "repository.yaml").write_text(repo_yaml)
    
    # Criar dados de rastreamento
    draft_json = """
        {
            "content": "def toalha():\\n    return 42\\n",
            "language": "python"
        }
    """
    (student1_track / "draft.py.json").write_text(draft_json)
    
    track_csv = """
        1704884400
        1704884700
        1704885000
    """
    (student1_track / "track.csv").write_text(track_csv)
    
    # Subdiretório myrep/ (variação)
    student2 = bloco_a / "bloco-a-student2"
    student2_myrep = student2 / "myrep"
    student2_tko = student2_myrep / ".tko"
    student2_log = student2_tko / "log"
    
    student2_log.mkdir(parents=True)
    
    # Criar arquivo de log com eventos diferentes
    log_content2 = """
        2025-01-11 14:00:00, MOVE, v:1, k:animal, mode:DOWN
        2025-01-11 14:00:10, MOVE, v:1, k:animal, mode:PICK
        2025-01-11 14:30:00, EXEC, v:1, k:animal, mode:LOCK, rate:80, size:42
    """
    (student2_log / "2025-01-11.log").write_text(log_content2)
    
    # Criar repository.yaml
    repo_yaml2 = """
        version: '0.2'
        tasks:
        animal: '{rate:80, guide:sena_e_readme, alone:10, study:60}'
        lang: py
    """
    (student2_tko / "repository.yaml").write_text(repo_yaml2)
    
    # Sem .tko/
    student3 = bloco_a / "bloco-a-student3"
    student3.mkdir()
    (student3 / "README.md").write_text("# Estudante 3 - Sem dados TKO")
    
    return tmp_path


@pytest.fixture
def minimal_classroom_multi_block(tmp_path):
    """
    Criar turma com múltiplos blocos (A e B).
    """
    turma = tmp_path / "turma_multi"
    turma.mkdir()
    
    # Bloco A
    bloco_a = turma / "poo-dd-bloco-a-submissions"
    bloco_a.mkdir()
    
    student_a1 = bloco_a / "poo-dd-bloco-a-alice"
    student_a1_poo = student_a1 / "poo"
    student_a1_tko = student_a1_poo / ".tko"
    student_a1_log = student_a1_tko / "log"
    student_a1_log.mkdir(parents=True)
    
    log_a1 = """  
        2025-01-12 09:00:00, MOVE, v:1, k:tarefa1, mode:PICK
        2025-01-12 09:30:00, EXEC, v:1, k:tarefa1, mode:LOCK, rate:100, size:30
    """
    (student_a1_log / "2025-01-12.log").write_text(log_a1)
    (student_a1_tko / "repository.yaml").write_text("version: '0.2'\ntasks:\n  tarefa1: '{rate:100}'\n")
    
    # Bloco B
    bloco_b = turma / "poo-dd-bloco-b-submissions"
    bloco_b.mkdir()
    
    student_b1 = bloco_b / "poo-dd-bloco-b-bob"
    student_b1_poo = student_b1 / "poo"
    student_b1_tko = student_b1_poo / ".tko"
    student_b1_log = student_b1_tko / "log"
    student_b1_log.mkdir(parents=True)
    
    log_b1 = """
        2025-01-12 11:00:00, MOVE, v:1, k:poo@tarefa2, mode:PICK
        2025-01-12 11:30:00, EXEC, v:1, k:poo@tarefa2, mode:LOCK, rate:90, size:35
    """
    (student_b1_log / "2025-01-12.log").write_text(log_b1)
    (student_b1_tko / "repository.yaml").write_text("version: '0.2'\ntasks:\n  'poo@tarefa2': '{rate:90}'\n")
    
    return tmp_path


@pytest.fixture
def expected_csv_output():
    """Conteúdo CSV esperado para minimal_classroom."""
    return """
        timestamp,student_id,task,event_type,mode,rate,size,human,iagen,guide,other,alone,study
        2025-01-10T10:00:00,HASH1,toalha,MOVE,DOWN,,,,,,,,
        2025-01-10T10:00:05,HASH1,toalha,MOVE,PICK,,,,,,,,
        2025-01-10T10:05:30,HASH1,toalha,EXEC,LOCK,100,25,,,,,,
        2025-01-10T10:30:00,HASH1,toalha,SELF,,100,,none,github_copilot,readme,google,8,45
        2025-01-11T14:00:00,HASH2,animal,MOVE,DOWN,,,,,,,,
        2025-01-11T14:00:10,HASH2,animal,MOVE,PICK,,,,,,,,
        2025-01-11T14:30:00,HASH2,animal,EXEC,LOCK,80,42,,,,,,
    """
