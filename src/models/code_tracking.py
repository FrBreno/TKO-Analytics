"""
Modelos Pydantic para rastreamento de código (code snapshots e patches).

Esses modelos representam a evolução do código do estudante ao longo do tempo,
capturando tanto snapshots completos quanto patches incrementais (diffs).
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class CodePatchEvent(BaseModel):
    """
    Representa um patch incremental (diff) de código.
    
    Patches são diferenças entre versões consecutivas do código,
    armazenadas em formato diff unificado.
    """
    timestamp: datetime = Field(description="Timestamp do patch")
    task_id: str = Field(description="ID da tarefa (ex: toalha, calculadora)")
    file_path: str = Field(default="draft.py", description="Caminho do arquivo (draft.py, draft.js, etc)")
    patch_content: str = Field(description="Conteúdo do diff em formato unificado")
    line_count: int = Field(ge=0, description="Número de linhas no código após aplicar o patch")
    student_name: Optional[str] = Field(None, description="Nome do estudante (opcional)")
    
    @field_validator('task_id')
    @classmethod
    def task_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('task_id não pode ser vazio')
        return v.strip()
    
    def is_empty_patch(self) -> bool:
        """Verifica se o patch está vazio (sem mudanças)."""
        return not self.patch_content or self.patch_content.strip() == ""


class FullCodeSnapshot(BaseModel):
    """
    Representa um snapshot completo do código em um ponto no tempo.
    
    Diferente de patches incrementais, contém o código-fonte completo.
    Usado como snapshot base para reconstrução temporal.
    """
    timestamp: datetime = Field(description="Timestamp do snapshot")
    task_id: str = Field(description="ID da tarefa")
    file_path: str = Field(default="draft.py", description="Caminho do arquivo")
    full_code: str = Field(description="Código-fonte completo")
    line_count: int = Field(ge=0, description="Número de linhas no código")
    student_name: Optional[str] = Field(None, description="Nome do estudante")
    
    @field_validator('task_id')
    @classmethod
    def task_id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('task_id não pode ser vazio')
        return v.strip()
    
    def get_lines(self) -> list[str]:
        """Retorna código dividido em linhas."""
        return self.full_code.split('\n') if self.full_code else []


class CodeSnapshotMetadata(BaseModel):
    """
    Metadata estruturado para snapshots de código.
    
    Armazenado como JSON no campo metadata das tabelas code_snapshots e code_patches.
    """
    version: int = Field(default=1, description="Versão do formato")
    encoding: str = Field(default="utf-8", description="Encoding do código")
    language: str = Field(default="python", description="Linguagem do código (python, javascript, etc)")
    is_full_snapshot: bool = Field(description="True se contém código completo, False se é patch")
    patch_sequence: Optional[int] = Field(None, description="Sequência do patch (1, 2, 3...)")
    total_patches: Optional[int] = Field(None, description="Total de patches para esta tarefa")
    
    class Config:
        json_schema_extra = {
            "example": {
                "version": 1,
                "encoding": "utf-8",
                "language": "python",
                "is_full_snapshot": False,
                "patch_sequence": 5,
                "total_patches": 12
            }
        }
