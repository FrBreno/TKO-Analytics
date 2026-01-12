#!/bin/bash

# =========================================
# TKO-Analytics - Setup Automatizado (Linux/Mac)
# =========================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "========================================"
echo "  TKO-Analytics - Setup Automatizado"
echo "========================================"
echo ""

# Verificar se Python está instalado
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}[ERRO]${NC} Python3 não encontrado! Instale Python 3.12 ou superior."
    echo "Ubuntu/Debian: sudo apt install python3 python3-venv python3-pip"
    echo "macOS: brew install python@3.12"
    exit 1
fi

echo -e "${BLUE}[1/4]${NC} Verificando versão do Python..."
python3 --version

# Criar ambiente virtual
echo ""
echo -e "${BLUE}[2/4]${NC} Criando ambiente virtual (.venv)..."
if [ -d ".venv" ]; then
    echo -e "${YELLOW}[AVISO]${NC} Ambiente virtual já existe. Pulando criação."
else
    python3 -m venv .venv
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERRO]${NC} Falha ao criar ambiente virtual!"
        exit 1
    fi
    echo -e "${GREEN}[OK]${NC} Ambiente virtual criado com sucesso!"
fi

# Ativar ambiente virtual
echo ""
echo -e "${BLUE}[3/4]${NC} Ativando ambiente virtual e instalando dependências..."
source .venv/bin/activate
if [ $? -ne 0 ]; then
    echo -e "${RED}[ERRO]${NC} Falha ao ativar ambiente virtual!"
    exit 1
fi

# Atualizar pip
python -m pip install --upgrade pip --quiet

# Instalar dependências
pip install -r requirements.txt --quiet
if [ $? -ne 0 ]; then
    echo -e "${RED}[ERRO]${NC} Falha ao instalar dependências!"
    exit 1
fi

# Instalar pacote em modo desenvolvimento
pip install -e . --quiet 2>/dev/null || echo -e "${YELLOW}[AVISO]${NC} Falha ao instalar pacote em modo desenvolvimento (pode ser ignorado)"

echo -e "${GREEN}[OK]${NC} Dependências instaladas com sucesso!"

# Criar arquivo .env
echo ""
echo -e "${BLUE}[4/4]${NC} Configurando ambiente..."
if [ -f ".env" ]; then
    echo -e "${YELLOW}[AVISO]${NC} Arquivo .env já existe. Criando backup como .env.backup"
    cp .env .env.backup
fi

# Gerar secrets aleatórios usando Python
python3 -c "import secrets; print(f'STUDENT_ID_SALT={secrets.token_urlsafe(32)}\nFLASK_SECRET_KEY={secrets.token_hex(32)}\nENVIRONMENT=production')" > .env

if [ $? -ne 0 ]; then
    echo -e "${RED}[ERRO]${NC} Falha ao gerar arquivo .env!"
    exit 1
fi

echo -e "${GREEN}[OK]${NC} Arquivo .env criado com sucesso!"

echo ""
echo "========================================"
echo -e "  ${GREEN}Setup concluído com sucesso!${NC}"
echo "========================================"
echo ""
echo "Próximos passos:"
echo "  1. Executar dashboard: ./run.sh"
echo "  2. Acessar: http://localhost:5000"
echo ""
