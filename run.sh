#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo "========================================"
echo "  TKO-Analytics - Iniciando Dashboard"
echo "========================================"
echo ""

# Verificar se ambiente virtual existe
if [ ! -d ".venv" ]; then
    echo -e "${RED}[ERRO]${NC} Ambiente virtual não encontrado!"
    echo "Execute primeiro: ./setup.sh"
    exit 1
fi

# Verificar se .env existe
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}[AVISO]${NC} Arquivo .env não encontrado!"
    echo "Execute primeiro: ./setup.sh"
    exit 1
fi

echo -e "${BLUE}[1/2]${NC} Ativando ambiente virtual..."
source .venv/bin/activate
if [ $? -ne 0 ]; then
    echo -e "${RED}[ERRO]${NC} Falha ao ativar ambiente virtual!"
    exit 1
fi

echo -e "${BLUE}[2/2]${NC} Iniciando servidor Flask..."
echo ""
echo -e "${GREEN}Dashboard disponível em:${NC} http://localhost:5000"
echo -e "Pressione ${YELLOW}Ctrl+C${NC} para parar o servidor"
echo ""

# Passar argumentos para serve.py (se fornecidos)
python serve.py "$@"
