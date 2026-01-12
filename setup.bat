@echo off
REM =========================================
REM TKO-Analytics - Setup Automatizado (Windows)
REM =========================================

echo.
echo ========================================
echo  TKO-Analytics - Setup Automatizado
echo ========================================
echo.

REM Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado! Instale Python 3.12 ou superior.
    echo Download: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/4] Verificando versao do Python...
python --version

REM Criar ambiente virtual
echo.
echo [2/4] Criando ambiente virtual (.venv)...
if exist .venv (
    echo [AVISO] Ambiente virtual ja existe. Pulando criacao.
) else (
    python -m venv .venv
    if errorlevel 1 (
        echo [ERRO] Falha ao criar ambiente virtual!
        pause
        exit /b 1
    )
    echo [OK] Ambiente virtual criado com sucesso!
)

REM Ativar ambiente virtual
echo.
echo [3/4] Ativando ambiente virtual e instalando dependencias...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERRO] Falha ao ativar ambiente virtual!
    pause
    exit /b 1
)

REM Atualizar pip
python -m pip install --upgrade pip --quiet

REM Instalar dependências
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [ERRO] Falha ao instalar dependencias!
    pause
    exit /b 1
)

REM Instalar pacote em modo desenvolvimento
pip install -e . --quiet
if errorlevel 1 (
    echo [AVISO] Falha ao instalar pacote em modo desenvolvimento (pode ser ignorado)
)

echo [OK] Dependencias instaladas com sucesso!

REM Criar arquivo .env
echo.
echo [4/4] Configurando ambiente...
if exist .env (
    echo [AVISO] Arquivo .env ja existe. Criando backup como .env.backup
    copy .env .env.backup >nul
)

REM Gerar secrets aleatórios usando Python
python -c "import secrets; print(f'STUDENT_ID_SALT={secrets.token_urlsafe(32)}\nFLASK_SECRET_KEY={secrets.token_hex(32)}\nENVIRONMENT=production')" > .env

if errorlevel 1 (
    echo [ERRO] Falha ao gerar arquivo .env!
    pause
    exit /b 1
)

echo [OK] Arquivo .env criado com sucesso!

echo ========================================
echo  Setup concluido com sucesso!
echo ========================================
echo.
echo Proximos passos:
echo   1. Executar dashboard: run.bat
echo   2. Acessar: http://localhost:5000
echo.
