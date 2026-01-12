@echo off
echo.
echo ========================================
echo  TKO-Analytics - Iniciando Dashboard
echo ========================================
echo.

REM Verificar se ambiente virtual existe
if not exist .venv (
    echo [ERRO] Ambiente virtual nao encontrado!
    echo Execute primeiro: setup.bat
    pause
    exit /b 1
)

REM Verificar se .env existe
if not exist .env (
    echo [AVISO] Arquivo .env nao encontrado!
    echo Execute primeiro: setup.bat
    pause
    exit /b 1
)

echo [1/2] Ativando ambiente virtual...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERRO] Falha ao ativar ambiente virtual!
    pause
    exit /b 1
)

echo [2/2] Iniciando servidor Flask...
echo.
echo Dashboard disponivel em: http://localhost:5000
echo Pressione Ctrl+C para parar o servidor
echo.

REM Passar argumentos para serve.py (se fornecidos)
python serve.py %*
