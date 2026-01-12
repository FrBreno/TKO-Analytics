# TKO-Analytics

Sistema de análise de telemetria educacional do TKO (Test Kit Operations) que transforma logs de atividades de estudantes em insights pedagógicos através de dashboards interativos.

## 📋 Descrição

TKO-Analytics é uma ferramenta para **professores** analisarem o comportamento e desempenho de estudantes em atividades de programação. O sistema:

- Processa logs de telemetria exportados do TKO
- Gera métricas pedagógicas (tempo de trabalho, tentativas até sucesso, padrões comportamentais)
- Apresenta dashboards interativos com visualizações (heatmaps, timelines, estatísticas)
- Executa **localmente** (sem necessidade de servidor ou internet)
- Utiliza banco de dados SQLite (portável e simples)
- Pseudonimiza dados de estudantes para privacidade

## 🚀 Setup e Execução

### Pré-requisitos

- **Python 3.12 ou superior**
- Sistema operacional: Windows, Linux ou macOS

### Passo a Passo Completo

#### 1. Clonar o Repositório

```bash
git clone <url-do-repositorio>
cd TKO-Analytics
```

#### 2. Executar Setup Automatizado

**Windows:**
```bash
setup.bat
```

**Linux/Mac:**
```bash
chmod +x setup.sh
./setup.sh
```

**O que o setup faz:**
- Verifica instalação do Python
- Cria ambiente virtual (`.venv`)
- Instala todas as dependências automaticamente
- Gera arquivo `.env` com configurações de segurança
- **Tempo estimado:** 2-5 minutos

#### 3. Executar o Dashboard

**Windows:**
```bash
run.bat
```

**Linux/Mac:**
```bash
chmod +x run.sh
./run.sh
```

#### 4. Acessar no Navegador

Abra seu navegador em:
```
http://localhost:5000
```

### Primeira Importação de Dados

1. Na primeira execução, o sistema mostrará um **wizard de configuração**
2. Clique em **"Começar Importação"**
3. Use o **browser de diretórios** para selecionar a pasta com dados do TKO
4. Configure o **modo de importação**:
   - **Limpa**: Remove dados anteriores (padrão na primeira vez)
   - **Incremental**: Adiciona aos dados existentes
5. Clique em **"Importar Dados"**
6. Aguarde o processamento
7. Dashboard estará disponível com os dados importados


## 🛠️ Comandos Úteis

```bash
# Executar dashboard
run.bat              # Windows
./run.sh             # Linux/Mac

# Executar com banco específico
run.bat caminho/para/banco.db

# Rodar testes (após ativar ambiente virtual)
pytest

# Importar dados via linha de comando
python scripts/import_tko_data.py --root-dir "caminho/para/turma" --output cohort_nome
```

## 🔒 Privacidade

- IDs de estudantes são **pseudonimizados** (SHA-256 com salt)
- Dados processados **localmente** (sem envio para servidores externos)
- Arquivo `.env` contém chaves de segurança (não compartilhar)

## 📞 Suporte

Para problemas durante a instalação ou execução, consulte:
- Logs do sistema na pasta `logs/`
