# TKO-Analytics 🚀

Uma ferramenta local para que professores analisem o processo de desenvolvimento dos estudantes utilizando a telemetria gerada pelo TKO (Test Kit Operations). 💡

O que faz, de forma prática ✅
- Processa os logs produzidos pelo TKO e gera métricas pedagógicas.
- Apresenta dashboards interativos com mapas de conformidade, timelines de execução, evolução de código e visualizador de diffs. 📊

Por quem e para quê 🎯
- Destinado a docentes e equipes de ensino que querem entender como os alunos trabalham durante exercícios de programação, sem depender exclusivamente de commits do Git.

Principais tecnologias (visão geral) 🧰
- Python 3.12 — aplicação e scripts de análise
- Flask — interface web local (dashboard)
- SQLite — banco portátil para armazenar eventos e métricas
- Plotly / CSS / Bootstrap — visualizações interativas e interface
- PM4Py (opcional) e Graphviz — descoberta de processos e renderização de Petri nets (visualizações) 🖼️

Pré-requisitos rápidos ✅
- Python 3.12 ou superior
- Graphviz (para renderização de modelos) — o instalador do projeto tenta automatizar a instalação, mas verifique se `dot` está disponível no PATH 🖥️

Instalação e execução (passos curtos) 🛠️
1. Clone o repositório e abra o diretório do projeto:
```bash
git clone <url-do-repositorio>
cd TKO-Analytics
```
2. Execute o instalador automático (o script cria um ambiente virtual, instala dependências e cria `.env`):
- Windows (Prompt):
```powershell
.\setup.bat
```
- Linux / macOS (Terminal):
```bash
chmod +x setup.sh
./setup.sh
```
3. Inicie o dashboard:
- Windows:
```powershell
.\run.bat
```
- Linux / macOS:
```bash
./run.sh
```
4. Abra o navegador em `http://localhost:5000` para usar a aplicação.

Primeira importação de dados (fluxo recomendado) 📥
1. Ao abrir o dashboard pela primeira vez, use o assistente (wizard) de importação.
2. Selecione a pasta que contém os dados exportados pelo TKO.
3. Escolha o modo: `Limpa` (apaga dados anteriores) ou `Incremental` (acrescenta aos dados existentes).
4. Aguarde o processamento; quando concluído as visualizações ficarão disponíveis.

Dicas rápidas e solução de problemas 🩺
- Se o Graphviz não estiver instalado, algumas visualizações (Petri nets) não serão renderizadas — o `setup` tenta instalá-lo, mas você pode instalar manualmente:
   - Ubuntu/Debian: `sudo apt install graphviz`
   - Fedora: `sudo dnf install graphviz`
   - macOS (Homebrew): `brew install graphviz`
   - Windows: use `winget install Graphviz.Graphviz` ou instale via instalador em https://graphviz.org/download/
- Se houver problemas de dependência Python, ative o ambiente virtual (`.venv\Scripts\activate` no Windows ou `source .venv/bin/activate` no Linux/macOS) e execute `pip install -r requirements.txt`.

Onde olhar: módulos principais (visão amigável) 🧭
- `src/dashboard/` — aplicação web (rotas, templates e estáticos). É a interface que você usa no navegador.
- `src/etl/` — código responsável por carregar, limpar e inserir eventos no banco de dados.
- `src/process_mining/` — ferramentas que geram modelos e checam conformidade (útil quando fizer análises PM).
- `src/visualizations/` — funções que geram heatmaps, timelines e gráficos usados no dashboard.
- `src/models/` e `src/metrics/` — representação de eventos, métricas calculadas e lógica de agregação.

Privacidade e ética 🔒
- Os dados são processados localmente por padrão; o sistema pseudonimiza identificadores de estudantes para proteção de privacidade.

Ajuda e suporte 📞
- Logs e outputs ficam em `outputs/` e `logs/` (quando aplicável).
- Para problemas de instalação, verifique o conteúdo dos scripts `setup.bat` / `setup.sh` e as instruções acima.

Contribuições e código-fonte 🤝
- O código do projeto está na pasta `src` deste repositório; contribuições são bem-vindas via pull requests.
