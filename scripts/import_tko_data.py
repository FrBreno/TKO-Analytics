#!/usr/bin/env python3
"""
Script CLI para importar dados TKO sem interface web.

Uso:
    python scripts/import_tko_data.py \
        --root-dir "D:\\turmas\\2024_2\\" \
        --output cohort_poo_2024_2
"""
import os
import sys
import argparse
from pathlib import Path

# Adiciona src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.tko_integration.scanner import ClassroomScanner
from src.tko_integration.transformer import TKOTransformer
from src.tko_integration.validator import DataValidator


def main():
    parser = argparse.ArgumentParser(
        description="Importa dados de turmas TKO",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Exemplos:
                # Importar dados locais de turma
                python scripts/import_tko_data.py --root-dir "D:/turmas/2024_2" --output cohort_2024_2
                
                # Especificar salt customizado para pseudonimização de IDs de estudantes
                python scripts/import_tko_data.py --root-dir "D:/turmas" --output minha_cohort --salt meu-salt-secreto
        """
    )
    
    parser.add_argument(
        "--root-dir",
        required=True,
        help="Diretório raiz contendo repositórios de turmas"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Nome da cohort de saída (ex: cohort_poo_2024_2)"
    )
    parser.add_argument(
        "--salt",
        default=os.getenv('STUDENT_ID_SALT', 'default-salt-change-me'),
        help="Salt para pseudonimização de IDs de estudantes (padrão: env STUDENT_ID_SALT ou 'default-salt-change-me')"
    )
    parser.add_argument(
        "--include-tracking",
        action='store_true',
        help="Incluir dados de rastreamento de código (draft.py.json) como eventos CODE_SNAPSHOT"
    )
    parser.add_argument(
        "--output-dir",
        default="tests/real_data",
        help="Diretório de saída para arquivos CSV e DB (padrão: tests/real_data)"
    )
    
    args = parser.parse_args()
    
    # Validar diretório raiz
    root_path = Path(args.root_dir)
    if not root_path.exists():
        print(f"Erro: Diretório não encontrado: {root_path}")
        return 1
    
    print("=" * 70)
    print("IMPORTAÇÃO DE DADOS TKO")
    print("=" * 70)
    print()
    
    # Fase de scan
    print(f"Escaneando {root_path}...")
    scanner = ClassroomScanner()
    scan = scanner.scan_directory(root_path)
    
    print(f"- Encontradas {len(scan.turmas)} turmas")
    print(f"- Total de estudantes: {scan.total_students}")
    print(f"- Repositórios válidos: {scan.valid_repos}/{scan.total_repos}")
    
    if scan.total_repos > 0:
        success_rate = scan.valid_repos / scan.total_repos
        print(f"- Taxa de sucesso: {success_rate:.1%}")
    
    print()
    
    # Mostra avisos se houver
    if scan.warnings:
        print(f"{len(scan.warnings)} avisos:")
        # Agrupa avisos por tipo
        missing_tko = [w for w in scan.warnings if "No .tko/" in w]
        unusual_subdir = [w for w in scan.warnings if "Unusual subdirectory" in w]
        multiple_tko = [w for w in scan.warnings if "Multiple .tko/" in w]
        root_tko = [w for w in scan.warnings if "repository root" in w]
        
        if missing_tko:
            print(f"   - Faltando .tko/: {len(missing_tko)} estudantes")
        if unusual_subdir:
            print(f"   - Nome de subdiretório incomum: {len(unusual_subdir)} estudantes")
        if multiple_tko:
            print(f"   - Múltiplos diretórios .tko/: {len(multiple_tko)} estudantes")
        if root_tko:
            print(f"   - .tko/ na raiz do repositório: {len(root_tko)} estudantes")
        
        print()
        print("   Execute com --verbose para ver todos os avisos")
        print()
    
    # Fase de transformação
    output_dir = Path(args.output_dir) / args.output
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "events.csv"
    
    print(f"Transformando para CSV...")
    transformer = TKOTransformer(args.salt)
    total_events = transformer.transform_scan_to_csv(
        scan, 
        csv_path,
        include_tracking=args.include_tracking
    )
    
    print(f"- {total_events} eventos escritos em {csv_path}")
    print()
    
    # Passo 3: Gerar relatório de validação
    print(f"Gerando relatório de validação...")
    validator = DataValidator()
    report = validator.generate_report(scan)
    
    report_path = output_dir / "validation_report.txt"
    report_path.write_text(report, encoding='utf-8')
    
    print(f"- Relatório salvo em {report_path}")
    print()
    
    # Resumo
    print("=" * 70)
    print("IMPORTAÇÃO CONCLUÍDA!")
    print("=" * 70)
    print()
    print(f"Diretório de saída: {output_dir}")
    print(f"Arquivo CSV: {csv_path}")
    print(f"Eventos: {total_events}")
    print(f"Estudantes: {scan.valid_repos}")
    print()
    
    if total_events > 0:
        print("Próximos passos:")
        print()
        print("   # Carregar CSV no banco de dados TKO-Analytics")
        print(f"   python -c \"from src.parsers import LogParser; from src.etl import init_database, SQLiteLoader; \\")
        print(f"              p = LogParser(); events = p.parse_file('{csv_path}'); \\")
        print(f"              init_database('{output_dir / 'tko_analytics.db'}'); \\")
        print(f"              loader = SQLiteLoader('{output_dir / 'tko_analytics.db'}'); \\")
        print(f"              loader.load_events(events, '{args.output}', 'aggregated')\"")
        print()
        print("   # Ou usar o dashboard")
        print(f"   python serve.py {output_dir / 'tko_analytics.db'}")
    else:
        print("Nenhum evento encontrado. Verifique se os diretórios .tko/log/ contêm arquivos de log.")
    
    print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nInterrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nErro fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
