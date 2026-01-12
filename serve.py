"""
Script to serve TKO Analytics Dashboard

Usage:
    python serve.py [database_path]

Example:
    python serve.py src.db
    python serve.py --help
"""
import sys
import argparse
from pathlib import Path
from src.dashboard import run_server
from src.etl.init_db import init_database


def main():
    parser = argparse.ArgumentParser(
        description="TKO Analytics Dashboard Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "database",
        type=str,
        nargs="?",
        default="src.db",
        help="Path to SQLite database (default: src.db)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to bind to (default: 5000)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (auto-reload on code changes)",
    )

    args = parser.parse_args()

    # Verificar se o banco de dados existe, senão criar vazio
    db_path = Path(args.database)
    if not db_path.exists():
        print(f"Database not found: {db_path.absolute()}")
        print(f"Creating empty database...")
        try:
            init_database(str(db_path))
            print(f"Empty database created successfully!")
            print(f"\n💡 Next steps:")
            print(f"   1. Visit http://{args.host}:{args.port}/import")
            print(f"   2. Configure and import TKO data")
            print(f"   3. Process with ETL pipeline to generate analytics\n")
        except Exception as e:
            print(f"Error creating database: {e}")
            sys.exit(1)

    # Start server
    print(f"Starting TKO Analytics Dashboard...")
    print(f"Database: {db_path.absolute()}")
    print(f"Server: http://{args.host}:{args.port}")
    print("\nPress CTRL+C to stop the server.\n")

    try:
        run_server(
            db_path=str(db_path),
            host=args.host,
            port=args.port,
            debug=args.debug,
        )
    except KeyboardInterrupt:
        print("\n\nServer stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
