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

    # Check if database exists
    db_path = Path(args.database)
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path.absolute()}")
        print("\nPlease run the ETL pipeline first to create the database.")
        print("Example:")
        print("  python tests/demo_files/create_demo_db.py")
        print("  python tests/demo_files/demo_pipeline.py")
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
