import duckdb
import shutil
import os

def get_readonly_connection(db_path='development.duckdb', raw_db_path='raw.db'):
    """
    Get a read-only DuckDB connection (avoids write lock conflicts with dbt).

    Args:
        db_path: Path to the main database file (default: development.duckdb)
        raw_db_path: Path to the raw database file (default: raw.db)

    Returns:
        Read-only DuckDB connection with raw.db attached and spatial/h3 extensions loaded
    """
    con = duckdb.connect(db_path, read_only=True)

    # Attach additional databases (read-only)
    con.execute(f"ATTACH IF NOT EXISTS '{raw_db_path}' AS raw (READ_ONLY)")

    # Load extensions
    con.execute("LOAD spatial")
    con.execute("LOAD h3")

    return con

def get_connection(db_path='development.duckdb', raw_db_path='raw.db'):
    """
    Get a DuckDB connection with standard configuration.

    Args:
        db_path: Path to the main database file (default: development.duckdb)
        raw_db_path: Path to the raw database file (default: raw.db)

    Returns:
        DuckDB connection with raw.db attached and spatial/h3 extensions loaded
    """
    con = duckdb.connect(db_path)

    # Attach additional databases
    con.execute(f"ATTACH IF NOT EXISTS '{raw_db_path}' AS raw")

    # Load extensions (already installed via setup script)
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    con.execute("LOAD h3")

    return con
