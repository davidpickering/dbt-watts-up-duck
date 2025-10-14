import duckdb

con = duckdb.connect('development.duckdb')

# Attach additional databases
con.execute("ATTACH 'raw.db' AS raw")
# con.execute("ATTACH 'development.db' AS development")
# con.execute("ATTACH 'analytics_staging.db' AS staging")
# con.execute("ATTACH 'analytics_prod.db' AS production")

# List attached databases
databases = con.execute("SELECT * FROM duckdb_databases()").df()
print(f"DuckDB Databases")
print(databases)

# Create schemas for organizing raw data
con.execute("CREATE SCHEMA IF NOT EXISTS raw.paren")
con.execute("CREATE SCHEMA IF NOT EXISTS raw.dcfast")

# Show schemas that exist
schemas = con.execute("SELECT * FROM duckdb_schemas()").df()
print(f"\nSchemas")
print(schemas)

con.close()