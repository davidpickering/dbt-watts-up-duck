import duckdb

con = duckdb.connect('main_database.db')

# Attach additional databases
con.execute("ATTACH 'raw.db' AS raw")
con.execute("ATTACH 'development.db' AS development")
con.execute("ATTACH 'analytics_staging.db' AS staging")
con.execute("ATTACH 'analytics_prod.db' AS production")


# Create schema for organizing raw data
con.execute("CREATE SCHEMA IF NOT EXISTS raw.paren")

# Create tables in attached databases if they don't exist
con.execute("CREATE TABLE IF NOT EXISTS development.metrics (id INTEGER, value FLOAT)")

# Query across databases
result = con.execute("""
    SELECT * 
    FROM development.metrics t1
""").df()

# List attached databases
databases = con.execute("SELECT * FROM duckdb_databases()").df()
print(f"DuckDB Databases")
print(databases)

# Show schemas that exist
schemas = con.execute("SELECT * FROM duckdb_schemas()").df()
print(f"\nSchemas")
print(schemas)

con.close()