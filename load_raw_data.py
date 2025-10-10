import duckdb

# Connect to raw database
con = duckdb.connect('raw.db')


# Load CSV directly into a table
con.execute("""
    CREATE OR REPLACE TABLE paren.paren_qsr_sample AS
    SELECT * FROM 'project_data/paren_qsr_sample.csv'
""")

# Verify it loaded
result = con.execute("SELECT COUNT(*) as total FROM paren.paren_qsr_sample").fetchone()
print(f"Loaded {result[0]} rows into paren.paren_qsr_sample table")

# Show sample
sample = con.execute("SELECT * FROM paren.paren_qsr_sample LIMIT 5").df()
print("\nSample data:")
print(sample)

con.close()
