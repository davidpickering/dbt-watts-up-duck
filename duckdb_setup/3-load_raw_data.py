import duckdb

def count_table_rows(dbname = None, schemaname = None, tablename = None):
    result = con.execute(f"SELECT COUNT(*) as total FROM {dbname}.{schemaname}.{tablename}").fetchone()
    
    return result[0]

def state_table_row_count(row_count=None, dbname = None, schemaname = None, tablename = None):
    row_count = count_table_rows(dbname, schemaname, tablename)
    result = f"{dbname}.{schemaname}.{tablename} has {row_count} rows"

    return result

def show_sample(dbname = None, schemaname = None, tablename = None):
    sample = con.execute(f"SELECT * FROM {dbname}.{schemaname}.{tablename} LIMIT 5").df()

    return sample


# Connect to raw database
con = duckdb.connect('raw.db')


# Paren
##Paren QSR Sample

### Load CSV directly into a table
con.execute("""
    CREATE OR REPLACE TABLE raw.paren.paren_qsr_sample AS
    SELECT * FROM 'project_data/paren_qsr_sample.csv'
""")

### Verify it loaded
result = state_table_row_count(dbname="raw", schemaname="paren", tablename="paren_qsr_sample")
print(result)

### Show sample
result = show_sample(dbname="raw", schemaname="paren", tablename="paren_qsr_sample")
print(f"\nSample data: {result}")

##EV Model Charge Capability

### Load CSV directly into a table
con.execute("""
    CREATE OR REPLACE TABLE raw.paren.ev_models_charge_capability AS
    SELECT * FROM 'project_data/ev_models_charge_capability.csv'
""")

### Verify it loaded
result = state_table_row_count(dbname="raw", schemaname="paren", tablename="ev_models_charge_capability")
print(result)

### Show sample
result = show_sample(dbname="raw", schemaname="paren", tablename="ev_models_charge_capability")
print(f"\nSample data: {result}")


# anshtanwar
# datasetengineer
# Tarekmasyro
## charging_stations_2025_world.csv
### Load CSV directly into a table
con.execute("""
    CREATE OR REPLACE TABLE raw.dcfast.world_charging_stations AS
    SELECT * FROM 'project_data/tarekmasryo/charging_stations_2025_world.csv'
""")

### Verify it loaded
result = state_table_row_count(dbname="raw", schemaname="dcfast", tablename="world_charging_stations")
print(result)

### Show sample
result = show_sample(dbname="raw", schemaname="dcfast", tablename="world_charging_stations")
print(f"\nSample data: {result}")

## ev_models_2025.csv
### Load CSV directly into a table
con.execute("""
    CREATE OR REPLACE TABLE raw.dcfast.ev_models_2025 AS
    SELECT * FROM 'project_data/tarekmasryo/ev_models_2025.csv'
""")

### Verify it loaded
result = state_table_row_count(dbname="raw", schemaname="dcfast", tablename="ev_models_2025")
print(result)

### Show sample
result = show_sample(dbname="raw", schemaname="dcfast", tablename="ev_models_2025")
print(f"\nSample data: {result}")

con.close()
