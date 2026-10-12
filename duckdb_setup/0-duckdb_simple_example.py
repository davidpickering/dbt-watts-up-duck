from pathlib import Path
import duckdb

# Resolve the CSV relative to this file so the script works from any working directory
csv_path = Path(__file__).resolve().parent.parent / "project_data" / "cle_open_data_water_meters.csv"

# data = duckdb.read_csv(str(csv_path), quotechar="\"", dtype={"ZIP": "VARCHAR"})
data = duckdb.read_csv(str(csv_path), quotechar="\"", sample_size=-1)

print("Show the first 5 rows of the dataset")
data.limit(5).show()

print("How many water meters are in the dataset?")
duckdb.sql("SELECT COUNT(*) as water_meters FROM data").show()


print("How many water meters are in each zip code?")
duckdb.sql("SELECT zip, COUNT(*) as water_meters FROM data GROUP BY 1 order by 2 desc").show()


print("What is the 'main street' of each city? (the street with the most water meters)")
duckdb.sql("""
    SELECT
        city,
        str_name || ' ' || str_type AS main_street,
        COUNT(*) AS meters
    FROM data
    WHERE str_name IS NOT NULL
    GROUP BY 1, 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY meters DESC) = 1
    ORDER BY meters DESC
""").show()

print("Show a summary of the dataset")
duckdb.sql("SUMMARIZE data").show()

