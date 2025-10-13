import duckdb
import numpy

result = duckdb.sql("SELECT * FROM 'duckdb_setup/paren_qsr_sample.csv'").df()
print(result)