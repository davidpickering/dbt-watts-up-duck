import duckdb
import numpy

result = duckdb.sql("SELECT * FROM 'project_data/paren_qsr_sample.csv'").df()
print(result)