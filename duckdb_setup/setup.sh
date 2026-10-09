#!/bin/bash

python duckdb_setup/1-setup_duckdb.py
python duckdb_setup/2-kaggle.py
python duckdb_setup/3-load_raw_data.py
# 4-reverse_geocode_ohio.py is more of a 1-time script. With improvement we could include it and add additional state's of data at a time.
python duckdb_setup/5-load_shapes.py