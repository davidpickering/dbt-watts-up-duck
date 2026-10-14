#!/bin/bash

python duckdb_setup/1-setup_duckdb.py
python duckdb_setup/2-kaggle.py
python duckdb_setup/3-cle_open_data.py
python duckdb_setup/3.5-google-sheets.py
python duckdb_setup/4-load_raw_data.py
python duckdb_setup/5-shapes.py
