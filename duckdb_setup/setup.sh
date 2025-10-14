#!/bin/bash

python duckdb_setup/1-setup_duckdb.py
python duckdb_setup/2-kaggle.py
python duckdb_setup/3-load_raw_data.py