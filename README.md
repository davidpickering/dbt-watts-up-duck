# dbt-watts-up-duck
Project demonstrating dbt and DuckDB

## Setup

1. python setup_duckdb.py 

2. 2-kaggle.py

3. dbt profiles

```
watts_up_duck:
  outputs:
    dev:
      type: duckdb
      path: development.duckdb
      threads: 4
      attach:
        - path: raw.db
          alias: raw
  target: dev
```

4. Run kaggle.py

## Load New Data

1. Land the data in project_data/ or a suitable outside storage location

2. Modify the 3-load_raw_data.py script to load the data into DuckDB, and run

3. Add the source to the _sources.yml file

4. Create Staging Model

5. Document the model in the appropriate _models.yml file