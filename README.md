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