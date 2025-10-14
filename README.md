# dbt-watts-up-duck
Project demonstrating dbt and DuckDB

<img width="761" height="330" alt="dbt_plus_duckdb" src="https://github.com/user-attachments/assets/97dc460f-a74a-4268-8a40-a276a13cc02a" />


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
