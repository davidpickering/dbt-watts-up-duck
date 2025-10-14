# dbt-watts-up-duck
Project demonstrating dbt and DuckDB

<img width="761" height="330" alt="dbt_plus_duckdb" src="https://github.com/user-attachments/assets/97dc460f-a74a-4268-8a40-a276a13cc02a" />


## Setup

### Repo Setup

1. Clone to repo to your local machine
2. Create a virtual environment called .venv
3. Install the dependencies `python -m pip install -r requirements.txt`
4. Activate the virtual environment `source .venv/bin/activate` -or- `source .venv/Scripts/activate`
5. Verify dbt is installed and available`dbt --version`
6. Setup dbt profiles using `dbt init`
  - Which database would you like to use? `[1] duckdb`
  - Verify dbt setup is complete by running `dbt debug`
7. When running `dbt debug` it will state `Using profiles dir at...` Go to that location and edit the profiles.yml file to include the following:
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
8. Install dbt dependencies using `dbt deps`
9. Accomplish project-specific setup from the root directory:
  - 9.1 run `python duckdb_setup/1-setup_duckdb.py ` - This will setup DuckDB with the proper database and schema names
  - 9.2 run `python duckdb_setup/2-kaggle.py` - This will download the datasets from Kaggle and copy them to the project_data directory
  - 9.3 run `python duckdb_setup/3-load_raw_data.py` - This will load the raw data into the DuckDB database
  - or - For your convenience, there is a setup.sh script that will run all of the above steps run `./duckdb_setup/setup.sh`
10. Verify successful setup of DuckDB, and loading of raw data to landing zone by running `dbt build`. You should see similar output to the following:
```
$ dbt build
16:59:08  Running with dbt=1.10.13
16:59:08  Registered adapter: duckdb=1.9.6
16:59:09  Unable to do partial parsing because saved manifest not found. Starting full parse.
16:59:11  Found 1 model, 1 source, 716 macros
16:59:11
16:59:11  Concurrency: 1 threads (target='dev')
16:59:11
16:59:12  1 of 1 START sql view model main.stg_paren__charge_points ...................... [RUN]
16:59:12  1 of 1 OK created sql view model main.stg_paren__charge_points ................. [OK in 0.10s]
16:59:12  
16:59:12  Finished running 1 view model in 0 hours 0 minutes and 0.72 seconds (0.72s).
16:59:12  
16:59:12  Completed successfully
16:59:12
16:59:12  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

## Project Life Cycle

### Load New Data

1. Land the data in project_data/ or a suitable outside storage location

2. Modify the 3-load_raw_data.py script to load the data into DuckDB

3. Run `./duckdb_setup/setup.sh` to run the setup scripts

4. Add the source to the _sources.yml file

5. Create Staging Model

6. Document the model in the appropriate _models.yml file
