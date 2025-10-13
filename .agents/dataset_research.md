1. GO Sales Data Warehouse (manz01/dbt-core-sample-duckdb) ⭐ BEST MATCH
This project demonstrates a complete ELT pipeline using dbt with DuckDB, modeling data through distinct layers: raw, staging, detailed (facts & dimensions), and marts. It uses the IBM GO Sales sample dataset and shows proper layered architecture with star schema implementation. GitHub - manz01/dbt-core-sample-duckdb: 🛍️ GO Sales Data Warehouse Project A dbt-powered analytics project built on the IBM GO Sales sample dataset. It models raw, staging, detail - facts & dimension tables (DET), and mart (MRT) layers for retail sales analysis using modern data stack tools like DuckDB and Python.
Key Features:

✅ Full staging → marts layer architecture
✅ Uses DuckDB as the data warehouse
✅ Demonstrates dimensional modeling (star schema)
✅ Includes dbt lineage visualization
✅ Python models alongside SQL

GitHub: https://github.com/manz01/dbt-core-sample-duckdb

2. Modern Data Stack in a Box
This comprehensive tutorial demonstrates deploying DuckDB, Meltano (for ELT), dbt (for transformations), and Apache Superset (for visualization) on a single machine, creating a complete Modern Data Stack locally. Modern Data Stack in a Box with DuckDB – DuckDB
Key Features:

✅ Complete ELT workflow
✅ DuckDB + dbt integration
✅ Apache Superset for BI dashboards
✅ Monte Carlo simulation example

Resource: https://duckdb.org/2022/10/12/modern-data-stack-in-a-box.html

3. DuckDB Serverless Python (hkorhola)
This project demonstrates using dbt to prepare views as parquet files, then querying them through a Streamlit dashboard. It shows how dbt artifacts can be consumed by downstream Python applications for real-time analytics. GitHub - hkorhola/duckdb-serverless-python: A playground for running duckdb as a stateless query engine over a data lake
Key Features:

✅ dbt for transformation
✅ Streamlit dashboard consuming dbt outputs
✅ Real-time querying
✅ Serverless architecture pattern

GitHub: https://github.com/hkorhola/duckdb-serverless-python

4. Geospatial Projects with DuckDB + H3
For H3 and Mapping:
DuckDB has excellent support for geospatial analysis through extensions like H3 and spatial. Projects demonstrate using DuckDB to generate H3 indices and create interactive maps with Folium in Streamlit applications. DuckDBMedium
Examples:

DuckDB Streamlit + Folium: https://duckdb.org/2025/03/28/using-duckdb-in-streamlit.html
Panel + DuckDB + H3 + MapLibre: https://savasalturk.medium.com/taking-geospatial-data-analytics-to-the-next-level-with-panel-duckdb-and-maplibre-9eb9beaee03f
NYC Taxi with H3 + KeplerGL: https://medium.com/@tibeggs/exploring-nyc-taxi-geospatial-data-with-duckdb-h3-and-keplergl-68da908f7397


5. BI Visualization Examples
The duckdb-dataviz-demo repository showcases three different BI tools (Evidence, Streamlit, and Rill) connecting to DuckDB for dashboard creation, demonstrating how to consume dbt outputs in various visualization frameworks. GitHub - mehd-io/duckdb-dataviz-demo: DuckDB with Dashboarding tools demo evidence, streamlit and rill
GitHub: https://github.com/mehd-io/duckdb-dataviz-demo
Another example shows using dbt with DuckDB for transformations and Dash (a Python framework) for building interactive data applications, complete with snapshots for historical analysis. Building a data application with DuckDB | by Fran Lozano | Medium