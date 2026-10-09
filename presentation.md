# GeoSpatial Analytics

## Goal

Make the case that if someone wants to do some geographic optimization there is a widely supported set of strategies they can use within their database - no map needed.

## Background

This presentation builds upon previous work done to show a dbt project, running against a local DuckDB database. 
- dbt is a SQL compiling and orchestration tool that makes it easy(easier) to for (most) analysts and developers to contribute to data warehousing efforts.
- DuckDB is
- Adds: Geospatial concepts such as shapefiles, polygon geometries, point geometries, within(), intersects() and a few other spatial operations.
- Adds: Folium - a Python wrapper of the [Leaflet](https://leafletjs.com/) mapping library.

### What are Shapefiles?

Shapefiles are a popular geospatial vector data format developed by Esri for storing the location, shape, and attributes of geographic features. A shapefile actually consists of multiple files (typically .shp, .shx, .dbf, and .prj) that work together to represent points, lines, or polygons on a map. They are widely used for representing administrative boundaries (like states, counties, zip codes), transportation networks, water bodies, and other geographic features. Despite being an older format, shapefiles remain one of the most universally supported geospatial formats across GIS software and modern databases.

At their core, shapefiles store geometry as ordered sequences of coordinate pairs (latitude/longitude or x/y coordinates). For polygons, the shape is defined by connecting these coordinates in order - drawing a line from the first coordinate to the second, then to the third, and so on, until finally connecting back to the first coordinate to close the shape. The .shp file contains this geometric data, while the .dbf file stores associated metadata and attributes (like the name of a state or zip code). The .prj file defines the coordinate reference system, and the .shx file acts as an index to quickly locate features within the .shp file.

```
Example: A Simple Polygon Shape in Ohio

Coordinates (Lon, Lat):     Visual Representation:
1. (-83.10, 40.15)
2. (-83.05, 40.18)                2 (-83.05,40.18)
3. (-83.02, 40.12)                *--------*
4. (-83.06, 40.08)               /          \ 3 (-83.02,40.12)
5. (-83.09, 40.10)              /            *
                               /              \
        1 (-83.10,40.15)      *                \
                               \                \
                                \                * 4 (-83.06,40.08)
                                 *--------------*
                              5 (-83.09,40.10)

The shape is drawn by connecting: 1→2→3→4→5→1 (back to start)

Associated metadata in .dbf file:
- NAME: "Sample Region"
- AREA: 42.5
- POPULATION: 15000
- GEOMETRY: POLYGON((-83.10 40.15, -83.05 40.18, -83.02 40.12, -83.06 40.08, -83.09 40.10, -83.10 40.15))
```

See: shapefile_demo.ipynb -> quick_example

### How well supported are Shapefiles?

#### DuckDB

DuckDB supports shapefiles through its spatial extension, which integrates the GDAL (Geospatial Data Abstraction Library) translator. Read more about it in this [DuckDB Post](https://duckdb.org/2023/04/28/spatial). The extension must be explicitly installed and loaded:

```python
con.execute("INSTALL spatial")
con.execute("LOAD spatial")
```

The spatial extension provides a `GEOMETRY` type for storing geospatial data as binary representations of vertices (X/Y coordinate pairs). It supports standard geometry subtypes including POINT, LINESTRING, POLYGON, and their multi-variants (MULTIPOINT, MULTILINESTRING, MULTIPOLYGON).

DuckDB uses the `ST_` prefix for spatial functions (following PostGIS convention) and leverages GEOS and PROJ libraries for spatial calculations. Key functions for working with shapefiles:
- `st_read('path/to/shapefile.shp')` - Table function to import shapefile data directly into DuckDB
- ST_Within - Returns True/False if shape 1 is within shape 2. e.g., is a specific zip code in Ohio?
- st_intersects - Returns True/False if shape 1 intersects shape 2. e.g. does a specific zip code touch other overlap with a specific county?
- ST_Point - Takes a pair of coordinates and creates a geography point
- ... and more. DuckDB provides over [120 spatial functions](https://duckdb.org/docs/stable/core_extensions/spatial/functions)

Learn more: [DuckDB Spatial Documentation](https://duckdb.org/docs/stable/core_extensions/spatial/overview)


#### Cloud Data Warehouses

##### Snowflake
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ✓ Yes (via Python UDF) | Use Snowpark Python UDF with dynamic file access to convert shapefiles to WKB format. Upload .zip to stage, parse into GEOGRAPHY or GEOMETRY based on projection |
| Load Point Geometries | ✓ Yes | Native GEOGRAPHY and GEOMETRY types. Use ST_POINT, ST_GEOGPOINT, or parse from WKT/WKB/GeoJSON |
| ST_WITHIN | ✓ Yes | ST_WITHIN(g1, g2) checks if g1 is fully contained by g2. Supports search optimization |
| ST_INTERSECTS | ✓ Yes | ST_INTERSECTS(g1, g2) returns TRUE if objects share any portion of space |

- [Snowflake Geospatial Docs](https://docs.snowflake.com/en/sql-reference/functions-geospatial) 
- Example [Load Shapefiles Guide](https://medium.com/snowflake/load-shapefiles-into-snowflake-the-easy-way-2af966a17c9a)
- Please note: If you have some familiarity with Snowflake's native file import it is possible to build a lite process that would parse the attributions of a shapefile into a .csv that would be imported into Snowflake.

##### Databricks
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ✓ Yes (via Mosaic) | Use Mosaic library: `spark.read.format("shapefile").load()`. Newer development involves native support in databricks runtime 17.1 and above  |
| Load Point Geometries | ✓ Yes | Native GEOMETRY and GEOMETRY types (Runtime 17.1+). Import from WKT, WKB, GeoJSON, or lat/long |
| ST_WITHIN | ✓ Yes | st_within(geom1, geom2) available in Runtime 17.1+ (Public Preview) |
| ST_INTERSECTS | ✓ Yes | st_intersects(geom1, geom2) available with 80+ spatial SQL functions |

- [Databricks Spatial SQL](https://www.databricks.com/blog/introducing-spatial-sql-databricks-80-functions-high-performance-geospatial-analytics) 
- [Mosaic Vector Readers](https://databrickslabs.github.io/mosaic/api/vector-format-readers.html)

##### AWS Redshift
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ✓ Yes | Direct shapefile loading documented. Supports GEOMETRY and GEOGRAPHY types |
| Load Point Geometries | ✓ Yes | POINT subtype supported. Use ST_Point or ST_GeomFromEWKT |
| ST_WITHIN | ✓ Yes | ST_Within(geom1, geom2) tests if one geometry is within another |
| ST_INTERSECTS | ✓ Yes | ST_Intersects(geom1, geom2) tests if geometries intersect |

[Redshift Geospatial Overview](https://docs.aws.amazon.com/redshift/latest/dg/geospatial-overview.html)

##### BigQuery
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ✗ No (convert first) | Must convert shapefiles to supported format (WKT, GeoJSON, etc.) using external tools before loading |
| Load Point Geometries | ✓ Yes | Use ST_GEOGPOINT(longitude, latitude) to create point geographies from coordinates |
| ST_WITHIN | ✓ Yes | ST_WITHIN optimized for spatial joins with INNER JOIN and CROSS JOIN |
| ST_INTERSECTS | ✓ Yes | ST_INTERSECTS optimized for spatial joins with INNER JOIN and CROSS JOIN |

[BigQuery Geospatial Data](https://cloud.google.com/bigquery/docs/geospatial-data)

#### OLTP Databases

##### PostgreSQL (PostGIS)
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ✓ Yes | PostGIS extension provides shp2pgsql command-line utility (ogr2ogr or shp2pgsql) to load shapefiles directly into PostgreSQL |
| Load Point Geometries | ✓ Yes | Native GEOMETRY and GEOGRAPHY types. Use ST_Point, ST_GeomFromText, or ST_MakePoint |
| ST_Within | ✓ Yes | ST_Within(geom_a, geom_b) returns true if geometry A is completely inside geometry B. Index-aware |
| ST_Intersects | ✓ Yes | ST_Intersects(geom_a, geom_b) returns true if geometries have any point in common. Index-aware |

- [PostGIS](https://postgis.net/workshops/postgis-intro/spatial_relationships.html)
- [Loading Data](https://postgis.net/workshops/postgis-intro/loading_data.html)

##### SQL Server
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ~ Yes (with translator libraries GDAL and OGR ) | No native shapefile import. Requires third-party tools or manual conversion to WKT/WKB format |
| Load Point Geometries | ✓ Yes | Native geometry and geography types. Use geometry::STPointFromText or geography::Point |
| STWithin | ✓ Yes | STWithin() returns 1 if instance is completely within another. Requires matching SRIDs. Spatial index supported |
| STIntersects | ✓ Yes | STIntersects() returns 1 if instances intersect. Available for both geometry and geography types |

- [SQL Server Spatial Data Overview](https://learn.microsoft.com/en-us/sql/relational-databases/spatial/spatial-data-types-overview)
- [3 Ways to Import Shapefiles](https://mapscaping.com/how-to-import-a-shape-file-into-sql-server/)

##### MySQL
| Feature | Support | Context |
|---------|---------|---------|
| Load Shapefiles | ~ Yes (with translator librarie ogr2ogr) | No native import. Requires 3rd party tools. MySQL supports reading WKB format |
| Load Point Geometries | ✓ Yes | Native GEOMETRY, POINT types. Use ST_GeomFromText, ST_PointFromText, or POINT constructor |
| ST_Within | ✓ Yes | ST_Within(geom1, geom2) returns 1 if left geometry is spatially enclosed within right one |
| ST_Intersects | ✓ Yes | ST_Intersects(geom1, geom2) returns 1 if left geometry spatially intersects right one. SPATIAL indexes supported |

- [Importing Shapefiles to MySQL](https://www.igismap.com/insert-shapefile-in-mysql-as-spatial-data/)
- [MySQL Spatial Relation Functions](https://dev.mysql.com/doc/refman/8.0/en/spatial-relation-functions-object-shapes.html) 
- [Working with Geospatial Features](https://planetscale.com/blog/geospatial-features-mysql)

### Where can you get Shapefiles?

#### Census Bureau's TIGERLine Site

This repo has a simple process to download shape files from the [Census Bureau's TIGERLine File FTP](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) site. 
- You can add additional URLs to the script in the `5-shapes.py` file to integrate additional shape files.
- The site offers both a web, and File Transfer Protocol (FTP) interface to access the files. 
- A brief description of the 2024 options are available [here](https://www2.census.gov/geo/tiger/TIGER2024/2024_TL_Shapefiles_File_Name_Definitions.pdf). 
- The most recent 2025 files are available [here](https://www2.census.gov/geo/tiger/TIGER2025/). 

This source is where you can find familiar boundaries such as state, county, zip code (approximated through ZCTA), and Place (cities/towns/villiages). You can also find boundaries of interest in academia such as block group, census tract, congressional district, state senate district, state house district, and school districts. 

#### Municipal GIS sites

Many cities operate municipal GIS portals to provide easy access to some of their geographic data sets. The City of Cleveland operates the [Open Data](https://data.clevelandohio.gov/) page with a dedicated [Locations and Boundaries](https://data.clevelandohio.gov/pages/locations-boundaries) page.
- The [Cleveland Water Service Address Points](https://data.clevelandohio.gov/maps/92a311dcc77446b9bbbaa4403e0e2bdb) data set offers a large number of points that may be useful for exploratory efforts managing GIS point data.

## Simple SQL Demo with Shapefiles

shapefile_demo.ipynb -> ## Simple SQL Demo
- Create Table
- Show Table
- Show data on Map

## No Map Required (But Still Helps)

#### What is a Centroid?
### From the Shape - Extract Centroid
#### Show Shape + Centroid on a Map

shapefile_demo.ipynb -> ### Centroid



### From the Shape - Extract Bounding Box
#### Show Shape + Centroid + Bounding Box on a Map


shapefile_demo.ipynb -> ### Bounding Box


### From the Shape - Extract Lat/Long of Centroid
#### Show Shape + Centroid + Lat/Long Lines on a Map


shapefile_demo.ipynb -> ### Extracting Latitudes and Longitudes


## Practical applications

### Showing Things on Maps - Shapes & Points

Fundamentally, geographic data is trying to provide a shortcut to understand relationships between objects. Many times, these objects have hierarchies (e.g., counties are in states, addresses are in zip codes, etc...). 
- At a surface level, having shapefile data allows you to show that information on a map, and understand basic relationships and distributions that tabular data alone may not reveal.
- Likewise, mapping data alone likely does not unlock all the value of data with a well curated geography dimension.

shapefile_demo.ipynb -> ### Showing Things on Maps - Shapes & Points
- This example; show a map with state boundaries and points on them.

### Frequency Distribution
- Did some of this heavy lifting in our dbt project, [DimGeos](review the models/warehouse/dim_geos.sql) model for the spatial join syntax
- Result is that we can positively identify, which state each point is within - even if we do not have address data.
- If we did have address data, there are plenty of times we want to validate data (is this zip code correct?). Coordiantes+spatial joins provide a path for that work.
shapefile_demo.ipynb -> ### Showing Things on Maps - From Spatial Join
- Example showing data just in a few states, using information from the spatial join
- *Data is filtered using its geographic properties alone*

shapefile_demo.ipynb -> #### Frequency Distribution
- Uses the point data, with spatial join to states, to show how many points are in each state.



Want to make the case that yes, showing on maps is in many ways the ultimate goal. However, once you have this data in your data base you can then introduce a lot more logic and intelligence in how you relate data to each other based on the relationships to the shapes.
(Show charging stations or SNAP retailer point data)

## Extra Content

Please review the shapefile_demo.ipynb notebook for more content and examples
