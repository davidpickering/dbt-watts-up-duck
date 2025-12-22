# dbt-watts-up-duck Presentation

## Background

### What are Shapefiles?

### How well supported are Shapefiles?
- DuckDB

#### Cloud Data Warehouses
- Snowflake
- Databricks
- RedShift
- BigQuery
- Azure Synapse

#### OLTP Databases
- PostGres
- SQLServer/MsSQL
- MySQL
- Oracle DB2
- Hadoop

### Where can you get Shapefiles?

#### Census Bureau's TIGERLine Site

This repo has code and processes to load and prepare

#### Municipal GIS sites

??? City of Cleveland GIS Portal ???

#### ??? Kaggle ???

## Simple SQL Demo with Shapefiles

### Create Table

```sql

```

### Query the Shape
```sql

```

#### Show the Shape on a Map

### From the Shape - Extract Centroid
```sql

```

#### What is a Centroid?

#### Show Shape + Centroid on a Map

### From the Shape - Extract Bounding Box
```sql

```

#### Show Shape + Centroid + Bounding Box on a Map

### From the Shape - Extract Lat/Long of Centroid
```sql

```

#### Show Shape + Centroid + Bounding Box + Lat/Long Lines on a Map

### From the Shape - Extract Min/Max of Lat/Long of Bounding Box
```sql

```

#### Show Shape + Centroid + Bounding Box + Min/Max Lat/Long Lines on a Map

## Practical applications

### Showing things on Maps!

At a surface level, the main purpose of having shapefile data, as opposed to just using tabular data + links is to show the data on maps.
e.g., Show the states

Want to make the case that yes, showing on maps is in many ways the ultimate goal. However, once you have this data in your data base you can then introduce a lot more logic and inntelligence in how you relate data to each other based on the relationships to the shapes.

### Aggregate data, show on map, scaled by count
e.g., Show thematic map with data aggregated by state

```sql

```

-----map-----

### Distance Calculations

#### Query for Distance
Given two shapes, determine how far apart they are
e.g., how far apart are California and Ohio

#### Filter based on Distance

Which states have their centroid within 1000 miles of Ohio?

#### Filter based on a bounding box

Which Zip Codes have their centroids within Ohio's Bounding Box?

### Spatial Operations - Within and Intersects

Demo: Show the zip codes in a state

#### Method 1: Determined by shapefile metadata
- Is there a way to see which zip codes are mostly within a state? (Zip3 data/logic?)

#### Method 2: st_within()
- Within will show you which zip code shapes are completely contained _within_ another shape
- Show Zip Codes within Ohio

#### Method 3: st_intersects()
- Intersects will show you which zip code shapes are within, cross into, or touch another shape.
- Very wide answer; Any zip codes that have anything to do with Ohio.

### Spatial Operations - ??? Other Interesting Methods?

## 

### Limitations of Mapping Applications

If you house shape data in your database, you can build responsive mapping applications on top of them
(Thinking have 1 map, and then multiple layers we can click on/off)
- Show the states in the US
- Show points in the states
- Tooltip for a point, note the state, cound/tabulate data by state

### Geography Types
Point Geographies
Shape geographies
??? Line
??? Other

### Spatial Joins
This is how to get shape file properties applied to other facts

#### Example associating x points with states
Points now have a state code on them

#### Not just States...
- Census Tracts, UNSD, CD, (something obscure...?)
(Show Ohio map with each of these - 3 pictures)

#### Materialize shape properties onto fact table
- Spatial joins & writing properties from shapes onto facts

```sql
(Generic example)
```

### H3 Geospatial Index

#### What is it?

Definition:

##### Large Scale example ~1 index shape per state

##### Small Scale example ~1 index shape for a building

#### Point Data - Convert to H3 Index
#### Point Data - Convert to H3 Index and Boundary (for shape)
#### Point Data - Convert to H3 Index + Boundary & Aggregate

(Show on Map)

#### Example - Using a large H3 shape boundary, intersect with zip codes
Aggregating point data by shapefile properties

Folium demo - interspersed in the talk, but show some of the results at the end.