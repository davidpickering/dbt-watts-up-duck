"""Map Cleveland water meters aggregated into H3 hexagons (resolution 8, ~0.7 km² per cell).

Run from the repo root after `dbt build`:  python h3_water_meters_map.py
Then open maps/h3_water_meters_map.html in a browser.
"""
import json
import branca.colormap as cm
import folium

from duckdb_setup.connection import get_readonly_connection

H3_RESOLUTION = 10
SAVE_PATH = 'maps/h3_water_meters_map.html'

con = get_readonly_connection()

# Aggregate points by H3 cell - just group by the H3 index
h3_aggregate_query = f"""
WITH h3_points AS (
    SELECT
        h3_latlng_to_cell(latitude, longitude, {H3_RESOLUTION}) as h3_index
    FROM stg_geocoding__water_meters
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND latitude <> 0
      AND city = 'Cleveland'
)
SELECT
    h3_index,
    COUNT(*) as water_meters,
    h3_cell_to_boundary_wkt(h3_index) as boundary_wkt,
    ST_AsGeoJSON(ST_GeomFromText(boundary_wkt)) as h3_geojson
FROM h3_points
GROUP BY h3_index
ORDER BY water_meters DESC
"""

h3_aggregated = con.execute(h3_aggregate_query).fetchdf()
print(f"{h3_aggregated['water_meters'].sum():,} water meters in {len(h3_aggregated):,} H3 cells (res {H3_RESOLUTION})")

# One FeatureCollection instead of one layer per cell keeps the map fast with thousands of hexagons
features = [
    {
        "type": "Feature",
        "geometry": json.loads(row['h3_geojson']),
        "properties": {"h3_index": str(row['h3_index']), "water_meters": int(row['water_meters'])},
    }
    for _, row in h3_aggregated.iterrows()
]
feature_collection = {"type": "FeatureCollection", "features": features}

# Color scale based on water meter count. Counts are skewed (a few very dense cells),
# so quantile steps keep the map from looking washed out.
colormap = cm.LinearColormap(
    ['#ffffcc', '#fd8d3c', '#800026'],
    vmin=h3_aggregated['water_meters'].min(),
    vmax=h3_aggregated['water_meters'].max(),
).to_step(data=h3_aggregated['water_meters'], n=6, method='quantiles')
colormap.caption = f'Water meters per H3 cell (res {H3_RESOLUTION})'

h3_map = folium.Map(location=[41.45, -81.70], zoom_start=10)

folium.GeoJson(
    feature_collection,
    style_function=lambda feature: {
        'fillColor': colormap(feature['properties']['water_meters']),
        'color': 'black',
        'weight': 0.3,
        'fillOpacity': 0.7,
    },
    tooltip=folium.GeoJsonTooltip(fields=['water_meters', 'h3_index'], aliases=['Water meters', 'H3 cell']),
).add_to(h3_map)

colormap.add_to(h3_map)

h3_map.save(SAVE_PATH)
print(f"Saved map to {SAVE_PATH}")
