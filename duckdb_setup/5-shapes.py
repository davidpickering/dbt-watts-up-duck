import os
import zipfile
import urllib.request
import duckdb

import folium
from IPython.display import display
import json

# Use shared connection with extensions loaded
from connection import get_connection

con = duckdb.connect('development.duckdb')
con.execute("ATTACH 'raw.db' AS raw")
con.execute("INSTALL spatial")
con.execute("LOAD spatial")
BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "project_data", "shapes")

# Each entry: URL to the .zip file, and the directory name to extract into.
# The .zip is kept in project_data/shapes/ alongside the extracted folder.
SHAPEFILES = [
    {
        "url": "https://www2.census.gov/geo/tiger/TIGER2025/STATE/tl_2025_us_state.zip",
        "extract_dir": "tl_2025_us_state",
    },
    # Add more shapefiles here, e.g.:
    # {
    #     "url": "https://www2.census.gov/geo/tiger/TIGER2025/ZCTA5/tl_2025_us_zcta5.zip",
    #     "extract_dir": "tl_2025_us_zcta5",
    # },
]


def download_and_extract(url, extract_dir):
    zip_filename = os.path.basename(url)
    zip_path = os.path.join(BASE_DIR, zip_filename)
    extract_path = os.path.join(BASE_DIR, extract_dir)

    os.makedirs(extract_path, exist_ok=True)

    if not os.path.exists(zip_path):
        print(f"Downloading {zip_filename}...")
        urllib.request.urlretrieve(url, zip_path)
        print(f"  Saved to {zip_path}")
    else:
        print(f"Already have {zip_filename}, skipping download.")

    print(f"Extracting to {extract_path}...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_path)
    print(f"  Done. Contents: {os.listdir(extract_path)}")


if __name__ == "__main__":
    os.makedirs(BASE_DIR, exist_ok=True)

    for entry in SHAPEFILES:
        download_and_extract(entry["url"], entry["extract_dir"])

    print("\nAll shapefiles ready.")


shapefile_path = os.path.join(BASE_DIR, "tl_2025_us_state", "tl_2025_us_state.shp")
con.execute(f"""
    CREATE OR REPLACE TABLE raw.shapes.states AS
    SELECT * FROM st_read('{shapefile_path}')
""")