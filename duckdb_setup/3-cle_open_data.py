"""
Download Cleveland Open Data sets to project_data/.

Source: https://data.clevelandohio.gov/ (ArcGIS Hub)
License: Open Data Commons Open Database License (ODbL)

ArcGIS Hub exposes a bulk download endpoint that renders the whole layer as a
single CSV, which avoids paging the FeatureServer 5,000 records at a time.
The file is cached on disk - pass --force to re-download it.

Once downloaded, 3-load_raw_data.py loads it into DuckDB.
"""

import argparse
import os
import urllib.request

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "project_data")

HUB_DOWNLOAD = "https://hub.arcgis.com/api/v3/datasets/{dataset_id}/downloads/data"

# Each entry: the ArcGIS Hub dataset id, and the filename to save it as.
DATASETS = [
    {
        # Address Points - Cleveland Water Service Area (~616k rows, ~200 MB)
        # https://data.clevelandohio.gov/maps/92a311dcc77446b9bbbaa4403e0e2bdb
        "dataset_id": "92a311dcc77446b9bbbaa4403e0e2bdb_0",
        "filename": "cle_open_data_water_meters.csv",
    },
]


def download(dataset_id, filename, force=False):
    dest_path = os.path.join(BASE_DIR, filename)

    os.makedirs(BASE_DIR, exist_ok=True)

    if os.path.exists(dest_path) and not force:
        size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        print(f"{filename} already present ({size_mb:.1f} MB) - skipping. Use --force to re-download.")
        return

    url = HUB_DOWNLOAD.format(dataset_id=dataset_id) + "?format=csv&spatialRefId=4326&where=1%3D1"

    # Hub generates the export on demand, so the first request can sit for a
    # while before any bytes arrive. Download to a .part file so an interrupted
    # run never leaves a truncated CSV behind for the loader to find.
    print(f"Downloading {filename} from ArcGIS Hub (this takes a minute)...")
    part_path = dest_path + ".part"
    try:
        with urllib.request.urlopen(url, timeout=600) as response, open(part_path, "wb") as out:
            downloaded = 0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                print(f"  {downloaded / (1024 * 1024):.1f} MB", end="\r")
        os.replace(part_path, dest_path)
        print(f"\nSaved {filename} ({downloaded / (1024 * 1024):.1f} MB) to {BASE_DIR}")
    except Exception:
        if os.path.exists(part_path):
            os.remove(part_path)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Cleveland Open Data sets to project_data/")
    parser.add_argument("--force", action="store_true", help="Re-download even if the file already exists")
    args = parser.parse_args()

    for dataset in DATASETS:
        download(dataset["dataset_id"], dataset["filename"], force=args.force)
