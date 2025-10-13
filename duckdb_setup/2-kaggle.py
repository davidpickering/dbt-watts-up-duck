import kagglehub
import shutil
from pathlib import Path
from typing import List, Optional


def copy_csv_files(source_path: str, csv_files: Optional[List[str]] = None,
                   dest_dir: Optional[str | Path] = None) -> int:
    """
    Copy CSV files from source directory to destination directory.

    Args:
        source_path: Path to the source directory containing CSV files
        csv_files: List of specific CSV filenames to copy. If None or empty, copies all CSV files.
        dest_dir: Destination directory (str or Path). If None, uses project_data directory.

    Returns:
        Number of files successfully copied
    """
    source_dir = Path(source_path)

    # Use project_data directory if no destination specified
    if dest_dir is None:
        dest_dir = Path(__file__).parent.parent / "project_data"
    else:
        # Convert to Path if string
        dest_dir = Path(dest_dir)

    # Ensure destination directory exists
    dest_dir.mkdir(parents=True, exist_ok=True)

    # If no specific files provided, copy all CSV files
    if not csv_files:
        csv_files = [f.name for f in source_dir.glob("*.csv")]
        print(f"No specific files selected. Found {len(csv_files)} CSV files to copy:")
        for csv_file in csv_files:
            print(f"  - {csv_file}")

        # If no CSV files found in root, check subdirectories
        if len(csv_files) == 0:
            print(f"\nNo CSV files in root directory. Checking subdirectories...")
            csv_files = [f.relative_to(source_dir).as_posix() for f in source_dir.glob("**/*.csv")]
            if csv_files:
                print(f"Found {len(csv_files)} CSV files in subdirectories:")
                for csv_file in csv_files:
                    print(f"  - {csv_file}")

    # Copy selected CSV files to destination directory
    copied_files = []
    for csv_file in csv_files:
        source_file = source_dir / csv_file
        dest_file = dest_dir / Path(csv_file).name  # Use only filename for destination

        if source_file.exists():
            shutil.copy2(source_file, dest_file)
            copied_files.append(csv_file)
            print(f"Copied: {csv_file}")
        else:
            print(f"Warning: {csv_file} not found in {source_dir}")
            print(f"  Checked path: {source_file}")

    print(f"\nSuccessfully copied {len(copied_files)} file(s) to {dest_dir}")
    return len(copied_files)


# Download latest version
# https://www.kaggle.com/datasets/tarekmasryo/global-ev-charging-stations
path = kagglehub.dataset_download("tarekmasryo/global-ev-charging-stations")
print("Path to dataset files:", path)

# List of CSV files to copy (add/modify as needed)
csv_files_to_copy = [
    "charging_stations_2025_ml.csv",
    "charging_stations_2025_world.csv",
    "country_summary_2025.csv",
    "ev_models_2025.csv",
    "world_summary_2025.csv"
]

copy_csv_files(path, csv_files_to_copy, "project_data/tarekmasryo")

# 2nd dataset
# https://www.kaggle.com/datasets/anshtanwar/residential-ev-chargingfrom-apartment-buildings
path = kagglehub.dataset_download("anshtanwar/residential-ev-chargingfrom-apartment-buildings")
print("Path to dataset files:", path)

# Copy all CSV files from 2nd dataset (or specify a list)
copy_csv_files(path, None, "project_data/anshtanwar")

### 3rd dataset

# Download latest version
# https://www.kaggle.com/datasets/datasetengineer/ev-intelligent-port-logistics
path = kagglehub.dataset_download("datasetengineer/ev-intelligent-port-logistics")

print("Path to dataset files:", path)
copy_csv_files(path, None, "project_data/datasetengineer")


copy_csv_files("duckdb_setup", None, "project_data")