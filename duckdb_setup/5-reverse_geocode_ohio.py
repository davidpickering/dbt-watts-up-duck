import duckdb
import time
import csv
import os
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def count_table_rows(con, dbname=None, schemaname=None, tablename=None):
    """Count rows in a table"""
    result = con.execute(f"SELECT COUNT(*) as total FROM {dbname}.{schemaname}.{tablename}").fetchone()
    return result[0]

def state_table_row_count(con, row_count=None, dbname=None, schemaname=None, tablename=None):
    """Get formatted row count message"""
    row_count = count_table_rows(con, dbname, schemaname, tablename)
    result = f"{dbname}.{schemaname}.{tablename} has {row_count} rows"
    return result

def reverse_geocode_coordinate(geolocator, latitude, longitude, max_retries=3):
    """
    Reverse geocode a single coordinate with retry logic
    """
    for attempt in range(max_retries):
        try:
            location = geolocator.reverse((latitude, longitude), timeout=10)
            if location:
                return {
                    'street_address': location.address,
                    'city': location.raw.get('address', {}).get('city') or 
                           location.raw.get('address', {}).get('town') or
                           location.raw.get('address', {}).get('village'),
                    'state': location.raw.get('address', {}).get('state'),
                    'zip_code': location.raw.get('address', {}).get('postcode'),
                    'geocoded_at': datetime.now().isoformat(),
                    'geocoding_source': 'Nominatim'
                }
            else:
                logger.warning(f"No location found for {latitude}, {longitude}")
                return None
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            logger.warning(f"Attempt {attempt + 1} failed for {latitude}, {longitude}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retry
            else:
                logger.error(f"All attempts failed for {latitude}, {longitude}")
                return None
        except Exception as e:
            logger.error(f"Unexpected error for {latitude}, {longitude}: {e}")
            return None
    
    return None

def main():
    """Main function to perform reverse geocoding for Ohio charging stations"""
    
    # Connect to raw database
    logger.info("Connecting to raw.db...")
    con = duckdb.connect('raw.db')
    
    # Check if charging stations data exists
    try:
        ohio_count = count_table_rows(con, "raw", "dcfast", "world_charging_stations")
        logger.info(f"Total charging stations in database: {ohio_count}")
        
        # Get Ohio stations (limited to 100 for demo purposes)
        ohio_stations = con.execute("""
            SELECT latitude, longitude, id, name, city, state_province
            FROM raw.dcfast.world_charging_stations 
            WHERE state_province = 'OH' 
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
            ORDER BY id
        """).fetchall()
        
        logger.info(f"Found {len(ohio_stations)} Ohio charging stations with coordinates")
        
        if len(ohio_stations) == 0:
            logger.error("No Ohio charging stations found. Please ensure data is loaded.")
            return
            
    except Exception as e:
        logger.error(f"Error accessing charging stations data: {e}")
        return
    
    # Initialize geocoder
    logger.info("Initializing Nominatim geocoder...")
    geolocator = Nominatim(user_agent="dbt-watts-up-duck-geocoding/1.0")
    
    # Prepare CSV file for output
    csv_filename = "ohio_geocoded_addresses.csv"
    csv_path = os.path.join("duckdb_setup", csv_filename)
    
    logger.info(f"Writing geocoded data to: {csv_path}")
    
    # CSV headers
    csv_headers = [
        'latitude', 'longitude', 'station_id', 'station_name', 'original_city', 'original_state',
        'street_address', 'city', 'state', 'zip_code', 'geocoded_at', 'geocoding_source'
    ]
    
    # Process each station and write to CSV
    successful_geocodes = 0
    failed_geocodes = 0
    
    logger.info(f"Starting reverse geocoding for {len(ohio_stations)} stations...")
    logger.info("Note: This will take approximately 1 second per station due to rate limiting")
    logger.info("Demo mode: Limited to 100 stations for faster testing")
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(csv_headers)  # Write header row
        
        for i, (latitude, longitude, station_id, station_name, original_city, original_state) in enumerate(ohio_stations, 1):
            logger.info(f"Processing station {i}/{len(ohio_stations)}: {station_name} ({latitude}, {longitude})")
            
            # Reverse geocode
            geocoded_data = reverse_geocode_coordinate(geolocator, latitude, longitude)
            
            if geocoded_data:
                # Write successful geocode to CSV
                row = [
                    latitude, longitude, station_id, station_name, original_city, original_state,
                    geocoded_data['street_address'], geocoded_data['city'], geocoded_data['state'],
                    geocoded_data['zip_code'], geocoded_data['geocoded_at'], geocoded_data['geocoding_source']
                ]
                writer.writerow(row)
                successful_geocodes += 1
                logger.info(f"✓ Geocoded: {geocoded_data['street_address']}")
            else:
                # Write failed geocode record to CSV
                row = [
                    latitude, longitude, station_id, station_name, original_city, original_state,
                    None, None, None, None, datetime.now().isoformat(), 'Failed'
                ]
                writer.writerow(row)
                failed_geocodes += 1
                logger.warning(f"✗ Failed to geocode")
            
            # Rate limiting - Nominatim allows ~1 request per second
            if i < len(ohio_stations):  # Don't sleep after the last item
                time.sleep(1.1)  # Slightly more than 1 second to be safe
    
    # Final summary
    logger.info("=" * 50)
    logger.info("GEOCODING COMPLETE")
    logger.info(f"Total stations processed: {len(ohio_stations)}")
    logger.info(f"Successful geocodes: {successful_geocodes}")
    logger.info(f"Failed geocodes: {failed_geocodes}")
    logger.info(f"Success rate: {(successful_geocodes/len(ohio_stations)*100):.1f}%")
    logger.info(f"CSV file created: {csv_path}")
    logger.info(f"File size: {os.path.getsize(csv_path)} bytes")
    
    # Show sample of successful geocodes from CSV
    logger.info("\nSample successful geocodes:")
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            if row['street_address'] and row['geocoding_source'] == 'Nominatim':
                logger.info(f"  {row['station_name']}: {row['street_address']}, {row['city']}, {row['state']} {row['zip_code']}")
                count += 1
                if count >= 5:
                    break
    
    con.close()
    logger.info("Database connection closed.")
    logger.info(f"\nNext steps:")
    logger.info(f"1. Copy {csv_filename} to project_data/ directory")
    logger.info(f"2. Run 'python duckdb_setup/3-load_raw_data.py' to load the CSV")
    logger.info(f"3. Run 'dbt run --models staging.geocoding warehouse.dim_addresses' to see enriched data")
    logger.info(f"\nTo geocode more stations, remove the LIMIT 100 from the SQL query in this script")

if __name__ == "__main__":
    main()
