import os
import duckdb


SHEET_IDS = ['1RYTIK0abgmycMxwuAdp8VnYqIlEdmEsuLQ30-V1zz6U', '1RrVGQOgy3Rku5Zr7R1CrdE9vM0CYT3fqtpNbuMokWFQ', '1b3--R8uf3evma-i33ESBKpHpuz3Q8vykek--iFVADJs', '1KrhcBgcIgQ7JahnWXSYTSV1q8GGAX5VMCRdLSBRnld8']
SHEET_NAMES = ['electricity_usage_1_hour', 'electricity_usage_1_min', 'airthings_dining_room_20251001_20260930', 'airthings-basement-20251001-20260930']

con = duckdb.connect('development.duckdb')
con.execute("ATTACH 'raw.db' AS raw")
con.execute("INSTALL gsheets FROM community")
con.execute("LOAD gsheets")


def authenticate():
    """Open a browser to sign in to Google, then save the token as a persistent secret.

    Persistent secrets are stored outside the repo (in ~/.duckdb/stored_secrets),
    so later runs reuse the token until it expires (about an hour).
    """
    print("Opening a browser to sign in to Google. Paste the token back here when prompted.")
    con.execute("DROP PERSISTENT SECRET IF EXISTS gsheet_auth")
    con.execute("CREATE PERSISTENT SECRET gsheet_auth (TYPE gsheet)")

    # Alternative: a service account key file never expires, so you never sign in through the browser.
    # Share the sheet with the service account's email first, then use this instead of the line above.
    # Set GSHEET_KEY_FILE to the key's path, e.g. $env:GSHEET_KEY_FILE = "C:/path/to/key.json"
    # con.execute(f"""CREATE OR REPLACE PERSISTENT SECRET gsheet_auth (
    #     TYPE gsheet, PROVIDER key_file, FILEPATH '{os.environ["GSHEET_KEY_FILE"]}')""")


def load_sheet(sheet_id, sheet_name):
    # Hyphens are a SQL syntax error in table names (unless quoted), so swap them for underscores
    table_name = sheet_name.replace('-', '_')
    # Access the data in Google Sheets and import it into DuckDB.
    print(f"Loading sheet {sheet_name} from Google Sheets ID {sheet_id} into raw.smart_home.{table_name}")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.smart_home.{table_name} AS
        select * from read_gsheet('{sheet_id}')
    """)
    print(f"Loaded {con.execute(f'SELECT count(*) FROM raw.smart_home.{table_name}').fetchone()[0]} rows into raw.smart_home.{table_name}")

# Only sign in if there's no saved token yet
has_secret = con.execute(
    "SELECT count(*) FROM duckdb_secrets() WHERE name = 'gsheet_auth'"
).fetchone()[0]
if not has_secret:
    authenticate()

reauthenticated = False
for sheet_id, sheet_name in zip(SHEET_IDS, SHEET_NAMES):
    try:
        load_sheet(sheet_id, sheet_name)
    except duckdb.ParserException:
        # A SQL mistake, not a login problem. Signing in again won't help.
        raise
    except duckdb.Error as e:
        if reauthenticated:
            raise
        # Most likely the saved token expired. Sign in again and retry this sheet once.
        print(f"Google Sheets read failed, re-authenticating: {e}")
        authenticate()
        reauthenticated = True
        load_sheet(sheet_id, sheet_name)

# duckdb_tables() lists every table DuckDB knows about, including its row count
tables = con.execute("""
    SELECT table_name, estimated_size AS row_count, column_count
    FROM duckdb_tables()
    WHERE database_name = 'raw' AND schema_name = 'smart_home'
    ORDER BY table_name
""").df()
print("\nTables in raw.smart_home")
print(tables)
