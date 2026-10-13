import os
import duckdb

SHEET_ID = '1CDTeJ-IHirS_mgyw8zVc03yKwyirYfoifcHQXKaTrs0'

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


def load_sheet():
    # Access the data in Google Sheets and import it into DuckDB.
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.smart_home.electricity_usage_1_min AS
        select * from read_gsheet('{SHEET_ID}')
    """)


# Only sign in if there's no saved token yet
has_secret = con.execute(
    "SELECT count(*) FROM duckdb_secrets() WHERE name = 'gsheet_auth'"
).fetchone()[0]
if not has_secret:
    authenticate()

try:
    load_sheet()
except duckdb.Error as e:
    # Most likely the saved token expired. Sign in again and retry once.
    print(f"Google Sheets read failed, re-authenticating: {e}")
    authenticate()
    load_sheet()

# duckdb_tables() lists every table DuckDB knows about, including its row count
tables = con.execute("""
    SELECT table_name, estimated_size AS row_count, column_count
    FROM duckdb_tables()
    WHERE database_name = 'raw' AND schema_name = 'smart_home'
    ORDER BY table_name
""").df()
print("\nTables in raw.smart_home")
print(tables)
