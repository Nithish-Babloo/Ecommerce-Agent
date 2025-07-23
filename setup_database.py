import pandas as pd
from sqlalchemy import create_engine
import sys

def get_google_sheet_csv_url(sheet_url):
    """Converts a Google Sheet URL to a direct CSV download link."""
    # The provided URLs have extra fragments like '#gid=...', which we need to handle.
    # We also need to convert the base URL to the export format.
    if '/edit?gid=' in sheet_url:
        base_url = sheet_url.split('/edit?gid=')[0]
        return f'{base_url}/export?format=csv&gid={sheet_url.split("gid=")[-1]}'
    else:
        return sheet_url.replace('/edit?usp=sharing', '/export?format=csv&gid=0')


# ==============================================================================
#  ACTION REQUIRED: Paste your Google Sheet URLs into the dictionary below.
#  Make sure the sharing permissions are set to "Anyone with the link can view".
# ==============================================================================
URLS = {
    "eligibility": "https://docs.google.com/spreadsheets/d/1Loc32KsHwEGhLAahSfMA6t1aZdEvxJIPADxpdzZEZTw/edit?gid=95626969#gid=95626969",
    "ad_sales": "https://docs.google.com/spreadsheets/d/1ZATJteA4sU7DXN-fqJxG8Td_Nwif5QB2fTQvGK8LegY/edit?gid=1720576947#gid=1720576947",
    "total_sales": "https://docs.google.com/spreadsheets/d/1ftXt9Z6uEXUMlIHSZK0CR2kLlNZyj8TUi4lQmMF6qWo/edit?gid=1942712772#gid=1942712772"
}


# Check if placeholder URLs have been replaced
if "PASTE_YOUR" in URLS["eligibility"]:
    print("ERROR: Please replace the placeholder URLs in the setup_database.py script.", file=sys.stderr)
    sys.exit(1)

print("Downloading data from Google Sheets...")
try:
    eligibility_df = pd.read_csv(get_google_sheet_csv_url(URLS["eligibility"]))
    ad_sales_df = pd.read_csv(get_google_sheet_csv_url(URLS["ad_sales"]))
    total_sales_df = pd.read_csv(get_google_sheet_csv_url(URLS["total_sales"]))
    print("✅ Data downloaded successfully.")
except Exception as e:
    print(f"❌ ERROR: Failed to download or read the data. {e}", file=sys.stderr)
    sys.exit(1)

# Clean column names to be SQL-friendly
for df in [eligibility_df, ad_sales_df, total_sales_df]:
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace(r'[^a-zA-Z0-9_]', '', regex=True)

# Create and connect to the SQLite database
engine = create_engine('sqlite:///ecommerce.db')

# Load data into the database
print("\nWriting data to local SQLite database 'ecommerce.db'...")
ad_sales_df.to_sql('ad_sales', engine, if_exists='replace', index=False)
total_sales_df.to_sql('total_sales', engine, if_exists='replace', index=False)
eligibility_df.to_sql('eligibility', engine, if_exists='replace', index=False)

print("✅ Database setup complete! Tables created: ad_sales, total_sales, eligibility")
