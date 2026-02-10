import wrds
import os
import pandas as pd
from sqlalchemy import text

# 1. SETTINGS
USERNAME = 'kazis'
OUTPUT_DIR = "./Datasets/ibes/summary_stats"
START_YEAR = 2004
END_YEAR = 2025

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 2. CONNECT TO WRDS
db = wrds.Connection(wrds_username=USERNAME)

# 3. DEFINE COLUMNS 
# Based on the nstatsum_epsus schema you provided
cols = [
    "ticker", "cusip", "statpers", "fpedats", "fpi", 
    "meanest", "medest", "stdev", "numest", "highest", "lowest",
    "actual", "anndats_act", "usfirm"
]
cols_sql = ", ".join(cols)

# 4. FETCH DATA YEAR BY YEAR
for year in range(START_YEAR, END_YEAR + 1):
    print(f"Fetching IBES data for {year}...")
    
    # We filter by statpers (the snapshot date) to get everything in that calendar year
    query = text(f"""
        SELECT {cols_sql} 
        FROM ibes.nstatsum_epsus 
        WHERE statpers >= '{year}-01-01' 
          AND statpers <= '{year}-12-31'
    """)
    
    try:
        # Using the direct engine connection to avoid SQLAlchemy 2.0 cursor errors
        with db.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if not df.empty:
            # Save as Parquet for Polars compatibility
            output_file = os.path.join(OUTPUT_DIR, f"ibes_eps_{year}.parquet")
            df.to_parquet(output_file, index=False)
            print(f"Saved {len(df)} rows to {output_file}")
        else:
            print(f"No data found for {year}.")
            
    except Exception as e:
        print(f"Error fetching {year}: {e}")

db.close()
print("Download complete.")
