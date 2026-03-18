# export_netnew_profiling.py

import os
import warnings
from dotenv import load_dotenv
import pytd.pandas_td as td
import pandas as pd
from datetime import datetime

# Suppress warnings from pytd/tdclient
warnings.filterwarnings("ignore", category=UserWarning, module="pytd")
warnings.filterwarnings("ignore", category=UserWarning, module="tdclient")

# Load environment variables from .env file
load_dotenv()

# === CONFIGURATION ===
TD_API_KEY = os.getenv("TD_API_KEY")
TD_ENDPOINT = "https://api.treasuredata.com"

# === SETUP ===
# Create connection and engine
con = td.connect(apikey=TD_API_KEY, endpoint=TD_ENDPOINT)
engine = td.create_engine("hive:sandbox_db", con=con)


def run_query(query, verbose=False):
    """Execute a query and return results as pandas DataFrame"""
    if verbose:
        print(f"Executing query: {query}")

    try:
        # Suppress warnings during query execution
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # Execute the query and get results as DataFrame
            df = td.read_td_query(query, engine)
        print("Query executed successfully!")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        return None


def save_to_zipped_csv(df, output_dir="data", filename_prefix="jn_netnew_profiling"):
    """Save DataFrame to a zipped CSV file"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Save to CSV with compression
    print(f"Saving to {filepath}.zip...")
    df.to_csv(f"{filepath}.zip", index=False, compression="zip")
    
    print(f"File saved successfully: {filepath}.zip")
    print(f"Total rows exported: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    
    return f"{filepath}.zip"


# === MAIN EXECUTION ===
if __name__ == "__main__":
    # Query to pull all data from sandbox_db.jn_netnew_profiling
    query = """
    SELECT *
    FROM sandbox_db.jn_netnew_profiling
    """

    print("Starting data export...")
    print(f"Source table: sandbox_db.jn_netnew_profiling")
    print("-" * 50)
    
    # Execute query
    df = run_query(query, verbose=True)
    
    if df is not None:
        print("-" * 50)
        print(f"Data retrieved: {len(df)} rows, {len(df.columns)} columns")
        print("\nColumn names:")
        print(df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head())
        print("-" * 50)
        
        # Save to zipped CSV
        output_file = save_to_zipped_csv(df)
        print(f"\n✓ Export complete: {output_file}")
    else:
        print("\n✗ Export failed: Unable to retrieve data from database")
