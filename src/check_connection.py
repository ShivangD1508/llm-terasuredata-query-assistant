# treasure_data_client.py

import os
import warnings
from dotenv import load_dotenv
import pytd.pandas_td as td

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
engine = td.create_engine("hive:mk_src", con=con)


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


# === EXAMPLE USAGE ===
if __name__ == "__main__":
    # Example query
    query = """
    select *
    from mk_src.joanns_customers
    limit 1
    """

    df = run_query(query, verbose=True)
    if df is not None:
        print("Results:")
        print(df)
