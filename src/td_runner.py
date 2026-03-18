import os
from dotenv import load_dotenv
import pytd.pandas_td as td

load_dotenv()

TD_API_KEY = os.getenv("TD_API_KEY")
TD_ENDPOINT = "https://api.treasuredata.com"

con = td.connect(apikey=TD_API_KEY, endpoint=TD_ENDPOINT)
engine = td.create_engine("hive:mk_src", con=con)

def run_td_query(query):
    df = td.read_td_query(query, engine)
    return df

# Example test
if __name__ == "__main__":
    q = """
    select count(*) as total_customers
    from mk_src.joanns_customers
    """
    result = run_td_query(q)
    print(result)
