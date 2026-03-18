import os
from dotenv import load_dotenv
from openai import OpenAI
from td_runner import run_td_query

# Load environment variables
load_dotenv()

# OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Business question
question = input("Ask your TD question: ")

# Prompt
prompt = f"""
You are an expert SQL generator for Treasure Data.

Available table:
mk_src.joanns_customers

Question:
{question}

Return only plain SQL.
No markdown.
No ```sql
No explanation.
"""

# Generate SQL
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "user", "content": prompt}
    ]
)

# Extract SQL
sql = response.choices[0].message.content.strip()

# Clean markdown if any appears
sql = sql.replace("```sql", "").replace("```", "").strip()

print("\nGenerated SQL:")
print(sql)

# Run TD query
result = run_td_query(sql)

print("\nQuery Result:")
print(result)