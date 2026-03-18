import os
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI
from td_runner import run_td_query

# Load environment variables
load_dotenv()

# OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

st.title("TD LLM Query Assistant")

# User input
question = st.text_input("Ask your Treasure Data question:")

if st.button("Run Query") and question:

    prompt = f"""
    You are an expert SQL generator for Treasure Data.

    Available table:
    mk_src.joanns_customers

    Question:
    {question}

    Return only plain SQL.
    Always alias aggregate columns clearly.
    No markdown.
    No explanation.
    """

    # Generate SQL
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    st.subheader("Generated SQL")
    st.code(sql)

    # Run TD query
    result = run_td_query(sql)

    st.subheader("Query Result")
    st.dataframe(result)