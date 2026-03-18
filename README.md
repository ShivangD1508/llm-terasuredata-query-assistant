# LLM Treasure Data Query Assistant

A lightweight natural-language-to-SQL assistant built with Python, OpenAI, and Treasure Data.

This project allows users to ask business questions in plain English and automatically generates SQL queries that run against Treasure Data.

## Features

- Natural language to SQL generation using OpenAI
- Treasure Data query execution
- Streamlit UI for interactive querying
- Secure API key handling using .env
- Modular Python architecture

## Example Questions

- Count total customers
- Show T12 shoppers
- Get customers with spend > 50

## Tech Stack

- Python
- OpenAI API
- Treasure Data API
- Streamlit

## Project Structure

src/  
├── app.py  
├── llm_td_runner.py  
├── td_runner.py  

## Setup

1. Add API keys in `.env`
2. Install dependencies
3. Run:

streamlit run src/app.py

## Note

Internal business tables are excluded in public version.