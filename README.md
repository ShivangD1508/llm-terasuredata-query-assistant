# Joann Customer Analytics Tracker

A Python tool for executing SQL queries against Treasure Data to analyze Joann customer data and compare it with MIK customer data.

## Overview

This project runs a series of SQL queries to track customer overlap and migration between Joann and MIK.

## Prerequisites

- Python 3.10 or higher
- `uv` package manager (Note: Using `uv` is optional. Alternatively, `conda` or simply `python -m venv` command can be used to create a Python virtual environment for this project.)
- Treasure Data API access

## Setup

1. **Clone the repository**
   ```bash
   git clone git@github.com:vishal-git/joann.git
   cd joann
   ```

2. **Install dependencies using uv**
   ```bash
   uv sync
   ```

3. **Set up environment variables**
   Create a `.env` file in the root directory:
   ```bash
   TD_API_KEY=your_treasure_data_api_key_here
   ```

## Usage

### 1. Test Your Connection

Before running the full analytics, test your Treasure Data connection:

```bash
uv run python src/check_connection.py
```

This will execute a simple test query to verify your API key and connection are working properly.

### 2. Run the Full Analytics

Execute the original tracker queries and generate results:

```bash
uv run python src/run_old_tracker.py
```

This will:
- Execute all SQL queries sequentially - the queries are located at `queries/old_tracker.sql`
- Display progress and results in real-time
- Save results to a timestamped CSV file in the `results/` directory
- Print a summary of successful and failed queries

Execute the new dashboard tracker query and generate results:

```bash
uv run python src/run_new_tracker.py
```

This will:
- Execute all SQL queries sequentially - the query is located at `queries/new_tracker_query.sql`
- Display progress and results in real-time
- Save results to a timestamped CSV file in the `results/` directory
- Print a summary of successful and failed queries

### 3. View Results

Results are automatically saved to `results/`.

## Project Structure

```
joann/
├── src/
│   ├── check_connection.py    # Database connection and test queries
│   ├── run_old_tracker.py     # Old tracker runner
│   └── run_new_tracker.py     # New tracker runner
├── queries/
│   ├── old_tracker.sql        # Old tracker queries
│   └── new_tracker.sql        # New tracker query
├── results/                   # Generated CSV results
├── pyproject.toml            # Project dependencies
└── README.md                 # This file
```

## Dependencies

- `pytd>=1.9.0`: Treasure Data Python client
- `python-dotenv>=1.1.1`: Environment variable management
- `pandas`: Data manipulation (included with pytd)