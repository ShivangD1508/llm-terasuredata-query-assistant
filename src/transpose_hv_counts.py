import pandas as pd
import numpy as np
from pathlib import Path
import argparse


def load_and_clean_data(csv_path: str) -> pd.DataFrame:
    """Load the CSV and clean the data."""
    df = pd.read_csv(csv_path)
    
    # Convert reporting_as_of to datetime
    df['reporting_as_of'] = pd.to_datetime(df['reporting_as_of'], format='%m/%d/%Y')
    
    # Sort by date to ensure proper order for incremental calculations
    df = df.sort_values('reporting_as_of').reset_index(drop=True)
    
    return df


def transpose_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transpose the data so customer_movement_category values become columns."""
    
    # Filter out UNMOVED category
    df_filtered = df[df['customer_movement_category'] != 'UNMOVED'].copy()
    
    # Pivot the data with reporting_as_of as index and customer_movement_category as columns
    # Use 'ct' as the values
    pivoted = df_filtered.pivot_table(
        index='reporting_as_of',
        columns='customer_movement_category',
        values='ct',
        fill_value=0  # Fill missing values with 0
    )
    
    # Reset index to make reporting_as_of a regular column
    pivoted = pivoted.reset_index()
    
    # Rename columns to be more readable (remove spaces and special characters)
    pivoted.columns = [col.replace(' ', '_').replace(':', '').replace('(', '').replace(')', '') 
                      for col in pivoted.columns]
    
    return pivoted


def calculate_incremental(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate incremental differences between consecutive dates for each column."""
    
    # Create a copy to avoid modifying the original
    result_df = df.copy()
    
    # Get all columns except the date column
    date_col = 'reporting_as_of'
    value_cols = [col for col in df.columns if col != date_col]
    
    # Calculate incremental differences
    for col in value_cols:
        # Calculate the difference between current row and previous row
        incremental_values = result_df[col].diff()
        
        # For the first row, the incremental is the same as the cumulative value
        incremental_values.iloc[0] = result_df[col].iloc[0]
        
        # Assign the incremental values to the new column
        result_df[f'{col}_incremental'] = incremental_values
    
    return result_df


def add_summary_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Add summary statistics to the dataframe."""
    
    # Get all incremental columns
    incremental_cols = [col for col in df.columns if col.endswith('_incremental')]
    
    # Calculate total incremental for each date
    df['total_incremental'] = df[incremental_cols].sum(axis=1)
    
    # Calculate running totals (cumulative sum of incremental values)
    df['total_cumulative'] = df['total_incremental'].cumsum()
    
    return df


def main():
    parser = argparse.ArgumentParser(description='Transpose HV raw counts and calculate incremental quantities')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('-o', '--output', help='Path to the output CSV file', 
                       default='data/joann_hv_transposed_counts.csv')
    parser.add_argument('--show-preview', action='store_true', 
                       help='Show a preview of the data')
    
    args = parser.parse_args()
    
    # Load and clean the data
    print(f"Loading data from {args.input_file}...")
    df = load_and_clean_data(args.input_file)
    print(f"Loaded {len(df)} records")
    
    # Show data preview
    if args.show_preview:
        print("\nOriginal data preview:")
        print(df.head(10))
        print(f"\nUnique dates: {df['reporting_as_of'].nunique()}")
        print(f"Unique categories: {df['customer_movement_category'].unique()}")
    
    # Transpose the data
    print("\nTransposing data...")
    transposed_df = transpose_data(df)
    print(f"Transposed data shape: {transposed_df.shape}")
    
    # Calculate incremental quantities
    print("Calculating incremental quantities...")
    result_df = calculate_incremental(transposed_df)
    
    # Add summary statistics
    result_df = add_summary_stats(result_df)
    
    # Show results preview
    if args.show_preview:
        print("\nTransposed data preview:")
        print(result_df.head())
        print("\nColumn names:")
        print(result_df.columns.tolist())
    
    # Save the results
    output_path = Path(args.output)
    result_df.to_csv(output_path, index=False)
    print(f"\nResults saved to {output_path}")
    
    # Show summary statistics
    print("\nSummary:")
    print(f"Date range: {result_df['reporting_as_of'].min()} to {result_df['reporting_as_of'].max()}")
    print(f"Total records: {len(result_df)}")
    
    # Show incremental totals by date
    print("\nIncremental totals by date:")
    summary_cols = ['reporting_as_of', 'total_incremental', 'total_cumulative']
    print(result_df[summary_cols].to_string(index=False))


if __name__ == "__main__":
    main()
