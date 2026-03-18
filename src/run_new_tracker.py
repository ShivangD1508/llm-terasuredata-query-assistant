#!/usr/bin/env python3
"""
Script to run the new_tracker query and export results to CSV.
"""

import pandas as pd
import time
from datetime import datetime
import sys
import os
import re

# Add the src directory to the path so we can import check_connection
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from check_connection import run_query

def read_sql_file():
    """Read the SQL file and extract query and tracker date."""
    sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'new_tracker.sql')
    
    try:
        with open(sql_path, 'r') as f:
            content = f.read()
        
        # Extract tracker date from SET command
        date_match = re.search(r"SET tracker_date = '([^']*)';", content)
        tracker_date = date_match.group(1) if date_match else '2025-06-07'
        
        # Remove SET line and replace Hive variables with Python format placeholders
        content = re.sub(r"SET tracker_date = '[^']*';\s*", "", content)
        content = content.replace("${hiveconf:tracker_date}", "'{TRACKER_DATE}'")
        
        return content.strip(), tracker_date
        
    except FileNotFoundError:
        print(f"Error: SQL file '{sql_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)

def execute_new_tracker_query():
    """Execute the new tracker query and return results."""
    
    print("=" * 80)
    print("NEW TRACKER - QUERY EXECUTION")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Read SQL file
    query, tracker_date = read_sql_file()
    print(f"Tracker date: {tracker_date}")
    print("=" * 80)
    
    try:
        formatted_query = query.format(TRACKER_DATE=tracker_date)
    except KeyError as e:
        print(f"Error formatting query: {e}")
        sys.exit(1)
    
    print("Executing new tracker query...")
    start_time = time.time()
    
    try:
        # Execute the query
        df = run_query(formatted_query)
        
        if df is not None and not df.empty:
            execution_time = (time.time() - start_time) / 60
            print(f"✅ SUCCESS: Query executed in {execution_time:.2f} minutes")
            print(f"Results shape: {df.shape}")
            return df, execution_time, tracker_date
        else:
            print("❌ ERROR: Query returned no results")
            return None, time.time() - start_time, tracker_date
            
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"❌ ERROR: {str(e)}")
        return None, execution_time, tracker_date

def save_results(df, tracker_date):
    """Save results to CSV file."""
    
    if df is None:
        print("No results to save.")
        return None
    
    # Generate output filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"new_tracker_results_{timestamp}.csv"
    
    # Create results directory if it doesn't exist
    results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'results')
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    # Save to CSV
    output_path = os.path.join(results_dir, output_file)
    df.to_csv(output_path)
    print(f"📁 Results saved to: {output_path}")
    
    return output_path

def read_summary_metrics_sql():
    """Read the summary metrics SQL file and extract query and tracker date."""
    sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'summary_metrics.sql')
    
    try:
        with open(sql_path, 'r') as f:
            content = f.read()
        
        # Extract tracker date from SET command
        date_match = re.search(r"SET tracker_date = '([^']*)';", content)
        tracker_date = date_match.group(1) if date_match else '2025-06-07'
        
        # Remove SET line and replace Hive variables with Python format placeholders
        content = re.sub(r"SET tracker_date = '[^']*';\s*", "", content)
        content = content.replace("${hiveconf:tracker_date}", "'{TRACKER_DATE}'")
        
        return content.strip(), tracker_date
        
    except FileNotFoundError:
        print(f"Error: SQL file '{sql_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)

def run_summary_metrics_on_csv(csv_file_path):
    """Run summary metrics calculations on a CSV file."""
    print("=" * 80)
    print("RUNNING SUMMARY METRICS ON CSV")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"CSV file: {csv_file_path}")
    
    # Read SQL file to get tracker date
    _, tracker_date = read_summary_metrics_sql()
    print(f"Tracker date: {tracker_date}")
    print("=" * 80)
    
    print("Reading CSV file and calculating summary metrics...")
    start_time = time.time()
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        print(f"✅ Successfully read CSV file with {len(df)} records")
        
        # Calculate summary metrics
        summary_metrics = {
            'marketables': len(df),
            'valid_email_subscribers': len(df[(df['valid_email'] == True) & (df['email_subscriber'] == True)]),
            'valid_email_non_subscribers': len(df[(df['valid_email'] == True) & (df['email_subscriber'] == False)]),
            'valid_email_non_subscribers_2yr_shopper': len(df[
                (df['valid_email'] == True) & 
                (df['email_subscriber'] == False) & 
                (df['joann_2yr_shopper'] == True)
            ]),
            'valid_email_non_subscribers_2yr_shopper_phone_only': len(df[
                (df['valid_email'] == True) & 
                (df['email_subscriber'] == False) & 
                (df['joann_2yr_shopper'] == True) &
                (df['valid_phone'] == True) &
                (df['valid_dm'] == False)
            ]),
            'valid_email_non_subscribers_2yr_shopper_dm_only': len(df[
                (df['valid_email'] == True) & 
                (df['email_subscriber'] == False) & 
                (df['joann_2yr_shopper'] == True) &
                (df['valid_phone'] == False) &
                (df['valid_dm'] == True)
            ]),
            'valid_email_non_subscribers_2yr_shopper_phone_and_dm': len(df[
                (df['valid_email'] == True) & 
                (df['email_subscriber'] == False) & 
                (df['joann_2yr_shopper'] == True) &
                (df['valid_phone'] == True) &
                (df['valid_dm'] == True)
            ]),
            'marketable_and_mik_non_shoppers': len(df[
                (df['marketable_flag'] == True) & 
                (df['mik_non_shopper'] == True)
            ]),
            'marketable_and_mik_active_shoppers': len(df[
                (df['marketable_flag'] == True) & 
                (df['mik_active_shopper'] == True)
            ]),
            'marketable_and_mik_lapsed_shoppers': len(df[
                (df['marketable_flag'] == True) & 
                (df['mik_lapsed_shopper'] == True)
            ])
        }
        
        # Convert to DataFrame
        summary_df = pd.DataFrame([summary_metrics])
        
        execution_time = (time.time() - start_time) / 60
        print(f"✅ SUCCESS: Summary metrics calculated in {execution_time:.2f} minutes")
        print(f"Results shape: {summary_df.shape}")
        return summary_df, execution_time, tracker_date
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"❌ ERROR: {str(e)}")
        return None, execution_time, tracker_date

def save_summary_results(summary_df, tracker_date):
    """Save summary results to CSV file."""
    
    if summary_df is None:
        print("No summary results to save.")
        return None
    
    # Generate output filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"new_tracker_summary_{timestamp}.csv"
    
    # Create results directory if it doesn't exist
    results_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'results')
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    # Save to CSV
    output_path = os.path.join(results_dir, output_file)
    summary_df.to_csv(output_path)
    print(f"📁 Summary results saved to: {output_path}")
    
    return output_path

def main():
    """Run the two-step process: new_tracker.sql -> CSV -> summary_metrics.sql."""
    print("=" * 80)
    print("TWO-STEP PROCESS: NEW_TRACKER -> CSV -> SUMMARY_METRICS")
    print("=" * 80)
    
    # Step 1: Run new_tracker.sql and save to CSV
    print("\n📊 STEP 1: Running new_tracker.sql...")
    df, execution_time, tracker_date = execute_new_tracker_query()
    
    if df is None:
        print("❌ Step 1 failed - no results to process.")
        return None
    
    # Save results to CSV
    csv_output_path = save_results(df, tracker_date)
    print(f"✅ Step 1 completed: Results saved to {csv_output_path}")
    
    # Step 2: Run summary_metrics.sql on the CSV file
    print("\n📊 STEP 2: Running summary_metrics.sql on CSV...")
    summary_df, summary_execution_time, _ = run_summary_metrics_on_csv(csv_output_path)
    
    if summary_df is not None:
        summary_output_path = save_summary_results(summary_df, tracker_date)
        print(f"✅ Step 2 completed: Summary saved to {summary_output_path}")
        
        total_time = (execution_time + summary_execution_time) / 60
        print(f"\n⏱️  Total execution time: {total_time:.2f} minutes")
        print(f"📁 CSV file: {csv_output_path}")
        print(f"📁 Summary file: {summary_output_path}")
    else:
        print("❌ Step 2 failed - could not generate summary metrics.")
    
    return summary_df

if __name__ == "__main__":
    main()
