#!/usr/bin/env python3
"""
Joann Customer Analytics Runner
Executes all SQL queries sequentially and stores results in a DataFrame
"""

import pandas as pd
import time
from datetime import datetime
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from check_connection import run_query
from sql.queries import queries, TRACKER_DATE

def run_all_analytics():
    """Execute all queries and return results as DataFrame"""
    
    print("=" * 80)
    print("JOANN TRACKER - QUERY EXECUTION")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total queries to execute: {len(queries)}")
    print(f"Tracker date: {TRACKER_DATE}")
    print("=" * 80)
    
    all_metrics = {}
    successful_queries = 0
    failed_queries = 0
    
    for i, (query_id, query_info) in enumerate(list(queries.items()), 1):
        print(f"\n[{i:2d}/{len(queries)}] Executing: {query_info['description']}")
        print(f"Query ID: {query_id}")
        
        start_time = time.time()
        
        try:
            # Execute the query
            df = run_query(query_info['sql'])
            
            if df is not None and not df.empty:
                execution_time = time.time() - start_time
                
                # Handle different query result formats
                if query_id in ["MIK_email_file_metrics", "MIK_transaction_metrics"]:
                    # Multi-column queries - extract each column as a separate metric
                    for col_name in df.columns:
                        metric_name = f"{query_id}_{col_name}"
                        metric_value = df.iloc[0][col_name]
                        all_metrics[metric_name] = {
                            'value': metric_value,
                            'query_id': query_id,
                            'description': f"{query_info['description']} - {col_name}",
                            'execution_time_seconds': round(execution_time, 2),
                            'status': 'success'
                        }
                        print(f"✅ {col_name}: {metric_value:,}")
                    
                else:
                    # Single count queries
                    count_value = df.iloc[0, 0]
                    metric_name = query_id
                    all_metrics[metric_name] = {
                        'value': count_value,
                        'query_id': query_id,
                        'description': query_info['description'],
                        'execution_time_seconds': round(execution_time, 2),
                        'status': 'success'
                    }
                    print(f"✅ SUCCESS: {count_value:,}")
                
                print(f"⏱️  Execution time: {execution_time:.2f} seconds")
                successful_queries += 1
                
            else:
                all_metrics[query_id] = {
                    'value': None,
                    'query_id': query_id,
                    'description': query_info['description'],
                    'execution_time_seconds': time.time() - start_time,
                    'status': 'no_results'
                }
                print("⚠️  WARNING: Query returned no results")
                failed_queries += 1
                
        except Exception as e:
            execution_time = time.time() - start_time
            all_metrics[query_id] = {
                'value': None,
                'query_id': query_id,
                'description': query_info['description'],
                'execution_time_seconds': round(execution_time, 2),
                'status': f'error: {str(e)}'
            }
            print(f"❌ ERROR: {str(e)}")
            failed_queries += 1
    
    # Create results DataFrame
    results_df = pd.DataFrame(list(all_metrics.values()))
    
    # Print summary
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Total queries: {len(queries)}")
    print(f"Successful: {successful_queries}")
    print(f"Failed: {failed_queries}")
    print(f"Total metrics extracted: {len(all_metrics)}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    return results_df

def create_tracker_dataset(results_df):
    """Create a single dataset with all metrics as rows and TRACKER_DATE as column"""
    
    # Filter successful results
    successful_results = results_df[results_df['status'] == 'success'].copy()
    
    if successful_results.empty:
        print("❌ No successful results to create tracker dataset")
        return None
    
    # Create the tracker dataset
    tracker_data = []
    for _, row in successful_results.iterrows():
        tracker_data.append({
            'metric_name': row['query_id'],
            'description': row['description'],
            TRACKER_DATE: row['value'],
            'execution_time_seconds': row['execution_time_seconds']
        })
    
    tracker_df = pd.DataFrame(tracker_data)
    
    print(f"\n📊 TRACKER DATASET CREATED:")
    print(f"Total metrics: {len(tracker_df)}")
    print(f"Date column: {TRACKER_DATE}")
    
    return tracker_df

def create_output_directory():
    """Create output directory based on TRACKER_DATE"""
    # Get the project root directory (two levels up from src/)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Create results directory path
    results_dir = os.path.join(project_root, 'results')
    
    # Create folder name based on TRACKER_DATE
    # Convert date format from '2025-07-12' to '2025-07-12' (keep as is for folder name)
    folder_name = TRACKER_DATE
    output_dir = os.path.join(results_dir, folder_name)
    
    # Create the directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Created output directory: {output_dir}")
    else:
        print(f"📁 Using existing output directory: {output_dir}")
    
    return output_dir

def save_results(results_df, tracker_df=None, filename=None):
    """Save results to CSV file in the TRACKER_DATE directory"""
    
    # Create output directory
    output_dir = create_output_directory()
    
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"joann_tracker_results_{timestamp}.csv"
    
    # Create full file paths
    detailed_results_path = os.path.join(output_dir, filename)
    tracker_results_path = os.path.join(output_dir, filename.replace('.csv', '_tracker.csv'))
    
    # Save detailed results
    results_df.to_csv(detailed_results_path, index=False)
    print(f"\n📁 Detailed results saved to: {detailed_results_path}")
    
    # Save tracker dataset if available
    if tracker_df is not None:
        tracker_df.to_csv(tracker_results_path, index=False)
        print(f"📁 Tracker dataset saved to: {tracker_results_path}")
        return detailed_results_path, tracker_results_path
    
    return detailed_results_path

def print_results_summary(results_df, tracker_df=None):
    """Print a formatted summary of results"""
    print("\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    
    # Filter successful queries
    successful_results = results_df[results_df['status'] == 'success']
    
    if not successful_results.empty:
        print("\n📊 SUCCESSFUL METRICS:")
        print("-" * 80)
        for _, row in successful_results.iterrows():
            print(f"{row['description']:<60} {row['value']:>15,}")
    
    # Show failed queries
    failed_results = results_df[results_df['status'] != 'success']
    if not failed_results.empty:
        print(f"\n❌ FAILED QUERIES ({len(failed_results)}):")
        print("-" * 80)
        for _, row in failed_results.iterrows():
            print(f"{row['description']:<60} {row['status']}")
    
    # Show timing statistics
    if not successful_results.empty:
        avg_time = successful_results['execution_time_seconds'].mean()
        total_time = successful_results['execution_time_seconds'].sum()
        print(f"\n⏱️  TIMING:")
        print(f"Average execution time: {avg_time:.2f} seconds")
        print(f"Total execution time: {total_time:.2f} seconds")
    
    # Show tracker dataset summary
    if tracker_df is not None:
        print(f"\n📈 TRACKER DATASET SUMMARY:")
        print(f"Total metrics: {len(tracker_df)}")
        print(f"Date column: {TRACKER_DATE}")
        print(f"Sample metrics:")
        for _, row in tracker_df.head().iterrows():
            print(f"  {row['metric_name']}: {row[TRACKER_DATE]:,}")

if __name__ == "__main__":
    # Run all analytics
    results_df = run_all_analytics()
    
    # Create tracker dataset
    tracker_df = create_tracker_dataset(results_df)
    
    # Print summary
    print_results_summary(results_df, tracker_df)
    
    # Save results
    save_results(results_df, tracker_df)
    
    # Display the detailed results DataFrame
    print(f"\n📋 COMPLETE RESULTS DATAFRAME:")
    print(results_df.to_string(index=False))
    
    # Display the tracker dataset
    if tracker_df is not None:
        print(f"\n📈 TRACKER DATASET:")
        print(tracker_df.to_string(index=False)) 