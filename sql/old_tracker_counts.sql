-- Channel Metrics Calculation SQL
-- This script calculates channel metrics for a given dataset segment
-- Based on the calculate_channel_metrics() function from run_new_tracker.py
-- This should be run on the output CSV file from new_tracker.sql

-- Set the tracker date variable - change this value to run for different dates
SET tracker_date = '2025-06-07';

-- This query should be run on the output CSV file from new_tracker.sql
-- Replace 'your_input_csv_file' with the actual CSV file path
WITH summary_metrics AS (
  SELECT
    -- Total count
    COUNT(*) as marketables,
    
    -- Emailable: valid_email = True AND email_subscriber = True
    COUNT(CASE 
      WHEN valid_email = True AND email_subscriber = True 
      THEN 1 
      ELSE NULL 
    END) as valid_email_subscribers,
    
    -- Valid email but not subscriber and 2-year shopper
    COUNT(CASE 
      WHEN valid_email = True 
        AND email_subscriber = False 
        AND joann_2yr_shopper = True
      THEN 1 
      ELSE NULL 
    END) as valid_email_non_subscribers_2yr_shopper,

    -- Valid email but not subscriber and 2-year shopper and phone only
    COUNT(CASE 
      WHEN valid_email = True 
        AND email_subscriber = False 
        AND joann_2yr_shopper = True
        AND valid_phone = True
        AND valid_dm = False
      THEN 1 
      ELSE NULL 
    END) as valid_email_non_subscribers_2yr_shopper_phone_only,

    -- Valid email but not subscriber and 2-year shopper and DM only
    COUNT(CASE 
      WHEN valid_email = True 
        AND email_subscriber = False 
        AND joann_2yr_shopper = True
        AND valid_phone = False
        AND valid_dm = True
      THEN 1 
      ELSE NULL 
    END) as valid_email_non_subscribers_2yr_shopper_dm_only,

    -- Valid email but not subscriber and 2-year shopper and phone and DM
    COUNT(CASE 
      WHEN valid_email = True 
        AND email_subscriber = False 
        AND joann_2yr_shopper = True
        AND valid_phone = True
        AND valid_dm = True
      THEN 1 
      ELSE NULL 
    END) as valid_email_non_subscribers_2yr_shopper_phone_and_dm,

    -- Marketable and MIK non-shopper
    COUNT(CASE 
      WHEN marketable_flag = True 
        AND mik_non_shopper = True
      THEN 1 
      ELSE NULL 
    END) as marketable_and_mik_non_shoppers,

    -- Marketable and MIK active shopper
    COUNT(CASE 
      WHEN marketable_flag = True 
        AND mik_active_shopper = True
      THEN 1 
      ELSE NULL 
    END) as marketable_and_mik_active_shoppers,

    -- Marketable and MIK lapsed shopper
    COUNT(CASE 
      WHEN marketable_flag = True 
        AND mik_lapsed_shopper = True
      THEN 1 
      ELSE NULL 
    END) as marketable_and_mik_lapsed_shoppers
  FROM your_input_csv_file  -- Replace with actual CSV file path
  WHERE 1=1  -- Add any additional filtering conditions here
)
SELECT * FROM summary_metrics;