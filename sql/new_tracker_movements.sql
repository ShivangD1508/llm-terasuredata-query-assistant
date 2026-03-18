-- Customer movement and transactions for Joann New Tracker
-- This query identifies customer movement between tracker dates and includes transaction details

CREATE TABLE reporting.joann_weekly_tracker_movements_and_sales AS
--INSERT INTO reporting.joann_weekly_tracker_movements_and_sales
WITH customer_movement AS (
  SELECT 
    -- Base customer info from 6/7 dataset
    coalesce(t1.email, t2.email) as email,

    t1.mik_status as mik_status_pre,
    t2.mik_status as mik_status_post,
    t2.tracker_date as tracker_date_post,
    t1.tracker_date as tracker_date_pre
    
  FROM reporting.joann_weekly_tracker_marketable_universe t1
  JOIN reporting.joann_weekly_tracker_marketable_universe t2
    ON t1.email = t2.email
    AND t1.tracker_date = '2025-06-07' -- do not change this date
    AND t2.tracker_date = '2025-06-14' -- change this date for each run
),
transaction_data AS (
  SELECT 
    email_std, 
    sum(extended_sls_amt) as total_sales_amt,
    sum(CASE WHEN dept_name IN ('stitchery', 'yarn') THEN extended_sls_amt ELSE 0 END) as joann_dept_sales_amt 
  FROM cdp_unification_mk.enrich_transactions_behaviour 
  WHERE transaction_time > '2025-06-07' -- do not change this date
    AND transaction_time <= '2025-06-14' -- change this date for each run
    AND email_std IS NOT NULL
  GROUP BY 
    email_std 
  --HAVING sum(extended_sls_amt) > 0
)
  SELECT 
    cm.*,
    -- Exhaustive customer movement categorization
    CASE 
      WHEN cm.mik_status_pre = 'MIK NON-SHOPPER' 
        AND cm.mik_status_post = 'MIK ACTIVE SHOPPER' 
      THEN 'ACQUIRED: NON-SHOPPER'
      
      WHEN cm.mik_status_pre = 'MIK LAPSED SHOPPER' 
        AND cm.mik_status_post = 'MIK ACTIVE SHOPPER' 
      THEN 'ACQUIRED: LAPSED SHOPPER'
      
      WHEN cm.mik_status_pre = 'MIK ACTIVE SHOPPER' 
        AND td.email_std IS NOT NULL 
      THEN 'RETAINED: ACTIVE SHOPPER (TRANSACTED)'
      
      ELSE 'UNMOVED'
    END as customer_movement_category,
  td.total_sales_amt,
  case when td.total_sales_amt > 0 then 1 else 0 end as transacted_w_revenue_post,
  td.joann_dept_sales_amt,
  case when td.joann_dept_sales_amt > 0 then 1 else 0 end as joann_dept_sales_post,
  -- Tracker dates used in analysis
  cm.tracker_date_pre as pre_tracker_date,
  cm.tracker_date_post as post_tracker_date
FROM customer_movement cm
LEFT JOIN transaction_data td
  ON cm.email = td.email_std