-- Find Michaels customers not in Joann tracker and calculate their Joann sales
-- This query identifies Michaels customers who are not in the Joann marketable universe
-- and calculates their Joann department sales between 6/7/2025 and 8/2/2025

WITH michaels_customers AS (
  SELECT DISTINCT 
    lower(trim(email_addr_txt)) as email
  FROM mk_gld.email_sms_loyalty
  WHERE email_addr_txt IS NOT NULL
    AND email_addr_txt != ''
),
joann_tracker_customers AS (
  SELECT DISTINCT 
    email
  FROM reporting.joann_weekly_tracker_marketable_universe
  WHERE tracker_date = '2025-06-07'
),
michaels_not_in_joann AS (
  SELECT 
    mc.email
  FROM michaels_customers mc
  LEFT JOIN joann_tracker_customers jtc
    ON mc.email = jtc.email
  WHERE jtc.email IS NULL  -- Only Michaels customers NOT in Joann tracker
),
joann_sales_for_michaels_customers AS (
  SELECT 
    mni.email,
    SUM(extended_sls_amt) as total_sales_amt,
    SUM(CASE WHEN dept_name IN ('stitchery', 'yarn') THEN extended_sls_amt ELSE 0 END) as joann_dept_sales_amt
  FROM michaels_not_in_joann mni
  LEFT JOIN cdp_unification_mk.enrich_transactions_behaviour etb
    ON mni.email = etb.email_std
    AND etb.transaction_time > '2025-06-07'
    AND etb.transaction_time <= '2025-08-02'
    AND etb.email_std IS NOT NULL
  GROUP BY mni.email
)
SELECT 
  COUNT(DISTINCT mni.email) as total_michaels_customers_not_in_joann,
  COUNT(CASE WHEN js.joann_dept_sales_amt > 0 THEN js.email END) as customers_with_joann_sales,
  SUM(COALESCE(js.total_sales_amt, 0)) as total_sales_amount,
  SUM(COALESCE(js.joann_dept_sales_amt, 0)) as total_joann_dept_sales_amount,
  AVG(COALESCE(js.total_sales_amt, 0)) as avg_total_sales_per_customer,
  AVG(COALESCE(js.joann_dept_sales_amt, 0)) as avg_joann_dept_sales_per_customer
FROM michaels_not_in_joann mni
LEFT JOIN joann_sales_for_michaels_customers js
  ON mni.email = js.email; 