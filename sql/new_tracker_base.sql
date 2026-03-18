
-- This query creates a table (or inserts records) with one record per email for marketable customers
-- The CREATE TABLE query should be run only once, with 2025-06-07 as the tracker_date, to create the initial table 
-- After the table is created, the INSERT INTO statementshould be run for each tracker_date 
-- The tracker_date needs to be changed at FOUR different places before running the INSERT INTO query

CREATE TABLE reporting.joann_weekly_tracker_marketable_universe AS
--INSERT INTO reporting.joann_weekly_tracker_marketable_universe
WITH joann_email_available AS (
  SELECT lower(trim(email)) as email,
    max(emailSubscribeStatus) as emailSubscribeStatus,
    max(emailvalidstatus) as emailvalidstatus
  FROM mk_src.joanns_emails
  WHERE email is not null
  GROUP BY email 
),
joann_phone_available AS (
  SELECT DISTINCT lower(trim(e.email)) as email
  FROM mk_src.joanns_customers c 
  JOIN mk_src.joanns_emails e
    ON c.customerid = e.customerid
  WHERE e.email is not null
    AND c.mobilephone is not null
),
joann_dm_eligible AS (
  SELECT DISTINCT lower(trim(e.email)) as email
  FROM mk_src.joanns_customers c 
  JOIN mk_src.joanns_emails e
    ON c.customerid = e.customerid
  WHERE e.email is not null
    AND c.address is not null
    AND c.maileligible='Y'
),
joann_marketable AS (
  SELECT 
    coalesce(m.email, p.email, d.email) as email,
    CASE WHEN m.email is not null
      AND m.emailvalidstatus='1' THEN 1 ELSE 0 END as valid_email,
    CASE WHEN m.email is not null
      AND m.emailSubscribeStatus='subscribed' THEN 1 ELSE 0 END as email_subscriber,
    CASE WHEN p.email is not null THEN 1 ELSE 0 END as valid_phone,
    CASE WHEN d.email is not null THEN 1 ELSE 0 END as valid_dm
  FROM joann_email_available m
  FULL OUTER JOIN joann_phone_available p 
    ON m.email = p.email
  FULL OUTER JOIN joann_dm_eligible d
    ON coalesce(m.email, p.email) = d.email
),
joann_2yr_shoppers AS (
  SELECT DISTINCT lower(trim(e.email)) as email
  FROM mk_src.joanns_customers c 
  JOIN mk_src.joanns_emails e
    ON c.customerid = e.customerid
  WHERE e.email is not null 
    AND c.lifetimeLastTransDate >= '2023-01-01'
),
mik_all_shoppers_asof_tracker_date AS (
  SELECT DISTINCT email_std as email
  FROM cdp_unification_mk.enrich_transactions_behaviour
  WHERE email_std is not null
    AND transaction_time is not null
    AND transaction_time <= '2025-06-07'  -- Change this date as needed
),
mik_active_shoppers_asof_tracker_date AS (
  SELECT DISTINCT email_std as email
  FROM cdp_unification_mk.enrich_transactions_behaviour
  WHERE email_std is not null
    AND transaction_time is not null
    AND transaction_time BETWEEN '2023-01-01' AND '2025-06-07'  -- Change this date as needed
),
mik_lapsed_shoppers AS (
  SELECT email_std as email
  FROM cdp_unification_mk.enrich_transactions_behaviour
  WHERE email_std is not null
    AND transaction_time is not null
    AND transaction_time <= '2025-06-07'  -- Change this date as needed
  GROUP BY email_std
  HAVING max(transaction_time) < '2023-01-01'
),
joann_email_universe AS (
  SELECT
    -- Joann marketable
    jm.email,
    jm.valid_email,
    jm.email_subscriber,
    jm.valid_phone,
    jm.valid_dm,
    -- Joann 2-year shopper
    CASE WHEN j2y.email is not null THEN 1 ELSE 0 END as joann_2yr_shopper,
    -- Marketable (unification) flag
    CASE WHEN
      (jm.valid_email = 1 AND jm.email_subscriber = 1) OR
      (jm.valid_email = 1 AND jm.email_subscriber = 0 AND j2y.email is not null AND
      (jm.valid_phone = 1 OR jm.valid_dm = 1))
    THEN 1 ELSE 0 END as marketable_flag,
    CASE 
      WHEN mall.email is null THEN 'MIK NON-SHOPPER'
      WHEN mas.email is not null THEN 'MIK ACTIVE SHOPPER'
      WHEN mls.email is not null THEN 'MIK LAPSED SHOPPER'
      ELSE 'UNCLASSIFIED'
    END as mik_status,
    -- Tracker date as string
    '2025-06-07' as tracker_date  -- Change this date as needed
  FROM joann_marketable jm
  LEFT JOIN mik_active_shoppers_asof_tracker_date mas
    ON jm.email = mas.email
  LEFT JOIN mik_lapsed_shoppers mls
    ON jm.email = mls.email
  LEFT JOIN mik_all_shoppers_asof_tracker_date mall
    ON jm.email = mall.email
  LEFT JOIN joann_2yr_shoppers j2y
    ON jm.email = j2y.email
)
SELECT * FROM joann_email_universe 
WHERE marketable_flag = 1;