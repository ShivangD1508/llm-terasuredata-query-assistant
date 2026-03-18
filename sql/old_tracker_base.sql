# SQL Queries for Joann Weekly Tracker

TRACKER_DATE = '2025-07-12'

queries = {
    "MIK_email_file_metrics": {
        "query_number": 1,
        "description": "MIK Email File Metrics",
        "sql": """
        with joann_all as (
          select
            lower(trim(e.email)) as email,
            max(to_date(c.lifetimelasttransdate)) as latest_transaction_date
          from 
            mk_src.joanns_customers c
          join 
            mk_src.joanns_emails e
            on c.customerid = e.customerid
          where 
            e.email is not null
          group by 
            lower(trim(e.email))
        ),

        mik_email_file as (
          select 
            lower(trim(email_addr_txt)) as email,
            max(engaged_1y_email_flag) as engaged_1y_email_flag, -- just in case we need it in future 
            max(to_date(from_unixtime(last_transaction_time_unixtime))) as latest_transaction_date
          from
            mk_gld.email_sms_loyalty
          where
            email_addr_txt is not null
          group by 
            lower(trim(email_addr_txt))
        )

        -- tracker metrics, rows 2-10
        select

          count(j.email) as total_joan_emails,

          sum(
            case when j.latest_transaction_date is not null then 1 else 0 end
          ) as joann_emails_w_joann_txns,

          sum(
            case when j.latest_transaction_date >= date '2023-01-01' then 1 else 0 end
          ) as joann_emails_w_post2023_joann_txns,

          sum(
            case when j.latest_transaction_date >= date '2024-01-01' then 1 else 0 end
          ) as joann_emails_w_post2024_joann_txns,

          sum(
            case when m.email is not null then 1 else 0 end
          ) as joann_emails_w_mik_email_file,

          sum(
            case when j.latest_transaction_date is not null
              and m.latest_transaction_date is not null then 1 else 0 end
          ) as joann_emails_w_joann_txns_w_mik_email_file,

          sum(
            case when j.latest_transaction_date >= date '2023-01-01'
              and m.email is not null then 1 else 0 end
          ) as joann_emails_w_joann_2yr_txns_w_mik_email_file,

          sum(
            case when j.latest_transaction_date >= date '2024-01-01'
              and m.email is not null then 1 else 0 end
          ) as joann_emails_w_joann_1yr_txns_w_mik_txns

        from 
          joann_all j
        left join
          mik_email_file m
          on j.email = m.email
        """
    },
    
    "MIK_transaction_metrics": {
        "query_number": 2,
        "description": "MIK Transaction Metrics",
        "sql": f"""
        with joann_all as (
          select
            lower(trim(e.email)) as email,
            max(to_date(c.lifetimelasttransdate)) as latest_transaction_date
          from 
            mk_src.joanns_customers c
          join 
            mk_src.joanns_emails e
            on c.customerid = e.customerid
          where 
            e.email is not null
          group by 
            lower(trim(e.email))
        ),

        mik_txns_all as (
          select distinct
            email_std as email,
            to_date(transaction_time) as transaction_date 
          from
            cdp_unification_mk.enrich_transactions_behaviour
          where
            transaction_time is not null  
            and email_std is not null
        )

        -- tracker metrics rows 11-19
        select
          count(distinct 
            case when j.latest_transaction_date is not null 
              and m.transaction_date <= date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_txns_w_mik_txns,

          count(distinct 
            case when j.latest_transaction_date >= date '2023-01-01'
              and m.transaction_date <= date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_2yr_txns_w_mik_txns,

          count(distinct 
            case when j.latest_transaction_date >= date '2024-01-01'
              and m.transaction_date <= date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_1yr_txns_w_mik_txns,

          count(distinct 
            case when j.latest_transaction_date is not null 
              and m.transaction_date between date '2023-01-01' and date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_txns_w_mik_2yr_txns,

          count(distinct 
            case when j.latest_transaction_date is not null 
              and m.transaction_date between date '2024-01-01' and date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_txns_w_mik_1yr_txns,

          count(distinct 
            case when j.latest_transaction_date >= date '2023-01-01'
              and m.transaction_date between date '2023-01-01' and date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_2yr_txns_w_mik_2yr_txns,

          count(distinct 
            case when j.latest_transaction_date >= date '2024-01-01'
              and m.transaction_date between date '2024-01-01' and date '{TRACKER_DATE}' then j.email else null end
          ) as joann_emails_w_joann_1yr_txns_w_mik_1yr_txns,

          count(distinct 
            case when j.latest_transaction_date >= date '2023-01-01'
              and m.transaction_date is null then j.email else null end
          ) as joann_emails_w_joann_2yr_txns_w_no_mik_txns,

          count(distinct 
            case when j.latest_transaction_date >= date '2024-01-01'
              and m.transaction_date is null then j.email else null end
          ) as joann_emails_w_joann_1yr_txns_w_no_mik_txns
        from 
          joann_all j
        left join -- this is a one-to-many join
          mik_txns_all m
          on j.email = m.email
        """
    },
    
    "MIK_retention_metric": {
        "query_number": 3,
        "description": "MIK Retention Metric",
        "weekly_tracker": False,
        "sql": """
        with joann_cust_2yr as (
          select distinct je.email 
          from mk_src.joanns_customers c
          join mk_src.joanns_emails je
            on c.customerid = je.customerid
          where 
            je.email is not null
            and c.lifetimeLastTransDate is not null
            and c.lifetimeLastTransDate >= '2023-01-01'
        ),
        mik_pre_april2025 as (
          select distinct t.email
          from cdp_unification_mk.enrich_transactions_behaviour t
          where t.transaction_time between '2023-01-01' and '2025-04-30'
        ),
        mik_post_april2025 as (
          select distinct t.email
          from cdp_unification_mk.enrich_transactions_behaviour t
          where t.transaction_time between '2025-05-01' and '{TRACKER_DATE}'
        )
        -- tracker metric rows 20
        select count(distinct j.email) as mik_retained
        from joann_cust_2yr j
        join mik_pre_april2025 pre
        on j.email = pre.email
        join mik_post_april2025 post 
        on j.email = post.email 
        where pre.email is not null and post.email is not null 
        """
    },
    
    "MIK_reactivation_metric": {
        "query_number": 4,
        "description": "MIK Reactivation Metric",
        "weekly_tracker": False,
        "sql": f"""
        with joann_cust_2yr as (
          select distinct je.email
          from mk_src.joanns_customers c
          join mk_src.joanns_emails je
          on c.customerid = je.customerid
          where je.email is not null
            and c.lifetimeLastTransDate is not null
            and c.lifetimeLastTransDate >= '2023-01-01'
        ),
        mik_2023_to_apr2025 as (
          select distinct t.email
          from cdp_unification_mk.enrich_transactions_behaviour t
          where t.transaction_time between '2023-01-01' and '2025-04-30'
        ),
        mik_since_may2025 as (
          select distinct t.email
          from cdp_unification_mk.enrich_transactions_behaviour t
          where t.transaction_time between '2025-05-01' and '{TRACKER_DATE}'
        )
        -- tracker metric rows 21
        select count(distinct j.email) as mik_reactivated
        from joann_cust_2yr j
        left join mik_since_may2025 mik_post
        on j.email = mik_post.email
        left join mik_2023_to_apr2025 mik_gap
        on j.email = mik_gap.email
        where mik_gap.email is null 
          and mik_post.email is not null
        """
    }
}