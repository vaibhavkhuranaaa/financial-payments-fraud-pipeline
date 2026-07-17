-- Daily fraud-ops rollup: volume, fraud count/rate, gross settled amount.
--
-- `gross_amount` sums only positive amounts (per contracts/transaction.schema.json,
-- negative amounts are reversals/credits and would understate true gross spend
-- if netted in).

with stg as (
    select * from {{ ref('stg_transactions') }}
),

daily as (
    select
        cast(event_time as date) as txn_date,
        count(*) as txn_count,
        sum(case when is_fraud then 1 else 0 end) as fraud_count,
        sum(case when amount > 0 then amount else 0 end) as gross_amount
    from stg
    group by 1
)

select
    txn_date,
    txn_count,
    fraud_count,
    case when txn_count > 0 then fraud_count::double / txn_count else 0.0 end as fraud_rate,
    gross_amount
from daily
order by txn_date
