-- Channel mix (card-present chip/swipe vs card-not-present online): volume,
-- fraud rate, and cross-border share per channel.

with stg as (
    select * from {{ ref('stg_transactions') }}
),

grouped as (
    select
        channel,
        count(*) as txn_count,
        sum(case when is_fraud then 1 else 0 end) as fraud_count,
        sum(case when merchant_country not in ('US', 'XX') then 1 else 0 end) as cross_border_count
    from stg
    group by 1
)

select
    channel,
    txn_count,
    fraud_count,
    case when txn_count > 0 then fraud_count::double / txn_count else 0.0 end as fraud_rate,
    case when txn_count > 0 then cross_border_count::double / txn_count else 0.0 end as cross_border_share
from grouped
order by txn_count desc
