-- Merchant-risk rollup by coarse spend category (mcc_group, mirrors the
-- model's mcc_group_id feature — see macros/mcc_group.sql) x merchant_state.

with stg as (
    select
        *,
        {{ mcc_group('mcc') }} as mcc_group
    from {{ ref('stg_transactions') }}
),

grouped as (
    select
        mcc_group,
        merchant_state,
        count(*) as txn_count,
        sum(case when is_fraud then 1 else 0 end) as fraud_count,
        avg(amount) as avg_amount
    from stg
    group by 1, 2
)

select
    mcc_group,
    merchant_state,
    txn_count,
    fraud_count,
    case when txn_count > 0 then fraud_count::double / txn_count else 0.0 end as fraud_rate,
    avg_amount
from grouped
order by txn_count desc
