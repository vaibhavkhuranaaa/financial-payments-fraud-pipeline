-- Analytical mirror of contracts/transaction.schema.json.
--
-- This model re-derives, in SQL, exactly what
-- src/pipeline/ingestion.py::to_event() computes in Python for the same raw
-- TabFormer CSV rows: card tokenization (salted SHA-256), channel renaming,
-- amount string parsing, merchant_state/merchant_country derivation, and the
-- Yes/No -> boolean is_fraud mapping. Keeping the two in sync by hand (rather
-- than via a shared library, since dbt-duckdb SQL can't import Python) is a
-- known limitation — see docs/governance/lineage.md and the README's
-- "What I'd Improve Next".
--
-- Reads the same sample CSV the local docker-compose producer replays, so
-- `dbt build` is runnable in CI without any live Kafka/Spark/Delta
-- dependency (see docs/tickets/04-analytics-docs.md).

with source as (
    select *
    from read_csv_auto('{{ var("sample_csv_path") }}', header = true)
),

renamed as (
    select
        "User" as user_id_raw,
        "Card" as card_id_raw,
        "Year" as txn_year,
        "Month" as txn_month,
        "Day" as txn_day,
        "Time" as txn_time,
        "Amount" as amount_raw,
        "Use Chip" as use_chip_raw,
        "Merchant Name" as merchant_name,
        "Merchant City" as merchant_city,
        "Merchant State" as merchant_state_raw,
        "Zip" as zip_raw,
        "MCC" as mcc,
        "Errors?" as errors_raw,
        "Is Fraud?" as is_fraud_raw
    from source
),

mapped as (
    select
        -- Deterministic per-row id (source has no natural event_id; the
        -- producer mints a fresh uuid4 per replayed event instead).
        row_number() over () as event_id,
        '1.0.0' as schema_version,

        -- card_token: SHA-256(salt || user_id:card_id) — same construction as
        -- ingestion.py::_card_token, so tokens computed here are byte-for-byte
        -- identical to what the streaming producer emits for this sample.
        sha256(
            '{{ var("tokenization_salt") }}' || ':' || cast(user_id_raw as varchar) || ':' || cast(card_id_raw as varchar)
        ) as card_token,
        cast(user_id_raw as varchar) as user_id,

        make_timestamp(
            txn_year, txn_month, txn_day,
            cast(split_part(cast(txn_time as varchar), ':', 1) as integer),
            cast(split_part(cast(txn_time as varchar), ':', 2) as integer),
            0
        ) as event_time,

        cast(replace(replace(amount_raw, '$', ''), ',', '') as double) as amount,
        'USD' as currency,

        case use_chip_raw
            when 'Chip Transaction' then 'chip'
            when 'Swipe Transaction' then 'swipe'
            when 'Online Transaction' then 'online'
            else 'online'
        end as channel,

        merchant_name,
        merchant_city,

        case
            when use_chip_raw = 'Online Transaction' then 'ONLINE'
            when trim(coalesce(merchant_state_raw, '')) <> '' then trim(merchant_state_raw)
            else 'XX'
        end as merchant_state,

        case
            when use_chip_raw = 'Online Transaction' then 'XX'
            when trim(coalesce(merchant_state_raw, '')) = '' then 'XX'
            when upper(trim(merchant_state_raw)) in (
                'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL',
                'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT',
                'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
                'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
                'DC','PR','VI','GU','AS','MP'
            ) then 'US'
            else coalesce(
                {{ country_name_to_iso2('trim(merchant_state_raw)') }},
                'XX'
            )
        end as merchant_country,

        case
            when zip_raw is null or trim(cast(zip_raw as varchar)) in ('', 'nan') then null
            else cast(cast(zip_raw as double) as bigint)::varchar
        end as zip,

        mcc,
        nullif(trim(coalesce(errors_raw, '')), '') as errors,

        -- read_csv_auto may sniff Yes/No into a native BOOLEAN, so the cast
        -- yields 'true'/'false' — accept both source representations.
        case
            when lower(trim(coalesce(cast(is_fraud_raw as varchar), ''))) in ('yes', 'true') then true
            when lower(trim(coalesce(cast(is_fraud_raw as varchar), ''))) in ('no', 'false') then false
            else null
        end as is_fraud
    from renamed
)

select * from mapped
