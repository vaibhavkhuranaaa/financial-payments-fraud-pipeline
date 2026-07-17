{#
  Generates a SQL CASE expression mirroring
  src/pipeline/ingestion.py::COUNTRY_NAME_TO_ISO2 — the raw TabFormer
  "Merchant State" country-name strings observed for non-US, non-online
  transactions, mapped to ISO-3166 alpha-2. Kept in sync by hand with the
  Python dict; see the header comment in stg_transactions.sql.
#}
{% macro country_name_to_iso2(expr) %}
  case {{ expr }}
    when 'Italy' then 'IT'
    when 'Mexico' then 'MX'
    when 'France' then 'FR'
    when 'Bangladesh' then 'BD'
    when 'Norway' then 'NO'
    when 'Malaysia' then 'MY'
    when 'Greece' then 'GR'
    when 'Netherlands' then 'NL'
    when 'Japan' then 'JP'
    when 'Spain' then 'ES'
    when 'Czech Republic' then 'CZ'
    when 'Thailand' then 'TH'
    when 'China' then 'CN'
    when 'Canada' then 'CA'
    when 'Germany' then 'DE'
    when 'United Kingdom' then 'GB'
    when 'Portugal' then 'PT'
    when 'Switzerland' then 'CH'
    when 'Sweden' then 'SE'
    when 'Poland' then 'PL'
    when 'Austria' then 'AT'
    when 'Belgium' then 'BE'
    when 'Ireland' then 'IE'
    when 'India' then 'IN'
    when 'South Korea' then 'KR'
    when 'Vietnam' then 'VN'
    when 'Philippines' then 'PH'
    when 'Indonesia' then 'ID'
    when 'Australia' then 'AU'
    when 'Brazil' then 'BR'
    when 'Argentina' then 'AR'
    when 'South Africa' then 'ZA'
    else null
  end
{% endmacro %}
