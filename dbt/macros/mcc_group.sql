{#
  Generates a SQL CASE expression mirroring
  src/pipeline/features.py::_mcc_group / _MCC_EXACT_GROUPS / MCC_GROUP_IDS —
  the same coarse merchant-category grouping used by the online/offline
  feature builders, so fct_merchant_risk's `mcc_group` lines up with the
  model's `mcc_group_id` feature. Kept in sync by hand; see
  docs/governance/lineage.md.
#}
{% macro mcc_group(expr) %}
  case
    when {{ expr }} in (4111, 4112, 4121, 4131, 4411, 4511) then 'travel'
    when {{ expr }} in (5411, 5422, 5451, 5499) then 'grocery'
    when {{ expr }} in (6010, 6011, 6012, 4829) then 'cash'
    when {{ expr }} in (5310, 5311, 5300, 5964, 5965, 5966, 5967, 5968, 5969) then 'online_retail'
    when {{ expr }} between 3000 and 3999 then 'travel'
    else 'other'
  end
{% endmacro %}
