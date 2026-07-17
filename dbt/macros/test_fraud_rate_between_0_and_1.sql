{#
  Generic test: fails if any row of `column_name` on `model` falls outside
  the closed interval [0, 1] — a fraud/cross-border rate is a proportion and
  must never leave that range. Returns the offending rows (dbt tests fail
  when the compiled query returns any rows).
#}
{% test fraud_rate_between_0_and_1(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0 or {{ column_name }} > 1 or {{ column_name }} is null

{% endtest %}
