{#
    Custom generic test: fails for any row whose timestamp is more than
    `grace_hours` in the future. Catches timezone bugs (UTC vs Adelaide time
    is a 9.5-10.5 hour difference, so a mix-up shows up immediately).

    Usage in yml:
      - not_in_future:
          grace_hours: 48
#}
{% test not_in_future(model, column_name, grace_hours=1) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} > dateadd(hour, {{ grace_hours }},
          convert_timezone('Australia/Adelaide', current_timestamp())::timestamp_ntz)
{% endtest %}
