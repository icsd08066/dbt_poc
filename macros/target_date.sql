{%macro target_date()%}

with max_dates as (
    select
        max(utc_date) AS max_stored_date,
        date_sub(current_date,{{ var('numOfDaysToOverwrite') }}) as target_date_to_overwrite
    from {{ source('kaizen_wars', 'FACT_Pandora_FreeBetToken') }}
)

select
    case
        when max_stored_date > target_date_to_overwrite then target_date_to_overwrite
        else max_stored_date
    end as target_date
FROM max_dates

{% endmacro %}