{% test check_sum(model, column_name, id, prod) %}

    with cte as (
        select sum({{column_name}}) as amount from {{model}}
        union
        select sum({{column_name}}) as amount from {{prod}}
    )
    select  sum(amount)
    from    cte
    having  count(*) > 1

{% endtest %}