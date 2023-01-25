-- Here, we're taking distinct values from a column and looping over these distinct values 

{% set payment_methods_query %}
select distinct payment_method from {{ ref('stg_payments') }}
{% endset %}

{% set results = run_query(payment_methods_query) %}

{% if execute %}
{% set payment_methods = results.columns[0].values() %}
{% endif %}

with payments as (
     select * from {{ ref('stg_payments') }}
 ),

pivoted as (
    
    select 
        order_id,
        {% for payment_method in payment_methods %}
        sum(case when payment_method = '{{payment_method}}' then amount else 0 end) as {{payment_method}}_amount,
        {% endfor %}
    from 
        payments
    where status = 'success'
    group by 1
)

select * from pivoted 
