{% set old_etl_relation=ref('customer_orders_practise') %} 

{% set dbt_relation=ref('fct_customer_orders_practise') %}  {{ 

audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="order_id"
    ) }}
