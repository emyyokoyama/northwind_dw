with source_data as (
    select
        order_id as id_pedido, 
        product_id, 
        discount, 
        unit_price, 
        quantity
    from {{ source('nortwind_etl', 'order_details' )}}
)

select * from source_data