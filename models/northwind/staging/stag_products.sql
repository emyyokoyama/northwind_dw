with products_data as (
    select
        product_id	
        , units_in_stock
        , category_id
        , unit_price
        /* Stich colums com esses sdc */
        , _sdc_table_version
        , product_name as nome_produto
        , quantity_per_unit
        , _sdc_received_at
        , supplier_id
        , _sdc_sequence
        , units_on_order
        , discontinued
        , _sdc_batched_at
        , reorder_level
    from {{ source('nortwind_etl', 'products' )}}
),
categories_data as (
    select
        category_id
        , category_name	as nome_categoria
        , description as descricao_categoria
        , _sdc_sequence	
        , _sdc_table_version	
        , _sdc_received_at	
        , _sdc_batched_at	 
    from {{ source('nortwind_etl', 'categories' )}}
),
produtos_categorias as (
    select 
        product_id
        , nome_produto
        , products_data.category_id
        , supplier_id
        , nome_categoria
        , descricao_categoria
        , units_in_stock
        , unit_price
        , quantity_per_unit
        , units_on_order
        , reorder_level
        , discontinued
        /* Stich colums com esses sdc */
        , products_data._sdc_table_version
        , products_data._sdc_received_at
        , products_data._sdc_sequence
        , products_data._sdc_batched_at

    from products_data
    left join categories_data on products_data.category_id = categories_data.category_id
)

select * from produtos_categorias