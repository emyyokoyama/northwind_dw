with selected as (
    select 
        product_id
        , nome_produto
        , category_id
        , supplier_id
        , nome_categoria
        , descricao_categoria
        , units_in_stock
        , unit_price
        , quantity_per_unit
        , units_on_order
        , reorder_level
        , discontinued

    from {{ ref( 'stag_products' )}}
),
transformed as (
   select
      row_number() over (order by product_id) as product_sk
      , *
   from selected
)

select * from transformed