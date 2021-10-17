{{ config( materialized='table') }}

with selected as (
    select
        id_cliente
        , pais_cliente
        , cidade_cliente
        , nome_cliente
        , contact_title
    from {{ ref( 'stag_customers' )}}
),
transformed as (
   select
      row_number() over (order by id_cliente) as customer_sk
      , *
   from selected
)

select * from transformed