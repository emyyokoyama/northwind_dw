{{ config( materialized='table') }}

with selected as (
    select	
        supplier_id	
        , pais_fornecedor
        , cidade_fornecedor
        , _sdc_table_version	
        , regiao_fornecedor
        , _sdc_received_at
        , _sdc_sequence	
        , contact_name
        , nome_fornecedor
        , contact_title
        , _sdc_batched_at	
    from {{ ref( 'stag_suppliers' )}}
),
transformed as (
   select
      row_number() over (order by supplier_id) as supplier_sk
      , *
   from selected
)

select * from transformed