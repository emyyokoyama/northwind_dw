with supplier_data as (
    select	
        supplier_id	
        , country as pais_fornecedor
        , city as cidade_fornecedor
        , _sdc_table_version	
        , region as regiao_fornecedor
        , _sdc_received_at
        , _sdc_sequence	
        , contact_name
        , company_name as nome_fornecedor
        , contact_title
        , _sdc_batched_at	
    from {{ source('nortwind_etl', 'suppliers' )}}
)

select * from supplier_data