with source_data as (
    select
        customer_id as id_cliente
        , country as pais_cliente
        , city as cidade_cliente
        , company_name as nome_cliente
        , contact_title
    from {{ source('nortwind_etl', 'customers' )}}
)

select * from source_data
