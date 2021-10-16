with shipper_data as (
    select	
        shipper_id	as id_transportador
        , company_name as nome_tansportador
        , _sdc_sequence
        , _sdc_table_version
        , _sdc_received_at
        , _sdc_batched_at

    from {{ source('nortwind_etl', 'shippers' )}}
)

select * from shipper_data