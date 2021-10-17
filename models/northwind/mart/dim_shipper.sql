{{ config( materialized='table') }}

with selected as (
    select	
        id_transportador
        , nome_tansportador
        , _sdc_sequence
        , _sdc_table_version
        , _sdc_received_at
        , _sdc_batched_at

    from {{ ref( 'stag_shipper' )}}
),
transformed as (
   select
      row_number() over (order by id_transportador) as shipper_sk
      , *
   from selected
)

select * from transformed