with
    customers as (
        select *
        from {{ ref( 'dim_customer' )}}
    ),
    products as (
        select *
        from {{ ref( 'dim_products' )}}
    ),
    shippers as (
        select *
        from {{ ref( 'dim_shipper' )}}
    ),
    suppliers as (
        select *
        from {{ ref( 'dim_supplier' )}}
    ),
    orders_sk as (
        select
            orders.id_pedido,
            customers.customer_sk as customer_fk,
            id_funcionario,
            shippers.shipper_sk as shipper_fk,  
            orders.data_pedido,
            orders.regiao_entrega,
            orders.data_expedicao,
            orders.pais_entrega,
            orders.nome_entrega,
            orders.cep_entrega,
            orders.cidade_entrega,
            orders.frete,
            orders.endereco_entrega,
            orders.data_prevista,
            orders.quantidade_total,
            orders.valor_faturado,
            orders.quantidade_itens
         from {{ ref( 'stg_orders' )}} as orders
         left join customers on orders.id_cliente=customers.id_cliente
         left join shippers on orders.id_transportador=shippers.id_transportador
                 
    ),

    order_detail_sk as(
        select
            id_pedido, 
            products.product_sk as product_fk,
            order_detail.discount,
            order_detail.unit_price,
            order_detail.quantity

            from {{ ref( 'stag_order_detail' )}} as order_detail
            left join products on order_detail.product_id=products.product_id
    ), 
    final as (
        select
             order_detail_sk.id_pedido,
             orders_sk.customer_fk,
             orders_sk.id_funcionario,
             orders_sk.shipper_fk,  
             orders_sk.data_pedido,
             orders_sk.regiao_entrega,
             orders_sk.data_expedicao,
             orders_sk.pais_entrega,
             orders_sk.nome_entrega,
             orders_sk.cep_entrega,
             orders_sk.cidade_entrega,
             orders_sk.frete,
             orders_sk.endereco_entrega,
             orders_sk.data_prevista,
             order_detail_sk.product_fk,
             order_detail_sk.discount,
             order_detail_sk.unit_price,
             order_detail_sk.quantity
        from orders_sk
        left join order_detail_sk on orders_sk.id_pedido=order_detail_sk.id_pedido
    )

select * from final


