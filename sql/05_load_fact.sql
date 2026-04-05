insert into fact_sales (
    sale_source_row_id,
    sale_date,
    customer_key,
    pet_key,
    seller_key,
    product_key,
    store_key,
    supplier_key,
    sale_quantity,
    sale_total_price
)
select
    m.id,
    m.sale_date,
    c.customer_key,
    p.pet_key,
    s.seller_key,
    pr.product_key,
    st.store_key,
    sp.supplier_key,
    m.sale_quantity,
    m.sale_total_price
from mock_data m
left join dim_customer c
    on c.customer_source_id = m.sale_customer_id
left join dim_pet p
    on p.customer_source_id = m.sale_customer_id
   and coalesce(p.pet_name, '') = coalesce(m.customer_pet_name, '')
   and coalesce(p.pet_type, '') = coalesce(m.customer_pet_type, '')
   and coalesce(p.pet_breed, '') = coalesce(m.customer_pet_breed, '')
left join dim_seller s
    on s.seller_source_id = m.sale_seller_id
left join dim_product pr
    on pr.product_source_id = m.sale_product_id
left join dim_store st
    on coalesce(st.store_name, '') = coalesce(m.store_name, '')
   and coalesce(st.store_email, '') = coalesce(m.store_email, '')
   and coalesce(st.store_phone, '') = coalesce(m.store_phone, '')
left join dim_supplier sp
    on coalesce(sp.supplier_name, '') = coalesce(m.supplier_name, '')
   and coalesce(sp.supplier_email, '') = coalesce(m.supplier_email, '')
   and coalesce(sp.supplier_phone, '') = coalesce(m.supplier_phone, '');

