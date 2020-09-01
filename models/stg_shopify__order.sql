with source as (

    select * from {{ source('shopify', 'order') }}

),

renamed as (

    select
        id,
        note,
        email,
        taxes_included,
        currency,
        subtotal_price,
        total_tax,
        total_price,
        created_at,
        updated_at,
        name,
        shipping_address_name,
        shipping_address_first_name,
        shipping_address_last_name,
        shipping_address_company,
        shipping_address_phone,
        shipping_address_address_1,
        shipping_address_address_2,
        shipping_address_city,
        shipping_address_country,
        shipping_address_country_code,
        shipping_address_province,
        shipping_address_province_code,
        shipping_address_zip,
        shipping_address_latitude,
        shipping_address_longitude,
        billing_address_name,
        billing_address_first_name,
        billing_address_last_name,
        billing_address_company,
        billing_address_phone,
        billing_address_address_1,
        billing_address_address_2,
        billing_address_city,
        billing_address_country,
        billing_address_country_code,
        billing_address_province,
        billing_address_province_code,
        billing_address_zip,
        billing_address_latitude,
        billing_address_longitude,
        customer_id,
        location_id,
        user_id,
        number,
        order_number,
        financial_status,
        fulfillment_status,
        processed_at,
        processing_method,
        referring_site,
        cancel_reason,
        cancelled_at,
        closed_at,
        total_discounts,
        total_line_items_price,
        total_weight,
        source_name,
        browser_ip,
        buyer_accepts_marketing,
        token,
        cart_token,
        checkout_token,
        test,
        landing_site_base_url,
        _fivetran_synced

    from source

)

select * from renamed