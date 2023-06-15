with base as (

    select * 
    from {{ ref('stg_shopify__order_tmp') }}

),

order_line_agg as (
    select 
        order_id,
        sum(price) as order_price
    from {{ ref('stg_shopify__order_line') }}
    group by order_id
),

fields as (

    select
    
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_shopify__order_tmp')),
                staging_columns=get_order_columns()
            )
        }}

        {{ fivetran_utils.source_relation(
            union_schema_variable='shopify_union_schemas', 
            union_database_variable='shopify_union_databases') 
        }}

    from base

),

final as (

    select 
        fields.id as order_id,
        fields.user_id,
        fields.total_discounts,
        order_line_agg.order_price as total_line_items_price,
        order_line_agg.order_price as total_price,
        order_line_agg.order_price as total_price_usd,
        fields.total_tax,
        fields.source_name,
        order_line_agg.order_price as subtotal_price,
        fields.taxes_included as has_taxes_included,
        fields.total_weight,
        fields.total_tip_received,
        fields.landing_site_base_url,
        fields.location_id,
        fields.name,
        fields.note,
        fields.number,
        fields.order_number,
        fields.cancel_reason,
        fields.cart_token,
        fields.checkout_token,
        {{ dbt_date.convert_timezone(column='cast(created_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as created_timestamp,
        {{ dbt_date.convert_timezone(column='cast(cancelled_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as cancelled_timestamp,
        {{ dbt_date.convert_timezone(column='cast(closed_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as closed_timestamp,
        {{ dbt_date.convert_timezone(column='cast(processed_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as processed_timestamp,
        {{ dbt_date.convert_timezone(column='cast(updated_at as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as updated_timestamp,
        fields.currency,
        fields.customer_id,
        lower(fields.email) as email,
        fields.financial_status,
        fields.fulfillment_status,
        fields.processing_method,
        fields.referring_site,
        fields.billing_address_address_1,
        fields.billing_address_address_2,
        fields.billing_address_city,
        fields.billing_address_company,
        fields.billing_address_country,
        fields.billing_address_country_code,
        fields.billing_address_first_name,
        fields.billing_address_last_name,
        fields.billing_address_latitude,
        fields.billing_address_longitude,
        fields.billing_address_name,
        fields.billing_address_phone,
        fields.billing_address_province,
        fields.billing_address_province_code,
        fields.billing_address_zip,
        fields.browser_ip,
        fields.shipping_address_address_1,
        fields.shipping_address_address_2,
        fields.shipping_address_city,
        fields.shipping_address_company,
        fields.shipping_address_country,
        fields.shipping_address_country_code,
        fields.shipping_address_first_name,
        fields.shipping_address_last_name,
        fields.shipping_address_latitude,
        fields.shipping_address_longitude,
        fields.shipping_address_name,
        fields.shipping_address_phone,
        fields.shipping_address_province,
        fields.shipping_address_province_code,
        fields.shipping_address_zip,
        fields.token,
        fields.app_id,
        fields.checkout_id,
        fields.client_details_user_agent,
        fields.customer_locale,
        fields.order_status_url,
        fields.presentment_currency,
        fields.test as is_test_order,
        fields._fivetran_deleted as is_deleted,
        fields.buyer_accepts_marketing as has_buyer_accepted_marketing,
        fields.confirmed as is_confirmed,
        {{ dbt_date.convert_timezone(column='cast(_fivetran_synced as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as _fivetran_synced,
        fields.source_relation

        {{ fivetran_utils.fill_pass_through_columns('order_pass_through_columns') }}

    from fields
    left join order_line_agg
        on fields.id = order_line_agg.order_id
)

select * 
from final
where not coalesce(is_test_order, false)
and not coalesce(is_deleted, false)