with base as (

    select * 
    from {{ ref('stg_shopify__order_line_tmp') }}

),

fields as (

    select
    
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_shopify__order_line_tmp')),
                staging_columns=get_order_line_columns()
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
        fields.id as order_line_id,
        fields.index,
        fields.name,
        fields.order_id,
        fields.fulfillable_quantity,
        fields.fulfillment_service,
        fields.fulfillment_status,
        fields.gift_card as is_gift_card,
        fields.grams,
        fields.pre_tax_price,
        product_variant.price * quantity as price,
        fields.product_id,
        fields.quantity,
        fields.requires_shipping as is_shipping_required,
        fields.sku,
        fields.taxable as is_taxable,
        fields.tax_code,
        fields.title,
        fields.total_discount,
        fields.variant_id,
        fields.variant_title,
        fields.variant_inventory_management,
        fields.vendor,
        fields.properties,
        fields.destination_location_address_1,
        fields.destination_location_address_2,
        fields.destination_location_city,
        fields.destination_location_country_code,
        fields.destination_location_id,
        fields.destination_location_name,
        fields.destination_location_province_code,
        fields.destination_location_zip,
        fields.origin_location_address_1,
        fields.origin_location_address_2,
        fields.origin_location_city,
        fields.origin_location_country_code,
        fields.origin_location_id,
        fields.origin_location_name,
        fields.origin_location_province_code,
        fields.origin_location_zip,
        {{ dbt_date.convert_timezone(column='cast(fields._fivetran_synced as ' ~ dbt.type_timestamp() ~ ')', target_tz=var('shopify_timezone', "UTC"), source_tz="UTC") }} as _fivetran_synced,
        fields.source_relation

        {{ fivetran_utils.fill_pass_through_columns('order_line_pass_through_columns') }}

    from fields
    left join {{ ref('stg_shopify__product_variant') }} as product_variant
        on fields.product_id = product_variant.product_id
        and fields.variant_id = product_variant.variant_id

)

select * 
from final