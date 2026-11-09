{{ config(
    materialized='incremental',
    unique_key='account_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['sfdc_silver_incremental']
) }}

with source as (

    select *
    from {{ source('sfdc_mt', 'account') }}
    {% if is_incremental() %}
    where "SystemModstamp" > (
        select dateadd(day, -{{ var('sfdc_lookback_days') }}, max(system_modstamp))
        from {{ this }}
    )
    {% endif %}

),

live_ids as (
    select "Id" as id from {{ source('sfdc_mt', 'account_sf_ids') }}
)

select
    a."Id"                                  as account_id,
    a."Name"                                as account_name,
    a."Type"                                as account_type,
    nullif(a."Industry", '0')               as industry,
    a."BillingCountry"                      as billing_country,
    a."BillingState"                        as billing_state,
    a."ShippingCountry"                     as shipping_country,
    a."OwnerId"                             as owner_id,
    a."ParentId"                            as parent_account_id,
    a."CreatedDate"                         as created_at,
    a."CurrencyIsoCode"                     as currency_iso_code,
    a."IsDeleted" or l.id is null           as is_deleted,
    a."SystemModstamp"                      as system_modstamp,
    {{ mt_loaded_at('Account') }}           as loaded_at
from source a
left join live_ids l on l.id = a."Id"
qualify row_number() over (partition by a."Id" order by a."SystemModstamp" desc) = 1
