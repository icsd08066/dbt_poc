{{ config(
    materialized='incremental',
    partition_by=['utc_date'],
    file_format='delta',
    database='de_sbx', 
    schema='dbt_poc'
) }}

select    CampaignID
        , CampaignName
        , CampaignStartTime
        , CampaignEndTime
        , CustomerDuration
        , CampaignCompany
        , CampaignBudget
        , CampaignBudgetEuro
        , CampaignStatusID
        , CampaignStatusName
        , CampaignTypeID
        , CampaignTypeName
        , CampaignRules
        , CampaignCreated
        , CreatedByName
        , CampaignLastUpdated
        , CampaignAmount
        , CampaignAmountEuro
        , CampaignIntentID
        , CampaignIntentDesc
        , cast(CampaignCreated as date) as utc_date
from    {{ ref('pandora_campaigns') }}
where   CampaignTypeID = 1