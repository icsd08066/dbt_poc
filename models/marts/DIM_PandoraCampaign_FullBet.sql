{{ config(
    partition_by=['utc_date'],
    file_format='delta'
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
where   CampaignTypeID = 2