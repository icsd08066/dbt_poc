{{ config(
    materialized='incremental',
    partition_by=['utc_date'],
    file_format='delta'
) }}


select   Token                  as FullbetToken
       , CustomerID             as FullbetCustomerID
       , StatusID               as FullbetStatusID
       , StatusName             as FullbetStatusName
       , TokenExpirationDate    as FullbetTokenExpirationDate
       , InitialAmount          as FullbetInitialAmount
       , InitialAmountEuro      as FullbetInitialAmountEuro
       , CampaignID             as FullbetCampaignID
       , CampaignName           as FullbetCampaignName
       , TokenCreated           as FullbetTokenCreated
       , TokenLastUpdated       as FullbetTokenLastUpdated
       , Stakes                 as FullbetStakes
       , StakesEuro             as FullbetStakesEuro
       , FullbetWinnings
       , FullbetWinningsEuro
       , cast(TokenCreated as date) as utc_date
from  {{ ref('Pandora_FreeBets_PandoraToken') }}
where CampaignTypeID = 2