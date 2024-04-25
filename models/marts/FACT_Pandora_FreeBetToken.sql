{{ config(
    materialized='incremental',
    partition_by=['utc_date'],
    file_format='delta'
) }}


select   Token                  as FreebetToken
       , CustomerID             as FreebetCustomerID
       , StatusID               as FreebetStatusID
       , StatusName             as FreebetStatusName
       , TokenExpirationDate    as FreebetTokenExpirationDate
       , InitialAmount          as FreebetInitialAmount
       , InitialAmountEuro      as FreebetInitialAmountEuro
       , CampaignID             as FreebetCampaignID   
       , CampaignName           as FreebetCampaignName
       , TokenCreated           as FreebetTokenCreated     
       , TokenLastUpdated       as FreebetTokenLastUpdated     
       , Stakes                 as FreebetStakes       
       , StakesEuro             as FreebetStakestEuro 
       , FreebetWinnings
       , FreebetWinningsEuro
       , cast(TokenCreated as date) as utc_date
from  {{ ref('Pandora_FreeBets_PandoraToken') }}
where CampaignTypeID = 1