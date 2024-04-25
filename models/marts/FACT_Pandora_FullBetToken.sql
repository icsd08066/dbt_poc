{{ config(
    materialized='incremental',
    file_format='delta',
    partition_by=['utc_date'],
    unique_key="FullbetToken||'-'||FullbetCustomerID",
    incremental_strategy='merge',
    database='de_sbx', 
    schema='dbt_poc'
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
where CampaignTypeID = 2