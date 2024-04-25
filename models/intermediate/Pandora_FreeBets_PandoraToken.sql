-- Use overwrite load strategy
{{ config(
    materialized='incremental',
    file_format='delta'
) }}

with part_prun as (
    select  DISTINCT fsbt.utc_date
    from       {{ source('pandoradb', 'fSportsbookToken') }}             as fsbt
    inner join {{ source('pandoradb', 'fSportsbookTokenCampaign') }}    as fstc on  fstcCampaignID = fsbtSportsbookTokenCampaignID
    where  ((       fstcCampaignTypeID                  = 1
          and   fsbtSportsbookTokenID               >= 20000000
          and   fsbtSportsbookTokenCampaignID       >= 10000
        )
        or  (fstcCampaignTypeID                  = 2 ))    
        and ifnull(fsbtLastUpdated,fsbtCreated) >= ({{ target_date() }})
)

,cte1 as (
select fsbtSportsbookTokenID
      ,fsbtCustomerID
      ,fsbtTokenStatusID
      ,msts.mstsName as TokenStatusName
      ,fsbtExpirationDate
      ,fsbtInitialAmount
      ,fsbtSportsbookTokenCampaignID
      ,fstcCompanyID
      ,fstcCampaignName
      ,fstcCampaignTypeID
      ,fsbtCreated
      ,fsts.utc_date
      ,ifnull(fsbtLastUpdated,fsbtCreated) as fsbtLastUpdated
      ,ifnull(abs(sum(fsttAmount)),0) as PlayedAmount
      ,fsts.comp_part
from       {{ source('pandoradb', 'fSportsbookToken') }}            as fsts
inner join {{ source('pandoradb', 'fSportsbookTokenCampaign') }}    as fstc on fstc.fstcCampaignID  = fsts.fsbtSportsbookTokenCampaignID and fstc.comp_part = fsts.comp_part
inner join part_prun                                                as prun on prun.utc_date        = fsts.utc_date
left join  de.PandoraDB.fSportsbookTokenTransaction  as fstt on fsts.fsbtSportsbookTokenID = fstt.fsttSportsbookTokenID         and fsts.comp_part = fstt.comp_part
left join  de.PandoraDB.mSportsbookTokenStatus       as msts on fsts.fsbtTokenStatusID     = msts.mstsTokenStatusID
where 
(
    (
          fstcCampaignTypeID                             = 1         -- freebets
    and   fsts.fsbtSportsbookTokenID                     >= 20000000 -- migrated geneity tokens
    and   coalesce(fstt.fsttSportsbookTokenID, 20000000) >= 20000000 -- migrated geneity tokens
    and   fsts.fsbtSportsbookTokenCampaignID             >= 10000    -- pandora
    )
    OR    fstcCampaignTypeID                             = 2         -- freebets
)
and ifnull(fsts.fsbtLastUpdated,fsts.fsbtCreated) >= ({{ target_date() }})
group by  fsbtSportsbookTokenID
        ,   fsbtCustomerID
        ,   fsbtTokenStatusID
        ,   msts.mstsName
        ,   fsbtExpirationDate
        ,   fsbtInitialAmount
        ,   fsbtSportsbookTokenCampaignID
        ,   fstcCampaignTypeID
        ,   fstcCompanyID
        ,   fstcCampaignName
        ,   fsbtCreated
        ,   fsts.utc_date
        ,   ifnull(fsbtLastUpdated,fsbtCreated)
        ,   fsts.comp_part
  )
, cte2 as (
      select fsts.fsbtSportsbookTokenID
             , fsts.fsbtCustomerID
             , cte1.fstcCompanyID
             , cte1.fstcCampaignTypeID
             , fstt.fsttBetID
             , fstt.utc_date  
      from       {{ source('pandoradb', 'fSportsbookToken') }}  as fSTS
      inner join cte1                                                   on fSTS.fsbtSportsbookTokenID = cte1.fsbtSportsbookTokenID and fSTS.comp_part = cte1.comp_part 
      left  join de.PandoraDB.fSportsbookTokenTransaction       as fSTT on fSTS.fsbtSportsbookTokenID = fSTT.fsttSportsbookTokenID and fSTS.comp_part = fSTT.comp_part 
      group by    fsts.fsbtSportsbookTokenID
                , fstt.fsttBetID
                , fsts.fsbtCustomerID 
                , cte1.fstcCompanyID
                , fstt.utc_date
                , cte1.fstcCampaignTypeID
  )
  , cte_winnings as (
      select   cte.fsbtSportsbookTokenID
             , cte.fsbtCustomerID
             , cte.fstcCampaignTypeID
             , sum(case when fstcCampaignTypeID = 1 then  SportBetSettled_Bet_Bonus_Winnings                                                                         end) as freebet_winnings
             , sum(case when fstcCampaignTypeID = 1 then  SportBetSettled_Bet_Bonus_Winnings_bccy_erc                                                                end) as freebet_winningsEuro
             , cast(sum(case when fstcCampaignTypeID = 2 then  SportBetSettled_winnings    + SportBetSettled_cash_out_win_bccy_erc / euro.rateLast end) as DECIMAL(38,7)) as fullbet_winnings
             , sum(case when fstcCampaignTypeID = 2 then  SportBetSettled_winnings_bccy_erc  +  SportBetSettled_cash_out_win_bccy_erc        end)                         as fullbet_winningsEuro
      from       de.kaizen_wars.fact_sportbetsettled as sbs 
      inner join cte2                             as cte on fsttBetID      = abs(sbs.sportbetsettled_bet_ID)
                                                        and fsbtCustomerID = sbs.sportbetsettled_Customer_ID
      LEFT JOIN {{ source('kaizen_wars', 'DIM_Exchange_Rate_euro') }}  euro on sbs.SportBetSettled_Currency_ID   = euro.currencyId
                                                        and euro.Date                         = sbs.utc_date
      where      sbs.sportbetsettled_status = 'A'                  
      and       sbs.utc_date BETWEEN (select min(utc_date) from cte2) and (select date_add(max(utc_date), 30) from cte2)
      group by  cte.fsbtSportsbookTokenID
              , cte.fsbtCustomerID
              , cte.fstcCampaignTypeID
  )
  select           cte1.fsbtSportsbookTokenID                                                           as Token
                  ,cte1.fsbtCustomerID                                                                  as CustomerID
                  ,cte1.fsbtTokenStatusID                                                               as StatusID
                  ,cte1.TokenStatusName                                                                 as StatusName
                  ,cte1.fsbtExpirationDate                                                              as TokenExpirationDate
                  ,cte1.fsbtInitialAmount                                                               as InitialAmount
                  ,cast(fsbtInitialAmount * euro.rateLast                        as decimal(38,15))     as InitialAmountEuro
                  ,cte1.fsbtSportsbookTokenCampaignID                                                   as CampaignID
                  ,cte1.fstcCampaignName                                                                as CampaignName
                  ,cte1.fsbtCreated                                                                     as TokenCreated
                  ,cte1.fsbtLastUpdated                                                                 as TokenLastUpdated
                  ,ifnull(cte1.PlayedAmount,0)                                                          as Stakes
                  ,cast(PlayedAmount * euro.rateLast                             as decimal(38,6))      as StakesEuro
                  ,ifnull(cw.freebet_winnings,0)                                                        as FreebetWinnings
                  ,ifnull(cw.freebet_winningsEuro,0)                                                    as FreebetWinningsEuro
                  ,ifnull(cw.fullbet_winnings,0)                                                        as FullbetWinnings
                  ,ifnull(cw.fullbet_winningsEuro,0)                                                    as FullbetWinningsEuro
                  ,cte1.fstcCampaignTypeID                                                              as CampaignTypeID
  from       cte1                               
  LEFT JOIN  cte_winnings                       as cw   on  cte1.fsbtSportsbookTokenID      = cw.fsbtSportsbookTokenID
                                                        and cte1.fsbtCustomerID             = cw.fsbtCustomerID
  LEFT JOIN de.masterconfigdb.dim_company_metadata company   on company.CompanyId              = fstcCompanyID
  LEFT JOIN de.KAIZEN_WArs.DIM_Exchange_Rate_euro euro       on company.CurrencyId             = euro.currencyId
                                                            and  euro.Date    = cte1.utc_date