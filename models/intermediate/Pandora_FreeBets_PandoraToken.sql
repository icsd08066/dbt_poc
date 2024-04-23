-- Use overwrite load strategy
{{ config(
    materialized='incremental',
    file_format='delta'
) }}

with cte1 as (
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
from       de.PandoraDB.fSportsbookToken             as fsts
inner join de.PandoraDB.fSportsbookTokenCampaign     as fstc on fstc.fstcCampaignID		= fsts.fsbtSportsbookTokenCampaignID and fstc.comp_part = fsts.comp_part
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
and ifnull(fsts.fsbtLastUpdated,fsts.fsbtCreated) >= (select * from {{ ref('target_date') }} )
and fsts.utc_date in (select * from {{ ref('partitionPruningDatesfSportsbookToken') }})   
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
      FROM       de.PandoraDB.fSportsbookToken            AS fSTS
      inner join cte1                                          ON fSTS.fsbtSportsbookTokenID = cte1.fsbtSportsbookTokenID AND fSTS.comp_part = cte1.comp_part 
      left  join de.PandoraDB.fSportsbookTokenTransaction AS fSTT ON fSTS.fsbtSportsbookTokenID = fSTT.fsttSportsbookTokenID AND fSTS.comp_part = fSTT.comp_part 
      group by    fsts.fsbtSportsbookTokenID
                , fstt.fsttBetID
                , fsts.fsbtCustomerID 
                , cte1.fstcCompanyID
                , fstt.utc_date
                , cte1.fstcCampaignTypeID
  )
  , cte_winnings as (
      SELECT   cte.fsbtSportsbookTokenID
             , cte.fsbtCustomerID
             , cte.fstcCampaignTypeID
             , SUM(CASE WHEN fstcCampaignTypeID = 1 THEN  SportBetSettled_Bet_Bonus_Winnings                                                                         END) AS freebet_winnings
             , SUM(CASE WHEN fstcCampaignTypeID = 1 THEN  SportBetSettled_Bet_Bonus_Winnings_bccy_erc                                                                END) AS freebet_winningsEuro
             , CAST(SUM(CASE WHEN fstcCampaignTypeID = 2 THEN  SportBetSettled_winnings    + SportBetSettled_cash_out_win_bccy_erc / euro.rateLast END) AS DECIMAL(38,7)) AS fullbet_winnings
             , SUM(CASE WHEN fstcCampaignTypeID = 2 THEN  SportBetSettled_winnings_bccy_erc  +  SportBetSettled_cash_out_win_bccy_erc        END)                         AS fullbet_winningsEuro
      FROM       de.kaizen_wars.fact_sportbetsettled AS sbs 
      INNER JOIN cte2                             AS cte ON fsttBetID      = abs(sbs.sportbetsettled_bet_ID)
                                                        AND fsbtCustomerID = sbs.sportbetsettled_Customer_ID
      LEFT JOIN de.KAIZEN_WArs.DIM_Exchange_Rate_euro euro ON sbs.SportBetSettled_Currency_ID   = euro.currencyId
                                                        AND euro.Date                         = sbs.utc_date
      WHERE      sbs.sportbetsettled_status = 'A'                  
      AND       sbs.utc_date BETWEEN (SELECT min(utc_date) FROM cte2) AND (SELECT date_add(max(utc_date), 30) FROM cte2)
      GROUP BY  cte.fsbtSportsbookTokenID
              , cte.fsbtCustomerID
              , cte.fstcCampaignTypeID
  )
  SELECT           cte1.fsbtSportsbookTokenID                                                           AS Token
                  ,cte1.fsbtCustomerID                                                                  AS CustomerID
                  ,cte1.fsbtTokenStatusID                                                               AS StatusID
                  ,cte1.TokenStatusName                                                                 AS StatusName
                  ,cte1.fsbtExpirationDate                                                              AS TokenExpirationDate
                  ,cte1.fsbtInitialAmount                                                               AS InitialAmount
                  ,CAST(fsbtInitialAmount * euro.rateLast                        as decimal(38,15))     AS InitialAmountEuro
                  ,cte1.fsbtSportsbookTokenCampaignID                                                   AS CampaignID
                  ,cte1.fstcCampaignName                                                                AS CampaignName
                  ,cte1.fsbtCreated                                                                     AS TokenCreated
                  ,cte1.fsbtLastUpdated                                                                 AS TokenLastUpdated
                  ,ifnull(cte1.PlayedAmount,0)                                                          AS Stakes
                  ,CAST(PlayedAmount * euro.rateLast                             as decimal(38,6))      AS StakesEuro
                  ,ifnull(cw.freebet_winnings,0)                                                        AS FreebetWinnings
                  ,ifnull(cw.freebet_winningsEuro,0)                                                    AS FreebetWinningsEuro
                  ,ifnull(cw.fullbet_winnings,0)                                                        AS FullbetWinnings
                  ,ifnull(cw.fullbet_winningsEuro,0)                                                    AS FullbetWinningsEuro
                  ,cte1.fstcCampaignTypeID                                                              AS CampaignTypeID
  FROM       cte1                               
  LEFT JOIN  cte_winnings                       AS cw   on  cte1.fsbtSportsbookTokenID      = cw.fsbtSportsbookTokenID
                                                        and cte1.fsbtCustomerID             = cw.fsbtCustomerID
  LEFT JOIN de.masterconfigdb.dim_company_metadata company   ON company.CompanyId              = fstcCompanyID
  LEFT JOIN de.KAIZEN_WArs.DIM_Exchange_Rate_euro euro       ON company.CurrencyId             = euro.currencyId
                                                          AND  euro.Date               = cte1.utc_date