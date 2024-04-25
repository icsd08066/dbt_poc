
select fstc.fstcCampaignID                                                                       as CampaignID
      ,fstc.fstcCampaignName                                                                     as CampaignName
      ,fstc.fstcStartTime                                                                        as CampaignStartTime
      ,fstc.fstcEndTime                                                                          as CampaignEndTime
      ,fstc.fstcCustomerDuration                                                                 as CustomerDuration
      ,fstc.fstcCompanyID                                                                        as CampaignCompany
      ,fstc.fstcCampaignBudget                                                                   as CampaignBudget
      ,cast(fstc.fstcCampaignBudget * euro.rateLast as decimal(38,15))                           as CampaignBudgetEuro
      ,fstc.fstcCampaignStatusID                                                                 as CampaignStatusID
      ,mtcs.mtcsName                                                                             as CampaignStatusName
      ,fstc.fstcCampaignTypeID                                                                   as CampaignTypeID
      ,mtct.mtctName                                                                             as CampaignTypeName
      ,fstcCampaignIntentID                                                                      as CampaignIntentID
      ,mcinName                                                                                  as CampaignIntentDesc
      ,fstc.fstcRules                                                                            as CampaignRules
      ,fstc.fstcCreated                                                                          as CampaignCreated
      ,pusr.userUserName                                                                         as CreatedByName
      ,fstc.fstcLastUpdated                                                                      as CampaignLastUpdated
      ,fstc.fstcAmount                                                                           as CampaignAmount
      ,cast(fstc.fstcAmount * euro.rateLast as decimal(38,15))                                   as CampaignAmountEuro
from        {{ source('pandoradb', 'fSportsbookTokenCampaign') }}       as fstc
inner join	{{ source('pandoradb', 'mSportsbookTokenCampaignStatus') }} as mtcs on fstc.fstcCampaignStatusID    = mtcs.mtcsCampaignStatusID
inner join	{{ source('pandoradb', 'mSportsbookTokenCampaignType') }}   as mtct on fstc.fstcCampaignTypeID	    = mtct.mtctCampaignTypeID
left  join  {{ source('masterconfigdb', 'dim_company_metadata') }}      as com  on com.CompanyId                = fstc.fstcCompanyID
left  join  {{ source('kaizen_wars', 'DIM_Exchange_Rate_euro') }}       as euro on com.CurrencyId               = euro.currencyId
                                                                                and  euro.Date                  = fstc.utc_date
left join	{{ source('pandoradb', 'uUser') }}                          as pusr ON fstc.fstcCreatedBy			= pusr.userUserID
left join   {{ source('pandoradb', 'mCampaignIntent') }}                as itnt ON fstc.fstcCampaignIntentID    = itnt.mcinCampaignIntentID
where  fstc.fstcCampaignTypeID = 2 -- fullbet
or    (       fstc.fstcCampaignTypeID = 1 -- freebet  
        and   fstc.fstcCampaignID >= 10000
      )