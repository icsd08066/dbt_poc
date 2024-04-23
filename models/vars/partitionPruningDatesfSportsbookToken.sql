
SELECT DISTINCT fsbt.utc_date 
FROM       {{ source('pandoradb', 'fSportsbookToken') }}             AS fsbt
INNER JOIN {{ source('pandoradb', 'fSportsbookTokenCampaign') }}    AS fstc ON  fstcCampaignID = fsbtSportsbookTokenCampaignID
WHERE  ((       fstcCampaignTypeID                  = 1
          AND   fsbtSportsbookTokenID               >= 20000000
          AND   fsbtSportsbookTokenCampaignID       >= 10000
        )
        or  (fstcCampaignTypeID                  = 2 ))    
        AND ifnull(fsbtLastUpdated,fsbtCreated) >= (select * from {{ ref('target_date') }} )