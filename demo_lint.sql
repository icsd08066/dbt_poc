SELECT  DISTINCT fsbt.utc_date
                ,   fsbt.fsbtCreated
FROM      {{ source('pandoradb', 'fSportsbookToken') }} AS fsbt
INNER JOIN {{ source('pandoradb', 'fSportsbookTokenCampaign') }} AS fstc ON  fstcCampaignID = fsbtSportsbookTokenCampaignID
WHERE  ((       fstcCampaignTypeID                  = 1
      AND fsbtSportsbookTokenID               >= 20000000
      AND   fsbtSportsbookTokenCampaignID       >= 10000
    )
OR  (fstcCampaignTypeID                             = 2 ))    
 AND ifnull(fsbtLastUpdated,fsbtCreated) >= ({{ target_date() }})