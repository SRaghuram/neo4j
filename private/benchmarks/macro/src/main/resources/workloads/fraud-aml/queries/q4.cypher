MATCH p=(ah1:BusinessCustomer:AccountHolder)-[:MAKES_PAYMENTS_TO|ENTITY_RESOLUTION*1..4]->(ah2)
WHERE ah1.accountName=$accountName
  AND id(ah1)<>id(ah2)
RETURN p
ORDER BY length(p)
LIMIT 500
