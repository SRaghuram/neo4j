MATCH p=(ah1:BusinessCustomer:AccountHolder)-[:MAKES_PAYMENTS_TO|ENTITY_RESOLUTION*1..4]->(ah2:BusinessCustomer:AccountHolder)
WHERE ah1.accountName=$accountName1
  AND ah2.accountName=$accountName2
RETURN p
ORDER BY length(p)
LIMIT 500
