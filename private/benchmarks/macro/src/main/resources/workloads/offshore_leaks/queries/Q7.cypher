MATCH (o:Officer)-->(e:Entity)<-[:INTERMEDIARY_OF]-(i:Intermediary)
WHERE o.country_codes CONTAINS $country and i.sourceID = $source
RETURN e.jurisdiction_description AS jurisdiction, count(*) AS number
ORDER BY number DESC LIMIT 10