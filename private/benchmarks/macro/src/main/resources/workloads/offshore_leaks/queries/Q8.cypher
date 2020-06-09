MATCH (a:Address)<-[:REGISTERED_ADDRESS]-(o:Officer),
(o)-->(e:Entity)<-[:INTERMEDIARY_OF]-(i:Intermediary)
WHERE a.address CONTAINS $city
RETURN e.jurisdiction_description AS jurisdiction, count(*) AS number
ORDER BY number DESC LIMIT 10