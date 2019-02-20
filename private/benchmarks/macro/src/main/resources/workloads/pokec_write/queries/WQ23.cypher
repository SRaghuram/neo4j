MATCH (n:PROFILES)
WHERE exists(n.AGE)
WITH n
LIMIT 500000
WITH n.AGE AS age, collect(n) AS nodes
MERGE (a:Age { age: age })
WITH a, nodes
FOREACH (n IN nodes | CREATE (a)<-[:HAS_AGE]-(n))