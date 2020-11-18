MATCH (n:PROFILES)
WHERE n.AGE IS NOT NULL
WITH n
LIMIT 500000
WITH n.AGE AS age, collect(n) AS nodes
MERGE (a:Age { age: age })
WITH a, nodes
FOREACH (n IN nodes | CREATE (a)<-[:HAS_AGE]-(n)
REMOVE n.AGE)