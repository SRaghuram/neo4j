MATCH (n1)
WHERE n1.life_style CONTAINS 'always'
MATCH (n2)
WHERE n2.life_style CONTAINS 'never'
MERGE (n1)-[:PERFECT_MATCH]->(n2)