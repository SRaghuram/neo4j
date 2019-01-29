MATCH (n1)
WHERE n1.life_style CONTAINS 'always'
MATCH (n2)
WHERE n2.life_style CONTAINS 'never'
MATCH (n1)-[r:PERFECT_MATCH]->(n2)
RETURN r