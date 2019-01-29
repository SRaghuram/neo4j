MATCH (p:Person)
WITH p
LIMIT 1000
SET p += { description: 'Person: ' + p.name }
RETURN p.description