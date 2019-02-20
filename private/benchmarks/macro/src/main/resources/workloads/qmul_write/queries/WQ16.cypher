MATCH (p:Person)
WITH p
LIMIT 1000
SET p = { name: p.name, description: 'Person: ' + p.name }
RETURN p.description