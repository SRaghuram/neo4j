MATCH (p:Person)
SET p = { name: p.name, description: 'Person: ' + p.name }
RETURN p.description