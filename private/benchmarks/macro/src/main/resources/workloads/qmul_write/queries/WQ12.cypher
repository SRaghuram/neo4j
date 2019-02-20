MATCH (p:Person { name: { name }})
SET p.description = 'Person: ' + p.name
RETURN p.description