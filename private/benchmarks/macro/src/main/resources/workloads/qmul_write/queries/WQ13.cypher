MATCH (p:Person { name: { name }})
SET p = { name: p.name, description: 'Person: ' + p.name }
RETURN p.description