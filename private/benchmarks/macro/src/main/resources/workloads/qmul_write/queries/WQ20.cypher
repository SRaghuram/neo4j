MATCH (p:Person)
SET p += { description: 'Person: ' + p.name }
RETURN p.description