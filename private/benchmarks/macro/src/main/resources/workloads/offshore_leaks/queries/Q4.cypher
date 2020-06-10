MATCH (o:Officer)-[:CONNECTED_TO]->(:Entity)
RETURN o.name, count(*) as entities
ORDER BY entities DESC
LIMIT 10