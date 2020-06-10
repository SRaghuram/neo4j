MATCH (o:Officer)
WHERE o.name CONTAINS $officer
MATCH path=(o)-[]->(:Entity)<-[]-(:Officer)-[]->(:Entity)
RETURN path
LIMIT 100