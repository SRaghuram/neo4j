MATCH (a:PROFILES { _key: { key }})-[pathRels:RELATION*2..2]->(b)
RETURN a._key, b._key, pathRels