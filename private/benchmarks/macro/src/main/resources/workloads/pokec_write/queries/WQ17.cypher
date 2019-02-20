MATCH (a:PROFILES { _key: { key }})-[pathRels:RELATION*2..2]->(b)
FOREACH (r IN pathRels | DELETE r)
CREATE (a)-[:KNOWS { pathlen: size(pathRels)}]->(b)