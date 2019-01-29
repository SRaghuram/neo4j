MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))
RETURN count(p) AS accessCount
UNION
MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource)
RETURN count(p) AS accessCount