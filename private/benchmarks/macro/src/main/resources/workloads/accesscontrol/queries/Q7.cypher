MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))
RETURN count(p) AS accessCount
UNION
MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))
RETURN count(p) AS accessCount
UNION
MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company))
RETURN count(p) AS accessCount
UNION
MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company))
RETURN count(p) AS accessCount
UNION
MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
RETURN count(p) AS accessCount
UNION
MATCH (admin:Administrator { name: { adminName }}),(resource:Resource { name: { resourceName }})
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
RETURN count(p) AS accessCount