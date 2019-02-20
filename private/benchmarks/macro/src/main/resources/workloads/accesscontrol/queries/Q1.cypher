MATCH (admin:Administrator { name: { adminName }})
MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))
RETURN company.name AS company
UNION
MATCH (admin:Administrator { name: { adminName }})
MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company))
RETURN company.name AS company
UNION
MATCH (admin:Administrator { name: { adminName }})
MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)
RETURN company.name AS company