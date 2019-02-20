MATCH (admin:Administrator { name: { adminName }}),(company:Company { name: { companyName }})
MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))
RETURN account.name AS account
UNION
MATCH (admin:Administrator { name: { adminName }}),(company:Company { name: { companyName }})
MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany))
RETURN account.name AS account
UNION
MATCH (admin:Administrator { name: { adminName }}),(company:Company { name: { companyName }})
MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
RETURN account.name AS account