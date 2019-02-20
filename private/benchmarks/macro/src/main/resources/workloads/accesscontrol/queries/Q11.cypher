MATCH (admin:Administrator { name: { adminName }})
MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany))
RETURN account.name AS account
UNION
MATCH (admin:Administrator { name: { adminName }})
MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
RETURN account.name AS account