MATCH (admin:Administrator { name: { adminName }})
MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))
RETURN employee.name AS employee, account.name AS account
UNION
MATCH (admin:Administrator { name: { adminName }})
MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company))
RETURN employee.name AS employee, account.name AS account
UNION
MATCH (admin:Administrator { name: { adminName }})
MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)
RETURN employee.name AS employee, account.name AS account