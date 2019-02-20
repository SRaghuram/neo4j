MATCH (company:Company { name: { companyName }})
MATCH (company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company))
RETURN admin.name AS admin
UNION
MATCH (company:Company { name: { companyName }})
MATCH (company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company))
RETURN admin.name AS admin
UNION
MATCH (company:Company { name: { companyName }})
MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin)
RETURN admin.name AS admin