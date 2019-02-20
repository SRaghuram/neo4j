MATCH (resource:Resource { name: { resourceName }})
MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin)
WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))
RETURN admin.name AS admin
UNION
MATCH (resource:Resource { name: { resourceName }})
MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin)
RETURN admin.name AS admin