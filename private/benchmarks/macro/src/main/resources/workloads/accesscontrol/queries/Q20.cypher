MATCH (admin:Administrator { name: $adminName }),(resource:Resource { name: $resourceName })
MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource)
RETURN count(p) AS accessCount