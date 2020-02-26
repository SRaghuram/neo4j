MATCH (admin:Administrator { name: $adminName })-[:MEMBER_OF]->(group)
CALL {
    WITH admin, group
    MATCH (group)-[:ALLOWED_INHERIT]->(company)
    WHERE NOT (admin)-[:MEMBER_OF]->()-[:DENIED]->(company)
    CALL {
        WITH company
        RETURN company AS innerCompany
      UNION
        WITH admin, company
        MATCH (company)<-[:CHILD_OF]-(childCompany)
        WHERE NOT (admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(childCompany)
        RETURN childCompany AS innerCompany
    }
    RETURN innerCompany AS company
  UNION
    WITH group
    MATCH (group)-[:ALLOWED_DO_NOT_INHERIT]->(company)
    RETURN company
}
RETURN company.name
