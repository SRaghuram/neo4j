LOAD CSV WITH HEADERS FROM {csv_filename} AS line
MERGE (u1:User {login: line.user1})
MERGE (u2:User {login: line.user2})
CREATE (u1)-[:FRIEND]->(u2)
