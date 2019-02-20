LOAD CSV WITH HEADERS FROM {csv_filename} AS line
MATCH (u1:User {login: line.user1})
MATCH (u2:User {login: line.user2})
RETURN COUNT(*)
