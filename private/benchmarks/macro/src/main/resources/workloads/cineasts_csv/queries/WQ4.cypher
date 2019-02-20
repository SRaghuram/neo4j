USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM {csv_filename} AS line
  MERGE (a:Person {id: line.actorId})
  MERGE (m:Movie {id: line.movieId})
  SET a:Actor
  CREATE (a)-[:ACTS_IN {name: line.roleName}]->(m)
