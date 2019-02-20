USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM {csv_filename} AS line
  MERGE (d:Person {id: line.directorId})
  MERGE (m:Movie {id: line.movieId})
  SET d:Director
  CREATE (d)-[:DIRECTED]->(m)
