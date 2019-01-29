LOAD CSV WITH HEADERS FROM {csv_filename} AS line
MATCH (a:Person {id: line.actorId})
MATCH (m:Movie {id: line.movieId})
RETURN COUNT(*)
