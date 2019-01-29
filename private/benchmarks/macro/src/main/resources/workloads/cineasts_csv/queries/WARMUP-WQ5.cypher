LOAD CSV WITH HEADERS FROM {csv_filename} AS line
MATCH (d:Person {id: line.directorId})
MATCH (m:Movie {id: line.movieId})
RETURN COUNT(*)
