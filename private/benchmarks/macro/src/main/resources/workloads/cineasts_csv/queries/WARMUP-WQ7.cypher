LOAD CSV WITH HEADERS FROM {csv_filename} AS line
MATCH (u:User {login: line.user})
MATCH (m:Movie {id: line.movieId})
MATCH (u)-[:RATED {stars: line.stars, comment: line.comment}]->(m)
RETURN COUNT(*)
