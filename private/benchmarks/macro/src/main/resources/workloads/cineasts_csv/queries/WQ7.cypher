LOAD CSV WITH HEADERS FROM {csv_filename} AS line
MERGE (u:User {login: line.user})
MERGE (m:Movie {id: line.movieId})
MERGE (u)-[:RATED {stars: line.stars, comment: line.comment}]->(m)
