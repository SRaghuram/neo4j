//Compiled regression?
//In compiled, we now put the projection of n.pets just by the seek instead of with the RETURN clause.
//This does not seem very dangerous though, because we only support index seeks in compiled.
MATCH (n:PROFILES)-[:UNUSUAL_TYPE]->(b) WHERE n.pets = 42 RETURN n.pets, b
