MATCH (artist:Artist { name:'Bob Dylan' })-[:COMPOSER]->(song:Song)<-[:LYRICIST]-(artist)
MATCH (song)<-[:PERFORMANCE]-(r:Recording)<-[:VOCAL]-(artist)
RETURN count(DISTINCT song.name)