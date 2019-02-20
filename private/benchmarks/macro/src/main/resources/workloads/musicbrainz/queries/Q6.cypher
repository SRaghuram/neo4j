MATCH (artist:Artist { name: { name }})-[:COMPOSER]->(song:Song)<-[:LYRICIST]-(artist)
MATCH (song)<-[:PERFORMANCE]-(r:Recording)<-[:VOCAL]-(artist)
RETURN count(DISTINCT song.name)